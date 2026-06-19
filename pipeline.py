import argparse
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import gcsio

class RouteGCSFilesFn(beam.DoFn):
    def __init__(self, raw_path, quarantine_path):
        self.raw_path = raw_path
        self.quarantine_path = quarantine_path

    def process(self, element):
        # El elemento es una ruta de archivo completa (ej. gs://dkft_bronze/landing/archivo.csv)
        gcs_path = element
        filename = os.path.basename(gcs_path)
        
        if not filename:
            return

        # Inicializar el cliente de Cloud Storage de Beam
        google_gcs = gcsio.GcsIO()
        
        # Determinar la carpeta de destino según la extensión .csv
        if filename.lower().endswith('.csv'):
            target_directory = self.raw_path
        else:
            target_directory = self.quarantine_path
            
        target_path = os.path.join(target_directory, filename)
        logging.info(f"Copiando archivo {filename} de landing hacia: {target_path}")
        
        try:
            # Leer el archivo original desde landing
            with google_gcs.open(gcs_path, 'r') as src:
                content = src.read()
            
            # Escribir el archivo exactamente igual en su carpeta destino
            with google_gcs.open(target_path, 'w') as dest:
                dest.write(content)
                
            logging.info(f"Archivo {filename} copiado con éxito.")
        except Exception as e:
            logging.error(f"Error procesando el archivo {filename}: {str(e)}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True, help='gs://dkft_bronze/landing/*')
    parser.add_argument('--output_raw', required=True, help='gs://dkft_bronze/Raw/')
    parser.add_argument('--output_quarantine', required=True, help='gs://dkft_bronze/quarantine/')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Usar Create con la ruta base expandida para listar los archivos reales de forma nativa
        from apache_beam.io.filesystems import FileSystems
        
        # Obtener la lista física de archivos que están en la carpeta landing actualmente
        match_result = FileSystems.match([known_args.input_path])
        files_found = [metadata.path for metadata in match_result[0].metadata_list]
        
        if not files_found:
            logging.info("No se encontraron archivos en la ruta de landing.")
            # Si no hay archivos, inicializar una lista vacía para evitar que Dataflow falle
            files_found = []

        (
            p
            | 'InicializarLista' >> beam.Create(files_found)
            | 'ProcesarEnrutamiento' >> beam.ParDo(RouteGCSFilesFn(known_args.output_raw, known_args.output_quarantine))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
