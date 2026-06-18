import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import gcsio

class EvaluarYMigrarArchivo(beam.DoFn):
    """
    Clase que evalúa cada archivo de la Landing Zone.
    Mueve los .csv a Bronze/Raw y el resto a Bronze/Quarantine.
    """
    def __init__(self, bucket_raw, bucket_quarantine):
        self.bucket_raw = bucket_raw
        self.bucket_quarantine = bucket_quarantine

    def process(self, gcs_path):
        # Inicializar el cliente nativo de Google Cloud Storage en Beam
        google_cloud_storage = gcsio.GcsIO()
        
        # Extraer el nombre del archivo de la ruta completa (gs://bucket/objeto)
        nombre_archivo = gcs_path.split('/')[-1]
        
        # Ignorar directorios vacíos que a veces entrega GCS
        if not nombre_archivo:
            return

        # Lógica de enrutamiento basada en la extensión
        if gcs_path.lower().endswith('.csv'):
            ruta_destino = f"{self.bucket_raw}/{nombre_archivo}"
            categoria = "RAW (CSV)"
        else:
            ruta_destino = f"{self.bucket_quarantine}/{nombre_archivo}"
            categoria = "QUARANTINE (No válido)"

        logging.info(f"Evaluando: {nombre_archivo} -> Redireccionando a {categoria}")

        try:
            # 1. Copiar el archivo al destino correspondiente
            google_cloud_storage.copy(gcs_path, r_path=ruta_destino)
            # 2. Eliminar el archivo original de la Landing Zone (Operación de movimiento)
            google_cloud_storage.delete(gcs_path)
            
            logging.info(f"Migración exitosa: {nombre_archivo} movido a {ruta_destino}")
            yield f"EXITO: {nombre_archivo} -> {ruta_destino}"
            
        except Exception as e:
            logging.error(f"Error al procesar el archivo {nombre_archivo}: {str(e)}")
            yield f"ERROR: {nombre_archivo} - {str(e)}"

def run(argv=None):
    parser = argparse.ArgumentParser()
    
    # Parámetros obligatorios que recibirá el Flex Template
    parser.add_argument('--landing_zone', required=True, help='Ruta completa de entrada, ej: gs://mi-landing/*')
    parser.add_argument('--bucket_raw', required=True, help='Ruta destino para CSV, ej: gs://mi-bronze-raw')
    parser.add_argument('--bucket_quarantine', required=True, help='Ruta destino para errores, ej: gs://mi-bronze-quarantine')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            # 1. Buscar todos los archivos que coincidan con el patrón en la Landing Zone
            | 'Listar Archivos Landing' >> beam.io.fileio.MatchFiles(known_args.landing_zone)
            # 2. Extraer la ruta de texto de cada archivo encontrado
            | 'Obtener Rutas GCS' >> beam.Map(lambda match_result: match_result.path)
            # 3. Aplicar la lógica de migración y descarte
            | 'Migrar Archivos dinámicamente' >> beam.ParDo(
                EvaluarYMigrarArchivo(
                    bucket_raw=known_args.bucket_raw, 
                    bucket_quarantine=known_args.bucket_quarantine
                )
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
