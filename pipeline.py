import argparse
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_path',
        dest='input_path',
        required=True,
        help='Ruta de entrada en GCS (ej. gs://dkft_bronze/landing/*)')
    parser.add_argument(
        '--output_raw',
        dest='output_raw',
        required=True,
        help='Ruta de salida Raw en GCS (ej. gs://dkft_bronze/Raw/)')
    parser.add_argument(
        '--output_quarantine',
        dest='output_quarantine',
        required=True,
        help='Ruta de salida Quarantine en GCS (ej. gs://dkft_bronze/quarantine/)')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        # 1. Leer metadatos y contenido de los archivos en landing
        files = (
            p 
            | 'BuscarArchivos' >> fileio.MatchFiles(known_args.input_path)
            | 'LeerMatches' >> fileio.ReadMatches()
            | 'ExtraerContenido' >> beam.Map(lambda file_metadata: (file_metadata.metadata.path, file_metadata.read_utf8()))
        )

        # 2. Filtrar archivos .csv (Evaluando el índice [0] que es la ruta)
        csv_files = (
            files
            | 'FiltrarCSV' >> beam.Filter(lambda file_tuple: file_tuple[0].lower().endswith('.csv'))
            | 'ObtenerTextoCSV' >> beam.Map(lambda file_tuple: file_tuple[1])
        )

        # 3. Filtrar archivos que NO son .csv
        non_csv_files = (
            files
            | 'FiltrarNoCSV' >> beam.Filter(lambda file_tuple: not file_tuple[0].lower().endswith('.csv'))
            | 'ObtenerTextoNoCSV' >> beam.Map(lambda file_tuple: file_tuple[1])
        )

        # 4. Escribir los resultados en sus respectivos destinos
        csv_files | 'EscribirEnRaw' >> beam.io.WriteToText(
            os.path.join(known_args.output_raw, 'datos'),
            file_name_suffix='.csv',
            shard_name_template=''  # Evita que se fragmente en múltiples archivos pequeños si es batch
        )

        non_csv_files | 'EscribirEnQuarantine' >> beam.io.WriteToText(
            os.path.join(known_args.output_quarantine, 'archivo'),
            file_name_suffix='.txt',
            shard_name_template=''
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
