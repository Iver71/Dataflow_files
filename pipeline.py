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
        # Leer archivos y obtener tuplas (nombre_archivo, contenido_lineas)
        files = (
            p 
            | 'ReadFiles' >> fileio.MatchFiles(known_args.input_path)
            | 'ReadMatches' >> fileio.ReadMatches()
            | 'ExtractContent' >> beam.Map(lambda file_metadata: (file_metadata.metadata.path, file_metadata.read_utf8()))
        )

        # Filtrar archivos .csv
        csv_files = (
            files
            | 'FilterCSV' >> beam.Filter(lambda file_tuple: file_tuple[0].endswith('.csv'))
        )

        # Filtrar archivos que no son .csv
        non_csv_files = (
            files
            | 'FilterNonCSV' >> beam.Filter(lambda file_tuple: not file_tuple[0].endswith('.csv'))
        )

        # Escribir en Raw
        def write_to_raw(file_tuple):
            path, content = file_tuple
            filename = os.path.basename(path)
            return beam.io.filesystems.FileSystems.create(os.path.join(known_args.output_raw, filename))

        # Escribir en Quarantine
        def write_to_quarantine(file_tuple):
            path, content = file_tuple
            filename = os.path.basename(path)
            return beam.io.filesystems.FileSystems.create(os.path.join(known_args.output_quarantine, filename))

        # Escribir el contenido (puedes ajustar el WriteToText a tus necesidades)
        csv_files | 'WriteRaw' >> beam.io.WriteToText(
            known_args.output_raw, 
            file_name_suffix='.txt'
        )
        non_csv_files | 'WriteQuarantine' >> beam.io.WriteToText(
            known_args.output_quarantine, 
            file_name_suffix='.txt'
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
