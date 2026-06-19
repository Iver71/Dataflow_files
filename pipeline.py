import argparse
import logging
import os
import posixpath
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio


class RouteGCSFilesFn(beam.DoFn):
    def __init__(self, raw_path, quarantine_path):
        self.raw_path = raw_path
        self.quarantine_path = quarantine_path

    def process(self, file_metadata):
        import apache_beam.io.gcp.gcsio as gcsio

        gcs = gcsio.GcsIO()

        # Path real del archivo en GCS
        gcs_path = file_metadata.metadata.path
        filename = os.path.basename(gcs_path)

        if not filename:
            return

        # Routing logic
        if filename.lower().endswith(".csv"):
            target_directory = self.raw_path
        else:
            target_directory = self.quarantine_path

        # ✅ IMPORTANTE: usar posixpath (NO os.path.join)
        target_path = posixpath.join(target_directory, filename)

        logging.info(f"Procesando {gcs_path} -> {target_path}")

        try:
            # Leer archivo desde GCS
            with gcs.open(gcs_path) as src:
                content = src.read()

            # Escribir archivo destino
            with gcs.open(target_path, "w") as dest:
                dest.write(content)

            logging.info(f"Archivo copiado correctamente: {filename}")

        except Exception as e:
            logging.error(f"Error procesando {filename}: {str(e)}")


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_raw", required=True)
    parser.add_argument("--output_quarantine", required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:

        (
            p
            | "Match Files" >> fileio.MatchFiles(known_args.input_path)
            | "Read Matches" >> fileio.ReadMatches()
            | "Route Files" >> beam.ParDo(
                RouteGCSFilesFn(
                    known_args.output_raw,
                    known_args.output_quarantine
                )
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()