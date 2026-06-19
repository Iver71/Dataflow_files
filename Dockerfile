FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

# Copiar pipeline
COPY pipeline.py /app/pipeline.py

# Instalar dependencias GCP necesarias
RUN pip install --no-cache-dir "apache-beam[gcp]==2.54.0"

WORKDIR /app

# Definir pipeline
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline.py"
