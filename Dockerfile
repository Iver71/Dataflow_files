FROM apache/beam_python3.10_sdk:2.54.0

# Copiar pipeline
COPY pipeline.py /app/pipeline.py

# Instalar dependencias GCP necesarias
RUN pip install --no-cache-dir "apache-beam[gcp]==2.54.0"

WORKDIR /app

# Definir pipeline
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline.py"

# ENTRYPOINT oficial Beam
ENTRYPOINT ["/opt/apache/beam/boot"]