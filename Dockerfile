FROM apache/beam_python3.10_sdk:2.54.0

# Instalar obligatoriamente Java (Requerido para operaciones de FileIO de Beam en GCP)
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

# Copiar el código de la pipeline al contenedor
COPY pipeline.py /app/pipeline.py

# Forzar la instalación/actualización de las dependencias GCP en Python
RUN pip install --no-cache-dir "apache-beam[gcp]==2.54.0"

# Establecer el entorno
WORKDIR /app
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline.py"

# Punto de entrada para el launcher de Dataflow
ENTRYPOINT ["/opt/apache/beam/boot"]
