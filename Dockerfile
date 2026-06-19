FROM apache_beam:python3.10

# Copiar el código de la pipeline al contenedor
COPY pipeline.py /app/pipeline.py

# Establecer el entorno
WORKDIR /app
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline.py"

# Punto de entrada para el launcher de Dataflow
ENTRYPOINT ["/opt/apache/beam/boot"]
