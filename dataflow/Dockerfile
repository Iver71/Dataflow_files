FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipeline.py"

COPY . /template

RUN apt-get update && apt-get install -y libffi-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r /template/requirements.txt
