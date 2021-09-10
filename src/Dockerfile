FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# install git due to bug
# https://issuetracker.google.com/issues/176570473
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /dataflow/template
COPY ./ .

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/dataflow/template/job-requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/dataflow/template/setup.py"

RUN pip install -U -r ./requirements.txt
