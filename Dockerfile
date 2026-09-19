FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Layout plano do my_ingestion: core/, pipelines/ e settings.py ficam no topo de
# include/my_ingestion/src (sem pip install do pacote). Mesmo valor em dev e prod.
ENV PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include/my_ingestion/src

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r ./requirements.txt && deactivate
