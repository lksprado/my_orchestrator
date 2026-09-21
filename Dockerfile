FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Layout plano do my_ingestion: core/, pipelines/ e settings.py ficam no topo de
# include/my_ingestion/src (sem pip install do pacote). Mesmo valor em dev e prod.
ENV PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include/my_ingestion/src

# Os bancos (dev e prod) são Postgres 16 e o cliente do bookworm é o 15: o
# pg_dump se recusa a ler servidor mais novo ("server version mismatch"), e é
# dele que o raw_copy.sh precisa para criar tabela nova no destino. psql e
# pg_dump já são links para o pg_wrapper do Debian, que passa a resolver para a
# maior versão instalada.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
         -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
         > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-16 \
    && rm -rf /var/lib/apt/lists/*
USER astro

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r ./requirements.txt && deactivate
