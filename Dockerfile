FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Layout plano do my_ingestion: core/, pipelines/ e settings.py ficam no topo de
# include/my_ingestion/src (sem pip install do pacote). Mesmo valor em dev e prod.
ENV PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include/my_ingestion/src

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r ./requirements.txt && deactivate

# Só em prod existe dbt/my_analytics no contexto do build (o deploy/build_prod.sh
# põe o repo ali); em dev ele vem do bind do workspace e este passo não faz nada.
# dbt_packages/ não é versionado no my_analytics: instala o package-lock.yml.
RUN if [ -f dbt/my_analytics/dbt_project.yml ]; then \
        cd dbt/my_analytics && /usr/local/airflow/dbt_venv/bin/dbt deps --project-dir . --profiles-dir . && rm -rf logs; \
    fi
