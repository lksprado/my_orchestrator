"""Investimentos: abas das planilhas do Google Sheets (diário).

Service account em GOOGLE_CREDENTIALS_FILE e URLs em URL_FINANCE__<CHAVE> (.env).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.financas.investimentos.investimentos_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "investimentos_google",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "0 3 * * *"
    tags=["financas"],
    description="Investimentos: planilhas do Google Sheets para raw_google",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "google")
