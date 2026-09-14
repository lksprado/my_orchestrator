"""Senado: cadastro de senadores e legislaturas (manual).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.senado.senado_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "senado_cadastro",
    schedule=None,
    tags=["demodados"],
    description="Senado: senadores atuais e legislaturas",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "legislaturas")
    etl_group(CONFIG_FILE, ETLS, "senadores")
