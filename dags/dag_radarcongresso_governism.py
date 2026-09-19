"""Radar do Congresso: governismo e parlamentares (semanal).

A staging desta fonte está desabilitada no dbt; a ingestão segue populando a raw.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.radar_congresso.radar_congresso_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "radar_congresso__governism__ingestion",
    schedule="@weekly",
    tags=["politics"],
    description="Radar do Congresso: governismo de deputados e senadores, parlamentares",
) as dag:
    for entidade in ("governismo_deputados", "governismo_senadores", "parlamentares"):
        etl_group(CONFIG_FILE, ETLS, entidade)
