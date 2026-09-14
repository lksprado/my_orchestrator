"""e-Cidadania: todas as páginas de consultas públicas (mensal).

O bronze gerado alimenta a DAG senado_status.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.ecidadania.ecidadania_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "ecidadania_paginas",
    schedule="30 6 20 * *",
    tags=["demodados"],
    description="e-Cidadania: páginas de consultas públicas",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "paginas")
