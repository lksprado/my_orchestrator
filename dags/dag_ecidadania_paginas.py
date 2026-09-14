"""e-Cidadania: todas as páginas de consultas públicas (mensal).

O bronze gerado alimenta a DAG senado_status.
"""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.ecidadania.ecidadania_etl import CONFIG_FILE, ETLS

with source_dag(
    "ecidadania_paginas",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "30 6 20 * *"
    tags=["demodados"],
    description="e-Cidadania: páginas de consultas públicas",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "paginas")
