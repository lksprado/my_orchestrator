"""e-Cidadania: números gerais e matérias mais votadas (diário).

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from pipelines.legislativo.ecidadania.ecidadania_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "ecidadania",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "0 5 * * *"
    tags=["demodados"],
    description="e-Cidadania: big numbers e mais votados",
) as dag:
    etl_group(CONFIG_FILE, ETLS, "bignumbers")
    etl_group(CONFIG_FILE, ETLS, "mais_votados")
