"""Senado Federal (my_ingestion: legislativo/senado).

votacoes grava o landing que votos_senadores lê e o id_processo.csv que
parametriza processo. votos_orientacao é independente. Cadastro fica em
senado_cadastro e status em senado_status.
"""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.senado.senado_etl import CONFIG_FILE, ETLS

with source_dag(
    "senado",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "30 2 1 * *"
    tags=["demodados"],
    description="Senado: votações (2001 em diante), votos, orientações e processos",
    max_active_tasks=2,
) as dag:
    votacoes = etl_group(CONFIG_FILE, ETLS, "votacoes")
    votacoes >> [
        etl_group(CONFIG_FILE, ETLS, "votos_senadores"),
        etl_group(CONFIG_FILE, ETLS, "processo"),
    ]
    etl_group(CONFIG_FILE, ETLS, "votos_orientacao")
