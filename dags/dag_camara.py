"""Câmara dos Deputados (my_ingestion: legislativo/camara).

votacoes gera id_votacoes.csv e id_proposicao.csv, que parametrizam as quatro
entidades seguintes. Cadastro (legislaturas e deputados) fica em camara_cadastro.
"""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

with source_dag(
    "camara",
    schedule=None,  # em validação, ver README (Validação das DAGs); original "30 2 * * 1"
    tags=["demodados"],
    description="Câmara: votações, votos, orientações e proposições",
    max_active_tasks=2,
) as dag:
    votacoes = etl_group(CONFIG_FILE, ETLS, "votacoes")
    votacoes >> [
        etl_group(CONFIG_FILE, ETLS, entidade)
        for entidade in ("votos_deputados", "votos_orientacao", "proposicao", "proposicao_tema")
    ]
