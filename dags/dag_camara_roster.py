"""Câmara: cadastro de deputados e legislaturas (mensal).

deputados rebaixa a ficha dos atuais (id_deputados.csv, atualizado pela API na
primeira task) e baixa só as que faltam dos deputados das legislaturas 51–57
(id_deputados_legislaturas.csv, gerado por legislaturas). Por isso legislaturas
roda antes.
"""

from airflow.sdk import task
from pipelines.legislativo._params.atualizar_deputados import obter_ids_deputados_atuais
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

from include.utils.etl_dag import etl_group, source_dag

with source_dag(
    "camara__roster__ingestion",
    schedule="5 5 1 * *",
    tags=["politics"],
    description="Câmara: legislaturas 51–57, ids dos deputados atuais e perfis",
) as dag:

    @task
    def atualizar_ids_deputados():
        obter_ids_deputados_atuais()

    deputados = etl_group(CONFIG_FILE, ETLS, "deputados")
    [atualizar_ids_deputados(), etl_group(CONFIG_FILE, ETLS, "legislaturas")] >> deputados
