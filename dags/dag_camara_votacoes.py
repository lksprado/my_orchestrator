"""Câmara: votações -> votos por deputado e orientação de bancada.

Piloto do padrão my_ingestion (load: table). Cada entidade roda
extract -> transform -> load com o GenericETL; a carga é full refresh em
raw_camara.<entidade> no banco do ambiente (settings.db_target), então não há
mais tabela _stg nem checagem de contagem aqui. votacoes gera os CSVs de IDs
(id_votacoes.csv) que os dois seguintes consomem.
"""

from datetime import datetime

from airflow.decorators import dag, task

from core import build_etl
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

default_args = {"owner": "airflow", "retries": 2}


def _etl(entidade: str):
    # build_etl instancia PipelineConfig (cria diretórios): só dentro de task.
    return build_etl(CONFIG_FILE, entidade, ETLS[entidade])


@dag(
    dag_id="camara_votacoes_pipeline",
    start_date=datetime(2026, 9, 13),
    schedule=None,  # em validação, ver README (seção Validação dos pilotos); original "30 2 * * 1"
    catchup=False,
    default_args=default_args,
    tags=["demodados"],
    max_active_tasks=1,
)
def camara_votacoes_pipeline():
    @task
    def extract(entidade: str):
        _etl(entidade).extract()

    @task
    def transform(entidade: str):
        _etl(entidade).transform()

    @task
    def check_bronze(entidade: str):
        # O load faz replace em raw_camara.<entidade>: bronze vazio zeraria a
        # tabela. Mesma proteção da checagem de staging da DAG antiga.
        path = _etl(entidade).cfg.bronze_filepath
        if not path.is_file():
            raise FileNotFoundError(f"Bronze ausente, abortando carga: {path}")
        with open(path, encoding="utf-8") as f:
            linhas = sum(1 for _ in f) - 1  # desconta o cabeçalho
        if linhas <= 0:
            raise ValueError(f"Bronze vazio, abortando carga: {path}")

    @task
    def load(entidade: str):
        _etl(entidade).load()

    def chain(entidade: str):
        e = extract.override(task_id=f"{entidade}_extract")(entidade)
        t = transform.override(task_id=f"{entidade}_transform")(entidade)
        c = check_bronze.override(task_id=f"{entidade}_check_bronze")(entidade)
        ld = load.override(task_id=f"{entidade}_load")(entidade)
        e >> t >> c >> ld
        return e, ld

    _, votacoes_load = chain("votacoes")
    votos_dep_extract, _ = chain("votos_deputados")
    votos_ori_extract, _ = chain("votos_orientacao")

    votacoes_load >> [votos_dep_extract, votos_ori_extract]


dag = camara_votacoes_pipeline()
