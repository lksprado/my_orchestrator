import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.local_setup.src.pipelines.legislativo.camara.camara_deputados import (
    extract as extract_deputados,
    transform as transform_deputados,
)
from include.local_setup.src.utils.loaders.postgres import PostgreSQLManager
from include.local_setup.src.utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config

_CONFIG_FILE = (
    Path(__file__).parent.parent
    / "include" / "local_setup" / "src" / "pipelines" / "legislativo"
    / "camara" / "camara_config.yml"
)


@dag(
    dag_id="deputados_pipeline",
    start_date=datetime(2025, 9, 25),
    schedule=None,
    catchup=False,
    tags=["demodados"],
)
def deputados_pipeline():
    logger = logging.getLogger("DAG: deputados")

    cfg = PipelineConfig(**load_source_config(_CONFIG_FILE, source="deputados", env="airflow"))
    target = cfg.db_table

    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)

    etl = GenericETL(cfg=cfg, extract_fn=extract_deputados, load_fn=None, log=logger)

    @task
    def t_extract():
        etl.extract()

    @task
    def t_transform():
        transform_deputados(cfg)

    @task
    def t_create_schema():
        pg.execute_query("CREATE SCHEMA IF NOT EXISTS raw")

    @task
    def t_load_staging():
        import pandas as pd

        pg.execute_query(f"DROP TABLE IF EXISTS raw.{etl.cfg.db_table}")
        df = pd.read_csv(etl.cfg.bronze_filepath, sep=";")
        pg.send_df_to_db(df, table_name=etl.cfg.db_table, filename=etl.cfg.bronze_filepath.name)

    @task
    def t_check_staging_count():
        result = pg.fetchone(f"SELECT COUNT(*) FROM raw.{etl.cfg.db_table}")
        if not result or result[0] == 0:
            raise ValueError("Staging está vazia, abortando promoção para raw")
        logger.info(f"Staging tem {result[0]} linhas")

    @task
    def t_insert():
        pg.execute_query(f"""
            CREATE TABLE IF NOT EXISTS raw.{target}
            AS SELECT * FROM raw.{etl.cfg.db_table} LIMIT 0;
            TRUNCATE TABLE raw.{target};
            INSERT INTO raw.{target}
            SELECT * FROM raw.{etl.cfg.db_table};
        """)

    @task
    def t_drop_stg_if_exists():
        pg.execute_query(f"DROP TABLE IF EXISTS raw.{etl.cfg.db_table};")

    extract = t_extract()
    transform = t_transform()
    create_raw = t_create_schema()
    load_staging = t_load_staging()
    check_staging = t_check_staging_count()
    insert_into_target = t_insert()
    drop_staging = t_drop_stg_if_exists()

    (
        extract
        >> transform
        >> create_raw
        >> load_staging
        >> check_staging
        >> insert_into_target
        >> drop_staging
    )


dag = deputados_pipeline()
