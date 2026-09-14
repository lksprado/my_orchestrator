"""dbt do the_dw (todos os domínios) via Cosmos.

O projeto é o submódulo dbt/the_dw (em dev, bind mount de ~/workspace/the_dw).
A conexão vem da connection postgres_dw; o target recebe o nome do ambiente
(ENV) só para o dbt saber onde está (target.name). O schema do profile é
irrelevante: generate_schema_name deriva o schema do caminho do model.
"""

import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT = "/usr/local/airflow/dbt/the_dw"

profile_config = ProfileConfig(
    profile_name="my_datawarehouse",
    target_name=os.environ.get("ENV", "dev"),
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "public"},
    ),
)

dag_dbt_the_dw = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT,
        project_name="the_dw",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        # dbt_packages/ é vendorado no the_dw: sem install_deps
        "target": profile_config.target_name,
        "threads": 1,
    },
    schedule="30 9 * * *",
    start_date=datetime(2026, 9, 13),
    catchup=False,
    dag_id="dag_dbt_the_dw",
    default_args={"retries": 2},
    tags=["dw", "demodados", "financas", "atibaia", "livros", "inflation", "nhl"],
    max_active_tasks=2,
)
