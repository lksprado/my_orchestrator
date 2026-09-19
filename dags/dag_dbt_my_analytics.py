"""dbt do my_analytics (todos os domínios) via Cosmos.

O projeto é montado em dbt/my_analytics (em dev, bind mount de ~/workspace/my_analytics).
A conexão vem da connection postgres_dw; o target recebe o nome do ambiente
(ENV) só para o dbt saber onde está (target.name). O schema do profile é
irrelevante: generate_schema_name deriva o schema do caminho do model.
"""

import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT = "/usr/local/airflow/dbt/my_analytics"

profile_config = ProfileConfig(
    profile_name="my_analytics",
    target_name=os.environ.get("ENV", "dev"),
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "public"},
    ),
)

dag_dbt_my_analytics = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT,
        project_name="my_analytics",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        # dbt_packages/ é vendorado no my_analytics: sem install_deps
        "target": profile_config.target_name,
        "threads": 1,
    },
    schedule="30 9 * * *",
    start_date=datetime(2026, 9, 13),
    catchup=False,
    dag_id="dbt__build",
    default_args={"retries": 2},
    tags=["datawarehouses"],
    max_active_tasks=2,
)
