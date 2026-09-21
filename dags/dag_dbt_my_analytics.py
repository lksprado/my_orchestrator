"""dbt do my_analytics (todos os domínios) via Cosmos.

O projeto é montado em dbt/my_analytics (em dev, bind mount de ~/workspace/my_analytics).
A conexão vem da connection postgres_dw; o target recebe o nome do ambiente
(ENV) só para o dbt saber onde está (target.name). O schema do profile é
irrelevante: generate_schema_name deriva o schema do caminho do model.
"""

import os
from datetime import datetime

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT = "/usr/local/airflow/dbt/my_analytics"

profile_config = ProfileConfig(
    profile_name="my_analytics",
    target_name=os.environ.get("ENV", "dev"),
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        # threads vai no profile (profiles.yml), único lugar de onde o dbt lê:
        # o Cosmos não conhece "threads" em operator_args. Como cada model é uma
        # task (dbt run --select <model>), threads pesa mesmo é na task única de
        # testes do AFTER_ALL. Default 1 = dev; prod define DBT_THREADS no .env.
        profile_args={
            "schema": "public",
            "threads": int(os.environ.get("DBT_THREADS", "1")),
        },
    ),
)

dag_dbt_my_analytics = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT,
        project_name="my_analytics",
    ),
    profile_config=profile_config,
    render_config=RenderConfig(
        # after_each põe o teste relationships na task .test da dimensão, que
        # consulta a fato antes dela existir; os testes vão todos para o fim
        test_behavior=TestBehavior.AFTER_ALL,
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        # dbt_packages/ é vendorado no my_analytics: sem install_deps
        "target": profile_config.target_name,
    },
    schedule="30 9 * * *",
    start_date=datetime(2026, 9, 13),
    dag_id="dbt__build",
    default_args={"retries": 2},
    tags=["datawarehouses"],
    # Paralelismo dos models: é daqui que vem o ganho, não do threads. Default 2
    # = dev (máquina compartilhada); prod define DBT_MAX_ACTIVE_TASKS no .env.
    max_active_tasks=int(os.environ.get("DBT_MAX_ACTIVE_TASKS", "2")),
)
