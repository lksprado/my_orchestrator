from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

# -----------------------------
# DEFAULT ARGS
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),
    "retries": 2,
}


# -----------------------------
# DAG MASTER
# -----------------------------
@dag(
    dag_id="nhl_master_pipeline",
    description="Pipeline master NHL: ingestão -> dbt -> ingestões complementares -> dbt",
    schedule="00 08 * * *",
    catchup=False,
    default_args=default_args,
    tags=["nhl"],
    max_active_tasks=2,
    max_active_runs=1,
)
def nhl_master_pipeline():
    # -----------------------------
    # 1️⃣ INGESTÃO CORE
    # -----------------------------
    trigger_games_summary = TriggerDagRunOperator(
        task_id="trigger_games_summary",
        trigger_dag_id="nhl_games_summary",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # 2️⃣ DBT RUN – APÓS CORE
    # -----------------------------
    trigger_dbt_pre = TriggerDagRunOperator(
        task_id="trigger_dbt_pre",
        trigger_dag_id="dag_dbt_nhl",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # 3️⃣ INGESTÕES COMPLEMENTARES
    # -----------------------------
    trigger_games_summary_details = TriggerDagRunOperator(
        task_id="trigger_games_summary_details",
        trigger_dag_id="nhl_games_summary",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_games_details = TriggerDagRunOperator(
        task_id="trigger_games_details",
        trigger_dag_id="nhl_games_details",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_games_play_by_play = TriggerDagRunOperator(
        task_id="trigger_games_play_by_play",
        trigger_dag_id="nhl_games_play_by_play",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_game_log = TriggerDagRunOperator(
        task_id="trigger_game_log",
        trigger_dag_id="nhl_game_log",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_club_stats = TriggerDagRunOperator(
        task_id="trigger_club_stats",
        trigger_dag_id="nhl_club_stats",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_players = TriggerDagRunOperator(
        task_id="trigger_players",
        trigger_dag_id="nhl_all_players",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # 4️⃣ DBT RUN FINAL
    # -----------------------------
    trigger_dbt_pos = TriggerDagRunOperator(
        task_id="trigger_dbt_pos",
        trigger_dag_id="dag_dbt_nhl",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # FLUXO
    # -----------------------------
    (
        trigger_games_summary
        >> trigger_dbt_pre
        >> trigger_games_summary_details
        >> trigger_games_details
        >> trigger_games_play_by_play
        >> trigger_club_stats
        >> trigger_game_log
        >> trigger_players
        >> trigger_dbt_pos
    )


# -----------------------------
# INSTANTIAÇÃO
# -----------------------------
dag = nhl_master_pipeline()
