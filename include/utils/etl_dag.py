"""Factory das DAGs por fonte do my_ingestion.

Uma DAG por fonte (``<fonte>_etl.py``): cada entidade vira um ``TaskGroup`` com
as etapas que ela realmente tem, na ordem ``extract → transform → check_bronze
→ load``. A lógica fica no my_ingestion; aqui só se decide quando e em que ordem.

Uso::

    from include.utils.etl_dag import etl_group, source_dag
    from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, ETLS

    with source_dag("camara", schedule="30 2 * * 1", tags=["demodados"]):
        votacoes = etl_group(CONFIG_FILE, ETLS, "votacoes")
        votacoes >> etl_group(CONFIG_FILE, ETLS, "votos_deputados")

Etapas de cada entidade (deduzidas do YAML e do ``Etl``):

- ``extract``: se o ``Etl`` tem ``extract`` ou o source tem ``base_url``;
- ``transform``: se o ``Etl`` tem ``transform``;
- ``load``: se o ``Etl`` tem ``load`` ou o modo não é ``none``;
- ``check_bronze``: antes do ``load`` nos modos ``table`` e ``files``. Os dois
  fazem ``replace``, então um bronze vazio zeraria a raw.

Parâmetro ``steps`` (disparo manual): lista das etapas a executar. As demais são
puladas; por exemplo ``["transform", "load"]`` reprocessa o landing sem bater na
fonte. Todas as tasks usam ``none_failed`` para que etapa pulada não pule o
resto da DAG.

O ETL é montado dentro de cada task (``build_etl`` instancia ``PipelineConfig``,
que cria diretórios); no parse só o YAML é lido.
"""

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, Param, TaskGroup, get_current_context, task
from core import Etl, build_etl, load_yaml

ALL_STEPS = ("extract", "transform", "load")
START_DATE = datetime(2026, 9, 14)
_CHECKED_MODES = ("table", "files")


def source_dag(
    dag_id: str,
    *,
    schedule: str | None,
    tags: Sequence[str],
    description: str | None = None,
    **kwargs,
) -> DAG:
    """DAG com os defaults do projeto e o parâmetro ``steps``."""
    default_args = {"owner": "airflow", "retries": 2, **kwargs.pop("default_args", {})}
    return DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=list(tags),
        description=description,
        default_args=default_args,
        params={
            "steps": Param(
                list(ALL_STEPS),
                type="array",
                items={"type": "string", "enum": list(ALL_STEPS)},
                description="Etapas do ETL a executar; as demais são puladas.",
            )
        },
        **kwargs,
    )


def source_options(config_file: Path | str, entidade: str) -> dict:
    """Bloco do source no YAML, com ``load`` resolvido pelo default do arquivo."""
    cfg = load_yaml(config_file)
    src = dict(cfg["sources"][entidade] or {})
    src["load"] = src.get("load", cfg.get("load")) or "table"
    return src


def entity_steps(config_file: Path | str, etl: Etl, entidade: str) -> list[str]:
    """Etapas que a entidade tem de fato, na ordem de execução."""
    src = source_options(config_file, entidade)
    steps = []
    if etl.extract is not None or src.get("base_url"):
        steps.append("extract")
    if etl.transform is not None:
        steps.append("transform")
    if etl.load is not None or src["load"] != "none":
        steps.append("load")
    return steps


def _selected(step: str) -> None:
    steps = get_current_context()["params"].get("steps") or list(ALL_STEPS)
    if step not in steps:
        raise AirflowSkipException(f"Etapa '{step}' fora de params.steps={steps}")


def _has_data_rows(path: Path) -> bool:
    with open(path, encoding="utf-8") as f:
        return f.readline() != "" and f.readline().strip() != ""


def _check_bronze(config_file: Path | str, etl: Etl, entidade: str, mode: str) -> None:
    cfg = build_etl(config_file, entidade, etl).cfg
    if mode == "table":
        path = cfg.bronze_filepath
        if not path.is_file():
            raise FileNotFoundError(f"Bronze ausente, abortando carga: {path}")
        if not _has_data_rows(path):
            raise ValueError(f"Bronze vazio, abortando carga: {path}")
        return
    pattern = cfg.options.get("file_pattern", "*.csv")
    files = sorted(Path(cfg.bronze_dir).glob(pattern))
    if not any(_has_data_rows(f) for f in files):
        raise ValueError(f"Nenhum CSV com dados em {cfg.bronze_dir}/{pattern}")


def etl_group(
    config_file: Path | str,
    etls: dict[str, Etl],
    entidade: str,
    *,
    group_id: str | None = None,
) -> TaskGroup:
    """``TaskGroup`` de uma entidade com as etapas que ela tem."""
    etl = etls[entidade]
    steps = entity_steps(config_file, etl, entidade)
    mode = source_options(config_file, entidade)["load"]

    with TaskGroup(group_id=group_id or entidade) as group:
        previous = None
        for step in steps:
            if step == "load" and mode in _CHECKED_MODES:

                @task(task_id="check_bronze", trigger_rule="none_failed")
                def check_bronze():
                    _selected("load")
                    _check_bronze(config_file, etl, entidade, mode)

                current = check_bronze()
                if previous is not None:
                    previous >> current
                previous = current

            @task(task_id=step, trigger_rule="none_failed")
            def run_step(step: str = step):
                _selected(step)
                getattr(build_etl(config_file, entidade, etl), step)()

            current = run_step()
            if previous is not None:
                previous >> current
            previous = current
    return group
