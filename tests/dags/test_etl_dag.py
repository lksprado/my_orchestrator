"""DAGs da factory ``include.utils.etl_dag``: etapas de cada entidade na ordem.

Rode dentro do scheduler (os volumes do my_ingestion e do the_dw só existem lá):
    docker exec $(docker ps -qf name=scheduler) bash -c \
        'cd /usr/local/airflow && pytest -q tests/dags/'
"""

import logging
from collections import defaultdict
from pathlib import Path

import pytest
from airflow.models import DagBag

ORDER = ("extract", "transform", "check_bronze", "load")


def _dagbag() -> DagBag:
    logging.getLogger("airflow").disabled = True
    try:
        return DagBag(include_examples=False)
    finally:
        logging.getLogger("airflow").disabled = False


BAG = _dagbag()
FACTORY_DAGS = [d for d in BAG.dags.values() if "steps" in d.params]


def _dag_files() -> list[Path]:
    folder = Path(BAG.dag_folder)
    ignore = folder / ".airflowignore"
    ignored = {
        line.strip()
        for line in (ignore.read_text().splitlines() if ignore.exists() else [])
        if line.strip() and not line.startswith("#")
    }
    return sorted(f for f in folder.glob("*.py") if f.name not in ignored)


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_every_dag_file_is_parsed(path):
    # O modo seguro do DagBag pula em silêncio arquivos sem "airflow" e "dag".
    locs = {Path(d.fileloc).resolve() for d in BAG.dags.values()}
    assert path.resolve() in locs, f"{path.name} não gerou nenhuma DAG"


def test_factory_dags_exist():
    assert FACTORY_DAGS, "nenhuma DAG com o parâmetro steps foi encontrada"


@pytest.mark.parametrize("dag", FACTORY_DAGS, ids=[d.dag_id for d in FACTORY_DAGS])
def test_entity_steps_in_order(dag):
    groups = defaultdict(dict)
    for t in dag.tasks:
        group, _, step = t.task_id.rpartition(".")
        if group and step in ORDER:
            groups[group][step] = t

    for group, steps in groups.items():
        present = [s for s in ORDER if s in steps]
        for prev, cur in zip(present, present[1:], strict=False):
            upstream = steps[cur].upstream_task_ids
            assert f"{group}.{prev}" in upstream, (
                f"{dag.dag_id}: {group}.{cur} deveria vir depois de {group}.{prev}"
            )
        if "load" in steps and "check_bronze" in steps:
            assert steps["load"].upstream_task_ids == {f"{group}.check_bronze"}


@pytest.mark.parametrize("dag", FACTORY_DAGS, ids=[d.dag_id for d in FACTORY_DAGS])
def test_factory_defaults(dag):
    assert dag.default_args.get("retries", 0) >= 2
    assert dag.tags
    assert dag.max_active_runs == 1
    assert not dag.catchup
