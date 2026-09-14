"""DAGs da factory ``include.utils.etl_dag``: etapas de cada entidade na ordem.

Rode dentro do scheduler (os volumes do my_ingestion e do the_dw só existem lá):
    docker exec $(docker ps -qf name=scheduler) bash -c \
        'cd /usr/local/airflow && pytest -q tests/dags/'
"""

import logging
from collections import defaultdict

import pytest
from airflow.models import DagBag

ORDER = ("extract", "transform", "check_bronze", "load")


def _factory_dags():
    logging.getLogger("airflow").disabled = True
    try:
        bag = DagBag(include_examples=False)
    finally:
        logging.getLogger("airflow").disabled = False
    return [d for d in bag.dags.values() if "steps" in d.params]


FACTORY_DAGS = _factory_dags()


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
