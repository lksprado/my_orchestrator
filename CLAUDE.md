# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow orchestration project (repo `my_orchestrator`) on Astro Runtime 3.0-10 (Airflow 3.0.6, Python 3.12). It only orchestrates: ingestion code lives in the `my_ingestion` monorepo and transformation in the `my_analytics` dbt project. Neither is a submodule: dev mounts `~/workspace` by volume, prod checks out the commits pinned in `deploy/versions.txt`. Data follows a medallion architecture: landing/bronze files in the lake → `raw_<fonte>.*` tables → dbt staging/intermediate/marts. Code, comments and docs are in Portuguese.

Two environments, selected by environment variables only: `dev` = this machine (CLI of my_ingestion and this local Airflow, database `analytics_dev`) and `prod` = the `atb` server (`homelab/stacks/airflow`). Same DAG code in both; only the `.env` changes.

## Common Commands

```bash
cp .env.example .env                       # required: settings fails on import without LAKE_ROOT/SEEDS_ROOT/DB__DEV__*
astro dev start                            # UI http://localhost:8090 (metadata db on 5436)
astro dev restart                          # rebuild after changing requirements.txt / Dockerfile
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'   # import errors, tags, retries >= 2 (astro dev pytest has no volumes: migrated DAGs fail with No module named 'core')
deploy/checkout_versions.sh <dest>        # prod: clone my_ingestion/my_analytics at the SHAs in deploy/versions.txt
```

Run the `smoke_my_ingestion` DAG after start: it checks imports, `.env`, database and my_analytics mount.

## Architecture

### Repository Structure

- `dags/` — one file per pipeline (`@dag`/`@task` style)
- `include/my_ingestion/` — mount point, gitignored (not a submodule); `src/` is on `PYTHONPATH` (Dockerfile), so DAGs import `core`, `pipelines`, `settings` **without package prefix** (`from core import build_etl`). The package is not pip-installed; only its deps are (see `requirements.txt`).
- `include/utils/` — Airflow-side code: `etl_dag.py` (DAG factory), `db_interactors.py` (loads via connection `postgres_dw`, upserts, `move_files_after_loading`), `logger_cfg.py`
- No git submodules remain. The legacy `include/` submodules were removed on 2026-09-14; their code lives in `my_ingestion`. DAGs that imported them are kept as reference but listed in `dags/.airflowignore` until migrated.
- `dbt/my_analytics/` — mount point, gitignored (not a submodule); single dbt project for all domains (schemas derived from model path by `generate_schema_name`). Run by Cosmos (`DbtDag`) with the `dbt_venv` executable.
- `deploy/prod-dags.txt` — allowlist of DAGs promoted to prod
- `deploy/versions.txt` + `deploy/checkout_versions.sh` — exact `my_ingestion`/`my_analytics` commits prod runs; promoting = bumping a SHA in its own commit
- `tests/dags/` — DagBag validation
- `airflow_settings.yaml` — local connections/variables (not for prod)
- `docker-compose.override.yml` — dev only: bind mounts of `~/workspace/my_ingestion/src`, `~/workspace/my_analytics`, the lake and `~/.secrets`

### Dev bind mounts

In dev the working trees of `~/workspace/my_ingestion/src` and `~/workspace/my_analytics` are mounted at `include/my_ingestion/src` and `dbt/my_analytics`, so edits there are live in Airflow. Without `~/workspace` checked out, the migrated DAGs fail to import. Only `src/` of my_ingestion is mounted on purpose: its own `.env` (localhost, `/media/...`) must not be read inside the container; configuration comes exclusively from this repo's `.env`.

### Configuration (`.env`)

Astro injects `.env` into every container (gitignored and dockerignored; template in `.env.example`). Keys: `ENV`, `LAKE_ROOT=/usr/local/airflow/mylake`, `SEEDS_ROOT=/usr/local/airflow/dbt/my_analytics/seeds`, `DB__DEV__*` / `DB__PROD__*` (pydantic-settings nested delimiter `__`), pipeline credentials (`APSYSTEMS_*`, `OPENWEATHER_API_KEY`, `GOOGLE_CREDENTIALS_FILE`, `URL_FINANCE__*`) and `SELENIUM_REMOTE_URL`. `settings.py` validates the active profile on import and, in dev, requires `DB__DEV__NAME=analytics_dev`. The Airflow connection `postgres_dw` is defined in the same `.env` as `AIRFLOW_CONN_POSTGRES_DW` and must point to the same database as the active profile (Cosmos and `include/utils` use the connection; `GenericETL` loads use `settings.db_target`). The env var wins over the entry in the metastore and in `airflow_settings.yaml`.

### DAG Pattern (one DAG per my_ingestion source)

```python
"""<Fonte>: <o que coleta>.

DAG do Airflow montada por include/utils/etl_dag.py.
"""

from include.utils.etl_dag import etl_group, source_dag
from pipelines.<dominio>.<fonte>.<fonte>_etl import CONFIG_FILE, ETLS

with source_dag("<fonte>", schedule="30 2 * * 1", tags=["<dominio>"]) as dag:
    votacoes = etl_group(CONFIG_FILE, ETLS, "votacoes")
    votacoes >> etl_group(CONFIG_FILE, ETLS, "votos_deputados")
```

- `include/utils/etl_dag.py`: `source_dag` sets the defaults (retries 2, no catchup, `max_active_runs=1`) and the `steps` param; `etl_group` builds a `TaskGroup` per entity with only the steps it has (`extract` if the `Etl` has one or the source has `base_url`; `transform` if the `Etl` has one; `load` unless `load: none`) plus `check_bronze` before `load` in `table`/`files` modes (both `replace`, an empty bronze would wipe the raw).
- The ETL is built **inside** each task (`build_etl` creates directories); only the YAML is read at parse.
- Manual trigger with `steps=["transform","load"]` reprocesses the landing without hitting the source; all tasks use `none_failed` so a skipped step does not skip the rest.
- **Every DAG file must contain the words "airflow" and "dag"**: DagBag safe mode silently skips files without them. Factory-only files mention Airflow in the docstring. `tests/dags/test_etl_dag.py` fails if a non-ignored file yields no DAG.
- Exceptions without raw load (atacadao, atacadao_historico, investimentos_fgc) call the my_ingestion function in a plain `@task`; `fundos_imobiliarios` runs `python -m ...run` via `BashOperator` because its logic lives in `__main__`. `load: none` sources (solar, weather) load in the DAG with `include/utils/db_interactors.py` + upsert SQL.
- Credentials from `settings` (env), not `Variable.get`. No `setup_logger()` (Airflow configures the root logger).
- New DAGs start with `schedule=None  # em validação...; original "<cron>"` and get the schedule only after passing validation (`airflow dags test <dag_id> --dagfile-path ...`, compared against the migrated copies in `analytics_dev`); results in README "Validação das DAGs".
- Table names are **aligned** with my_analytics since my_ingestion `b3c79f3`: sources the dbt already read load into those tables (`raw_camara.raw_camara_*`, `raw_senado.raw_senado_*`, `raw_apsystem`, `raw_vide_editora.vide_raw_home_featured`, `raw_google_sheets`). `raw_<fonte>.<entidade>` is only for new sources.

### Runtime requirements

- `packages.txt`: `poppler-utils` (Avenue PDFs via `pdftotext`).
- `SELENIUM_REMOTE_URL` in `.env` (solar, fundos imobiliários): the image has no Chrome; dev uses the `selenium_container` at `http://host.docker.internal:4444/wd/hub`. Merged in my_ingestion `main`: `9a68a75` remote driver, `c3ab58d` new APsystems report iframe, `8bcde24` `--disable-dev-shm-usage` for FII (the Selenium container has 64 MB of /dev/shm).
- `raw_apsystem.solar_daily_energy` / `solar_hourly_energy` must have PKs on `date` / `datetime` (upsert `ON CONFLICT`); `dag_solar.py` reads the schema from `solar_config.yml`.
- `senado_status` copies the e-Cidadania `paginas` bronze into the Senado `parameter_dir` before running (link not declared in the YAMLs).

### Key Dependencies & Pinning

`pandas==2.1.4` stays pinned: Airflow 3.0.6 uses SQLAlchemy 1.4 and pandas ≥ 2.2 requires SQLAlchemy 2 ("Engine has no attribute 'cursor'"). `my_ingestion` itself runs pandas 3.x in its own venv; the in-process choice here is validated by the smoke DAG. Deps of my_ingestion are mirrored in `requirements.txt` (block "deps do include/my_ingestion") with the versions from its `uv.lock`.

### Airflow Connections (local)

- `postgres_dw` → from `AIRFLOW_CONN_POSTGRES_DW` in `.env`: `host.docker.internal:5435`, database `analytics_dev` (Cosmos + `include/utils`)
- `demodadosdw` → legacy database `demodados`, **which no longer exists** (only `analytics_dev` and `metabase` remain on 5435). Legacy DAGs that depended on the old `postgres`/`demodados` databases are already broken.
- `openweather_conn` → HTTP to `api.openweathermap.org`

### Prod (`atb`)

Not automated yet. When promoting a DAG (`deploy/prod-dags.txt`), the homelab Airflow needs: `.env` with `ENV=prod` + `DB__PROD__*` + secrets; the same `PYTHONPATH`; `deploy/checkout_versions.sh` output mounted at the same container paths; lake at `/usr/local/airflow/mylake`; an image with Python 3.12 (Runtime 3.3-2 defaults to 3.14, my_ingestion pins `<3.13`).

### Known cross-repo mismatches (not fixable here)

- Atacadão: my_ingestion writes CSVs only (no DB load), but my_analytics still reads `raw_atacadao.atacadao_raw`.
- `investimentos_fgc` SQL reads `intermediate.int_renda_fixa`; my_analytics builds `intermediate_financas.int_renda_fixa` (DAG fails).
- `atacadao_historico` writes `minha_inflacao.csv` with columns `Mês passado, Var`; my_analytics seed `seed_minha_inflacao.csv` has `Categoria, Mes`.
- NHL `param_schema: staging` in `nhl_config.yml`, but my_analytics builds `vw_stg_request_*` in `staging_nhl`.
- Data from the old databases (`demodados`, `postgres`) does not migrate by itself to `analytics_dev` (upsert tables such as `openweather_daily` must be copied first).
