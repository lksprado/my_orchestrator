# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow orchestration project (repo `my_orchestrator`) on Astro Runtime 3.0-10 (Airflow 3.0.6, Python 3.12). It only orchestrates: ingestion code lives in the `my_ingestion` monorepo and transformation in the `the_dw` dbt project (remote `my_datawarehouse`). Neither is a submodule: dev mounts `~/workspace` by volume, prod checks out the commits pinned in `deploy/versions.txt`. Data follows a medallion architecture: landing/bronze files in the lake → `raw_<fonte>.*` tables → dbt staging/intermediate/marts. Code, comments and docs are in Portuguese.

Two environments, selected by environment variables only: `dev` = this machine (CLI of my_ingestion and this local Airflow, database `analytics_dev`) and `prod` = the `atb` server (`homelab/stacks/airflow`). Same DAG code in both; only the `.env` changes.

## Common Commands

```bash
git submodule update --init --recursive   # after fresh clone (legacy include/ submodules only)
cp .env.example .env                       # required: settings fails on import without LAKE_ROOT/SEEDS_ROOT/DB__DEV__*
astro dev start                            # UI http://localhost:8090 (metadata db on 5436)
astro dev restart                          # rebuild after changing requirements.txt / Dockerfile
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'   # import errors, tags, retries >= 2 (astro dev pytest has no volumes: migrated DAGs fail with No module named 'core')
deploy/checkout_versions.sh <dest>        # prod: clone my_ingestion/the_dw at the SHAs in deploy/versions.txt
```

Run the `smoke_my_ingestion` DAG after start: it checks imports, `.env`, database and the_dw mount.

## Architecture

### Repository Structure

- `dags/` — one file per pipeline (`@dag`/`@task` style)
- `include/my_ingestion/` — mount point, gitignored (not a submodule); `src/` is on `PYTHONPATH` (Dockerfile), so DAGs import `core`, `pipelines`, `settings` **without package prefix** (`from core import build_etl`). The package is not pip-installed; only its deps are (see `requirements.txt`).
- `include/utils/` — Airflow-side helpers kept here: `db_interactors.py` (loads via connection `postgres_dw`, upserts, `move_files_after_loading`), `logger_cfg.py`
- `include/{local_setup,Solar,openweather,nhl_extraction,vide,inflation,finance}/` — **legacy submodules, being phased out**. Only DAGs not yet migrated import them. Do not add new code there.
- `dbt/the_dw/` — mount point, gitignored (not a submodule); single dbt project for all domains (schemas derived from model path by `generate_schema_name`). Run by Cosmos (`DbtDag`) with the `dbt_venv` executable.
- `deploy/prod-dags.txt` — allowlist of DAGs promoted to prod
- `deploy/versions.txt` + `deploy/checkout_versions.sh` — exact `my_ingestion`/`the_dw` commits prod runs; promoting = bumping a SHA in its own commit
- `tests/dags/` — DagBag validation
- `airflow_settings.yaml` — local connections/variables (not for prod)
- `docker-compose.override.yml` — dev only: bind mounts of `~/workspace/my_ingestion/src`, `~/workspace/the_dw`, the lake and `~/.secrets`

### Dev bind mounts

In dev the working trees of `~/workspace/my_ingestion/src` and `~/workspace/the_dw` are mounted at `include/my_ingestion/src` and `dbt/the_dw`, so edits there are live in Airflow. Without `~/workspace` checked out, the migrated DAGs fail to import. Only `src/` of my_ingestion is mounted on purpose: its own `.env` (localhost, `/media/...`) must not be read inside the container; configuration comes exclusively from this repo's `.env`.

### Configuration (`.env`)

Astro injects `.env` into every container (gitignored and dockerignored; template in `.env.example`). Keys: `ENV`, `LAKE_ROOT=/usr/local/airflow/mylake`, `SEEDS_ROOT=/usr/local/airflow/dbt/the_dw/seeds`, `DB__DEV__*` / `DB__PROD__*` (pydantic-settings nested delimiter `__`), pipeline credentials (`APSYSTEMS_*`, `OPENWEATHER_API_KEY`, `GOOGLE_CREDENTIALS_FILE`, `URL_FINANCE__*`). `settings.py` validates the active profile on import and, in dev, requires `DB__DEV__NAME=analytics_dev`. The Airflow connection `postgres_dw` is defined in the same `.env` as `AIRFLOW_CONN_POSTGRES_DW` and must point to the same database as the active profile (Cosmos and `include/utils` use the connection; `GenericETL` loads use `settings.db_target`). The env var wins over the entry in the metastore and in `airflow_settings.yaml`.

### DAG Pattern (migrated DAGs)

```python
from core import build_etl
from pipelines.<dominio>.<fonte>.<fonte>_etl import CONFIG_FILE, ETLS

@task
def extract():
    build_etl(CONFIG_FILE, "<entidade>", ETLS["<entidade>"]).extract()
```

- Build the ETL **inside** tasks, never in the `@dag` body (`PipelineConfig` creates directories; keep parse cheap).
- One `@task` per step (`extract` / `transform` / `load`); `load: none` sources (solar, weather) are loaded by `include/utils` + upsert SQL in the DAG.
- Credentials from `settings` (env), not `Variable.get`. No `setup_logger()` (Airflow configures the root logger).
- `default_args={"retries": 2}` and at least one tag (enforced by `tests/dags/test_dag_example.py`).
- Migrated so far: `dag_dbt_the_dw`, `dag_camara_votacoes`, `dag_weather_etl`, `dag_nhl_games_summary`, `dag_smoke_my_ingestion`. The pilots stay `schedule=None` until validated (`airflow dags test`, compared against the migrated copies in `analytics_dev`); results in README "Validação dos pilotos". A DAG listed in `.airflowignore` needs `--dagfile-path`. Everything else still uses the legacy submodules and the old `raw` schema; migrate one source at a time following the source README in `include/my_ingestion/src/pipelines/<dominio>/<fonte>/README.md`.

### Key Dependencies & Pinning

`pandas==2.1.4` stays pinned: Airflow 3.0.6 uses SQLAlchemy 1.4 and pandas ≥ 2.2 requires SQLAlchemy 2 ("Engine has no attribute 'cursor'"). `my_ingestion` itself runs pandas 3.x in its own venv; the in-process choice here is validated by the smoke DAG. Deps of my_ingestion are mirrored in `requirements.txt` (block "deps do include/my_ingestion") with the versions from its `uv.lock`.

### Airflow Connections (local)

- `postgres_dw` → from `AIRFLOW_CONN_POSTGRES_DW` in `.env`: `host.docker.internal:5435`, database `analytics_dev` (Cosmos + `include/utils`)
- `demodadosdw` → legacy database `demodados`, **which no longer exists** (only `analytics_dev` and `metabase` remain on 5435). Legacy DAGs that depended on the old `postgres`/`demodados` databases are already broken.
- `openweather_conn` → HTTP to `api.openweathermap.org`

### Prod (`atb`)

Not automated yet. When promoting a DAG (`deploy/prod-dags.txt`), the homelab Airflow needs: `.env` with `ENV=prod` + `DB__PROD__*` + secrets; the same `PYTHONPATH`; `deploy/checkout_versions.sh` output mounted at the same container paths; lake at `/usr/local/airflow/mylake`; an image with Python 3.12 (Runtime 3.3-2 defaults to 3.14, my_ingestion pins `<3.13`).

### Known cross-repo mismatches (not fixable here)

- the_dw source schemas vs my_ingestion `db_schema`: `raw_apsystem`≠`raw_solar`, `raw_vide_editora`≠`raw_vide_editorial`, `raw_google_sheets`≠`raw_google`.
- NHL `param_schema: staging` in `nhl_config.yml`, but the_dw builds `vw_stg_request_*` in `staging_nhl`.
- Data from the old databases (`demodados`, `postgres`) does not migrate by itself to `analytics_dev` (upsert tables such as `openweather_daily` must be copied first).
