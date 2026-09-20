# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow orchestration project (repo `my_orchestrator`) on Astro Runtime 3.0-10 (Airflow 3.0.6, Python 3.12). It only orchestrates: ingestion code lives in the `my_ingestion` monorepo and transformation in the `my_analytics` dbt project. Neither is a submodule: dev mounts `~/workspace` by volume, prod bakes the tip of each repo's default branch into the image (CI/CD on merge). Data follows a medallion architecture: landing/bronze files in the lake → `raw_<fonte>.*` tables → dbt staging/intermediate/marts. Code, comments and docs are in Portuguese.

Two environments, selected by environment variables only: `dev` = this machine (CLI of my_ingestion and this local Airflow; raw loads go to database `ingestion_sandbox`, dbt runs on `analytics_dev`) and `prod` = the `atb` server (`/srv/airflow`, deployed by `.github/workflows/deploy.yml` on every merge). Same DAG code in both; only the `.env` changes.

## Common Commands

```bash
cp .env.example .env                       # required: settings fails on import without LAKE_ROOT/SEEDS_ROOT/DB__DEV__*
astro dev start                            # UI http://localhost:8090 (metadata db on 5436)
astro dev restart                          # rebuild after changing requirements.txt / Dockerfile
docker exec $(docker ps -qf name=scheduler) bash -c 'cd /usr/local/airflow && pytest -q tests/dags/'   # import errors, tags, retries >= 2 (astro dev pytest has no volumes: migrated DAGs fail with No module named 'core')
gh run watch                               # prod: follow the Deploy prod workflow after a merge (no manual deploy)
ssh atb head -2 /srv/airflow/DEPLOYED.txt  # prod: what is live
```

Run the `smoke_my_ingestion` DAG after start: it checks imports, `.env`, database and my_analytics mount.

## Architecture

### Repository Structure

- `dags/` — one file per pipeline (`@dag`/`@task` style)
- `include/my_ingestion/` — mount point, gitignored (not a submodule); `src/` is on `PYTHONPATH` (Dockerfile), so DAGs import `core`, `pipelines`, `settings` **without package prefix** (`from core import build_etl`). The package is not pip-installed; only its deps are (see `requirements.txt`).
- `include/utils/` — Airflow-side code: `etl_dag.py` (DAG factory), `db_interactors.py` (**now unused**: its `to_sql` loads, upserts and `move_files_after_loading` served solar/weather, which moved to `GenericETL.load()`; safe to delete), `logger_cfg.py`
- No git submodules remain. The legacy `include/` submodules were removed on 2026-09-14; their code lives in `my_ingestion`. The DAGs that imported them are gone too (the NHL ones were the last, replaced by `dag_nhl_stats.py`), so `dags/.airflowignore` is empty.
- `dbt/my_analytics/` — mount point, gitignored (not a submodule); single dbt project for all domains (schemas derived from model path by `generate_schema_name`). Run by Cosmos (`DbtDag`) with the `dbt_venv` executable.
- `deploy/build_prod.sh` / `deploy/estado.sh` / `deploy/verify_prod.sh` — called by `.github/workflows/deploy.yml` on the atb runner: assemble `/srv/airflow` from three checkouts, then check import errors / DAG count / `0.0.0.0` after the restart
- `deploy/prod/` — prod-only files swapped in by `deploy/build_prod.sh`: compose override, `.astro/config.yaml`, `start.sh`, `.env.example`
- `.github/workflows/` — `deploy.yml` (self-hosted `atb-airflow`, on push to `main`, `repository_dispatch: deploy` from my_ingestion/my_analytics, or manual) and `pr.yml` (syntax)
- `tests/dags/` — DagBag validation
- `airflow_settings.yaml` — local connections/variables (not for prod)
- `docker-compose.override.yml` — dev only: bind mounts of `~/workspace/my_ingestion/src`, `~/workspace/my_analytics`, the lake and `~/.secrets`

### Dev bind mounts

In dev the working trees of `~/workspace/my_ingestion/src` and `~/workspace/my_analytics` are mounted at `include/my_ingestion/src` and `dbt/my_analytics`, so edits there are live in Airflow. Without `~/workspace` checked out, the migrated DAGs fail to import. Only `src/` of my_ingestion is mounted in dev on purpose: its own `.env` (localhost, `/media/...`) must not be read inside the container; configuration comes exclusively from this repo's `.env`. Prod also ships `scripts/` (raw maintenance run with `docker exec`), never the `.env`.

### Configuration (`.env`)

Astro injects `.env` into every container (gitignored and dockerignored; template in `.env.example`). Keys: `ENV`, `LAKE_ROOT=/usr/local/airflow/mylake`, `SEEDS_ROOT=/usr/local/airflow/dbt/my_analytics/seeds`, `DB__DEV__*` / `DB__PROD__*` (pydantic-settings nested delimiter `__`), pipeline credentials (`APSYSTEMS_*`, `OPENWEATHER_API_KEY`, `GOOGLE_CREDENTIALS_FILE`, `URL_FINANCE__*`) and `SELENIUM_REMOTE_URL`. `settings.py` validates the active profile on import and, in dev, requires `DB__DEV__NAME=ingestion_sandbox`. The Airflow connection `postgres_dw` is defined in the same `.env` as `AIRFLOW_CONN_POSTGRES_DW` and is the dbt database (Cosmos, `export_table_to_csv`). Every raw load (`GenericETL` and `include/utils/db_interactors.py`) uses `settings.db_target`. In dev they are different databases (`ingestion_sandbox` vs `analytics_dev`; raw reaches `analytics_dev` via my_ingestion's `scripts/raw_copy.sh promote`); in prod both point to `analytics_prod`. The env var wins over the entry in the metastore and in `airflow_settings.yaml`.

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
- Exceptions without raw load (atacadao, atacadao_historico, investimentos_fgc) call the my_ingestion function in a plain `@task`; `fundos_imobiliarios` runs `python -m ...run` via `BashOperator` because its logic lives in `__main__`. Solar and weather were `load: none` with the upsert written in the DAG; they are now ordinary `GenericETL` sources (`load: table`, full refresh from the landing), so the DAG only wires the steps.
- Credentials from `settings` (env), not `Variable.get`. No `setup_logger()` (Airflow configures the root logger).
- New DAGs start with `schedule=None  # em validação...; original "<cron>"` and get the schedule only after passing validation (`airflow dags test <dag_id> --dagfile-path ...`, compared against the migrated copies in `analytics_dev`).
- Table names are **aligned** with my_analytics since my_ingestion `b3c79f3`: sources the dbt already read load into those tables (`raw_camara.raw_camara_*`, `raw_senado.raw_senado_*`, `raw_apsystem`, `raw_vide_editora.vide_raw_home_featured`, `raw_google_sheets`). `raw_<fonte>.<entidade>` is only for new sources.

### Runtime requirements

- `packages.txt`: `poppler-utils` (Avenue PDFs via `pdftotext`).
- `SELENIUM_REMOTE_URL` in `.env` (solar, fundos imobiliários): the image has no Chrome; dev uses the `selenium_container` at `http://host.docker.internal:4444/wd/hub`. Merged in my_ingestion `main`: `9a68a75` remote driver, `c3ab58d` new APsystems report iframe, `8bcde24` `--disable-dev-shm-usage` for FII (the Selenium container has 64 MB of /dev/shm).
- `raw_apsystem.solar_daily_energy` / `solar_hourly_energy` no longer need their PKs on `date` / `datetime`: the load is a full refresh, not an upsert. The existing unique indexes are harmless and can stay.
- Solar and weather keep their JSON history in the landing (`raw/solar_project`, `raw/weather_project`): the transform rebuilds the tables from it, so **nothing may move those files after the load**. my_ingestion's `scripts/lake_migra_clima_solar.sh` does the one-time move from the old `staging/` + `bronze/` layout and must run on `/usr/local/airflow/mylake` before the first prod run.
- `senado_status` copies the e-Cidadania `paginas` bronze into the Senado `parameter_dir` before running (link not declared in the YAMLs).

### Key Dependencies & Pinning

`pandas==2.1.4` stays pinned: Airflow 3.0.6 uses SQLAlchemy 1.4 and pandas ≥ 2.2 requires SQLAlchemy 2 ("Engine has no attribute 'cursor'"). `my_ingestion` itself runs pandas 3.x in its own venv; the in-process choice here is validated by the smoke DAG. Deps of my_ingestion are mirrored in `requirements.txt` (block "deps do include/my_ingestion") with the versions from its `uv.lock`.

### Airflow Connections (local)

- `postgres_dw` → from `AIRFLOW_CONN_POSTGRES_DW` in `.env`: `host.docker.internal:5435`, database `analytics_dev` (Cosmos; raw loads do not use it)
- `demodadosdw` → legacy database `demodados`, **which no longer exists** (only `analytics_dev`, `ingestion_sandbox` and `metabase` remain on 5435). Legacy DAGs that depended on the old `postgres`/`demodados` databases are already broken.
- `openweather_conn` → HTTP to `api.openweathermap.org`

### Prod (`atb`)

Astro project at `/srv/airflow` on `atb`. **The default branch is production**: every merged PR in this repo (`main`), `my_ingestion` (`main`) or `my_analytics` (`main`) runs `.github/workflows/deploy.yml` on the self-hosted runner `atb-airflow` (user `github-runner`; the other two repos trigger it via `repository_dispatch` with the `DEPLOY_DISPATCH_TOKEN` secret). The job checks out the three tips (private my_ingestion via the `INGESTION_READ_TOKEN` secret, nothing stored on atb), runs `deploy/build_prod.sh` (git archive of the whole repo, i.e. the same DAGs as dev minus `dags/.airflowignore`; `deploy/prod/*`, `DEPLOYED.txt`, local `rsync --delete` never touching `.env`, the auth passwords file, `dbt_packages/` or the `.deploy-*.sha256` state), then only if `deploy/estado.sh` detects a change since the last successful deploy: `/srv/airflow/start.sh restart` (image/config/plugins/`.env` changed) and `dbt deps` via `docker exec` in the scheduler (`package-lock.yml` changed), and always `deploy/verify_prod.sh`. Like dev, prod reads DAGs, my_ingestion and the dbt project from disk (Astro binds `dags/`, `include/`, `plugins/`; the prod override binds `dbt/my_analytics`), so code-only deploys are a plain rsync with no restart; the dag-processor `refresh_interval` is 60 s in prod. `workflow_dispatch` has a `restart` input to force it; pushes touching only `*.md`/`docs/` do not deploy. No pinned SHAs, no manual deploy. Never push to `main`: branch → PR → manual approval/merge. A DAG that needs new my_ingestion code is merged only after the my_ingestion PR.

- Same image as dev (Runtime 3.0-10, Python 3.12). Code is baked into the image, but Astro also bind-mounts the project's `dags/` and `include/`, so containers read it from `/srv/airflow`; the scheduler (root) writes `__pycache__` there, which `rsync` excludes.
- `.env` lives only in `/srv/airflow/.env` (template `deploy/prod/.env.example`): `ENV=prod`, `DB__PROD__*` → `postgres-dwh:5432/analytics_prod` on `homelab-net`, `AIRFLOW_CONN_OPENWEATHER_CONN` (dev gets it from `airflow_settings.yaml`, which is not deployed), `SELENIUM_REMOTE_URL=http://selenium:4444/wd/hub` (Selenium service in the prod override).
- Lake = SeaweedFS buckets FUSE-mounted at `/srv/lake/buckets` by the homelab `seaweedfs-mount` service, bound to `/usr/local/airflow/mylake`. `start.sh` refuses to start if it is not mounted.
- UI at `http://100.82.7.107:8080`, simple auth user `admin`.

### Known cross-repo mismatches (not fixable here)

- Atacadão: my_ingestion writes CSVs only (no DB load), but my_analytics still reads `raw_atacadao.atacadao_raw`.
- `investimentos_fgc` SQL reads `intermediate.int_renda_fixa`; my_analytics builds `intermediate_financas.int_renda_fixa` (DAG fails).
- `atacadao_historico` writes `minha_inflacao.csv` with columns `Mês passado, Var`; my_analytics seed `seed_minha_inflacao.csv` has `Categoria, Mes`.
- Data from the old databases (`demodados`, `postgres`) does not migrate by itself to `analytics_dev` (upsert tables such as `openweather_daily` must be copied first).
