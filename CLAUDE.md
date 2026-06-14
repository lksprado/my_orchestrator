# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow orchestration project built on the Astronomer runtime (3.0-10 / Airflow 2.10.x). Manages 34+ pipelines across multiple domains: Brazilian political data (Câmara, Senado, e-Cidadania), NHL stats, solar energy, OpenWeather, book scraping (Vide Editorial), and inflation tracking (Atacadão). Data follows a medallion architecture: raw files → staging tables → DBT bronze/silver/gold layers.

## Common Commands

```bash
# Start/stop local Airflow
astro dev start
astro dev stop

# Run DAG tests
pytest tests/dags/

# Update all git submodules to latest
git submodule update --remote

# After fresh clone
git submodule update --init --recursive
```

**Airflow UI:** http://localhost:8080 | **API server:** http://localhost:8090

## Architecture

### Repository Structure

- `dags/` — Airflow DAG files (one file per pipeline)
- `include/` — Shared Python modules; each subdomain is a git submodule
  - `utils/` — Core shared utilities (`db_interactors.py`, `s3_cons.py`, `logger_cfg.py`)
  - `local_setup/` — `PipelineConfig`, `GenericETL`, `PostgreSQLManager`, YAML pipeline configs
  - `nhl_extraction/`, `Solar/`, `openweather/`, `inflation/`, `vide/` — domain ETL packages
- `dbt/` — Two DBT projects (both git submodules)
  - `my_datawarehouse/` — NHL, solar, inflation, books (selectors: `nhl`, `energia`, `inflation`, `livros`)
  - `demodadosdw/` — Brazilian political data (three-layer medallion, surrogate key `sk_parlamentar`)
- `tests/dags/` — DAG import validation and connection tests
- `airflow_settings.yaml` — Local dev connections and variables (not for prod)
- `docker-compose.override.yml` — Mounts local DBT projects and the datalake directory

### Git Submodules

All `include/` domain packages and both `dbt/` projects are git submodules with their own repos. Changes to those packages must be committed in their respective repos, then the pointer updated here.

### DAG Pattern

All DAGs use the `@dag` / `@task` decorator style:

```python
@dag(dag_id="...", schedule="...", tags=["domain"], default_args={"retries": 2, ...})
def my_pipeline():
    @task
    def extract(): ...
    @task
    def load(): ...
    extract() >> load()

dag = my_pipeline()
```

Every DAG must have at least one tag and `retries >= 2` (enforced by `tests/dags/test_dag_example.py`).

### ETL Data Flow

1. Extract from API/scraper → write CSV to `/usr/local/airflow/mylake/<domain>/staging/`
2. Load CSV to PostgreSQL staging table via `send_csv_df_to_db()` in `include/utils/db_interactors.py`
3. Move files staging → bronze via `move_files_after_loading()`
4. DBT transforms bronze → silver → gold (triggered by `TriggerDagRunOperator` or `DbtDag` via Cosmos)

Loads use INSERT … ON CONFLICT for idempotency. Solar and weather data are also backed up to S3.

### DBT / Cosmos

DBT runs use `astronomer-cosmos` (`DbtDag`). The two DBT projects share the same Postgres database but different schemas. The `dbt_env` Airflow variable controls target (`dev` by default).

### Key Dependencies & Pinning

`pandas==2.1.4` is pinned for compatibility with `sqlalchemy==1.4.54`. Using a newer pandas version causes "Engine has no attribute 'cursor'" errors. Do not bump either without testing the full ETL chain.

### Airflow Connections (local)

Defined in `airflow_settings.yaml`:
- `postgres_dw` → Postgres at `host.docker.internal:5435`, schema `postgres`
- `demodadosdw` → Postgres at `host.docker.internal:5435`, schema `demodados`
- `openweather_conn` → HTTP to `api.openweathermap.org`
- `aws_solar_weather` → AWS S3

### Airflow Variables

- `lake_base_dir`: `/usr/local/airflow/mylake` — root datalake path used by all ETL tasks
- `dbt_env`: `dev` — DBT target
- `apsystem_user` / `apsystem_pw`: Solar system credentials
- `openweather_api`: OpenWeather API token
