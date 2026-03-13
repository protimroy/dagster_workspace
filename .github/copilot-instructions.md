# Copilot Instructions — Dagster Workspace

## Development Environment

- **Dagster is NOT installed on this machine.** Do not suggest running `dagster dev`, `dagster asset materialize`, or any other Dagster CLI commands locally. The Dagster daemon and webserver run on a remote VM.
- Code is developed locally and pushed/pulled to the VM via git. Suggest `git push` to deploy changes — never local Dagster commands.
- The local Python environment is used only for editing and linting — not for running pipelines.
- **Use `uv` for all virtual environment and dependency management.** Create envs with `uv venv`, install with `uv pip install -r requirements.txt`, and add packages with `uv pip install <pkg>`. Do not use `pip`, `virtualenv`, or `conda` directly.

## Database

- The PostgreSQL database runs on a **separate VM** from both the dev machine and the Dagster VM. **Never use `localhost` as a database host default.**
- Always connect via environment variables. The standard set is:
  ```
  DB_HOST      # required — always set this, no localhost fallback
  DB_PORT      # default: 5432
  DB_NAME
  DB_USER
  DB_PASSWORD
  ```
- Database URL construction must always read from `DB_HOST`:
  ```python
  host = os.getenv("DB_HOST")  # No fallback to "localhost"
  ```
- Use SQLAlchemy 2.0+ with `psycopg[binary]` (psycopg3). Session management lives in `backend/db/session.py` and reads env vars via `python-dotenv`.

## New ETL Structure

When creating a new Dagster ETL, follow the **flat file structure of `weather_toronto_etls`**:

```
<etl_name>/
├── __init__.py          # Dagster Definitions (defs = Definitions(...))
├── data_sources.py      # API client(s) — flat file, not a subdirectory
├── jobs.py              # define_asset_job per asset group
├── schedules.py         # ScheduleDefinition per schedule
├── resources.py         # RESOURCES = {}  (minimal — DB uses env vars directly)
├── pyproject.toml
├── requirements.txt
├── .env.example
├── assets/
│   ├── __init__.py
│   └── <domain>_etl.py  # one file per logical domain
├── backend/
│   ├── __init__.py
│   └── db/
│       ├── __init__.py
│       ├── models.py      # SQLAlchemy ORM models
│       ├── persistence.py # upsert / bulk-insert helpers
│       └── session.py     # get_db_session() context manager
└── scripts/
    ├── init_db.py
    ├── create_tables.sql
    └── check_tables.py
```

**Do not** use separate top-level subdirectory modules for `data_sources/`, `models/`, `utils/`, etc. (as seen in `nba_etl` and `nfl_etl`) — keep data sources and helpers as flat `.py` files at the package root or in `assets/`.

## Tech Stack

| Concern | Library |
|---|---|
| Orchestration | `dagster`, `dagster-webserver`, `dagster-postgres` |
| Database ORM | `sqlalchemy>=2.0` |
| DB driver | `psycopg[binary]>=3.1` |
| HTTP (async) | `httpx` |
| HTTP (sync) | `requests` |
| Data frames | `pandas`, `numpy` |
| Config / secrets | `python-dotenv`, `pydantic-settings` |
| Logging | `structlog` |
| Python | `>=3.11` |

## Dagster Patterns

- `Definitions` + `load_assets_from_modules` in `__init__.py`
- `define_asset_job` with `AssetSelection.assets(...)` in `jobs.py`
- `ScheduleDefinition` with `DefaultScheduleStatus.RUNNING` and `execution_timezone` in `schedules.py`
- `RESOURCES = {}` in `resources.py` — DB sessions are injected via env vars, not as Dagster resources
- Register each new ETL in the workspace-root `workspace.yaml` under `load_from: python_module`
- Asset groups follow the **lifecycle phase** pattern: `pregame`, `live`, `postgame`, `analytics` (or domain equivalents)

## Database Table Naming Conventions (NFL / multi-sport)

| Prefix | Layer | Examples |
|---|---|---|
| `ref_` | Reference / static lookups | `ref_divisions`, `ref_positions` |
| `dim_` | Dimension / master entities | `dim_teams`, `dim_players` |
| `stg_` | Staging — raw from source | `stg_nflverse_schedules` |
| `fact_` | Fact / processed events | `fact_games`, `fact_betting_lines` |
| `agg_` | Aggregate / pre-computed | `agg_team_season_stats` |

## Data Quality & Production Safety

- **Never generate or return mock/random data by default.** Use `allow_mock_data: bool = False` and fail fast in production:
  ```python
  except requests.RequestException as e:
      if self.allow_mock_data:
          ...
      raise  # Fail fast — no silent fallbacks
  ```
- Use upsert (INSERT … ON CONFLICT DO UPDATE) for incremental loads to avoid duplicates.

## Workspace Overview

| ETL | Status | Description |
|---|---|---|
| `nba_etl` | Active | NBA game data, player stats, odds |
| `nhl_etl` | Active | NHL three-stage pipeline (live → intermediary → historical) |
| `weather_toronto_etls` | Active | Toronto Pearson weather (EC OGC API + Open-Meteo NWP) |
| `nfl_etl` | Experimental | NFL analytics (commented out of `workspace.yaml`) |
