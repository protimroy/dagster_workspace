# Dagster Workspace

Central workspace for Dagster-powered sports analytics and weather data pipelines. Each ETL project is a **git submodule** with its own virtual environment managed by [uv](https://docs.astral.sh/uv/).

## Projects

| Submodule | Database | Status | Description |
|-----------|----------|--------|-------------|
| [**nba_etl**](./nba_etl/) | `nba_research` | Active | NBA player stats ingestion, rolling feature computation, schedule/injury tracking |
| [**nfl_etl**](./nfl_etl/) | `nfl_analytics_dev` | Active | NFL historical data, live schedule/weather/injury/odds, game projections |
| [**nhl_etl**](./nhl_etl/) | — | In progress | NHL game data and stats |
| [**weather_toronto_etls**](./weather_toronto_etls/) | — | Active | Toronto Pearson Airport weather observations and NWP model data |

## Quick Start

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/protimroy/dagster_workspace.git
cd dagster_workspace

# Or init submodules in an existing clone
git submodule update --init --recursive

# Install a project's dependencies
cd nba_etl
uv sync

# Start Dagster (single project)
uv run dagster dev
```

## Architecture

```
                          dagster_workspace/
                          ├── workspace.yaml
                          │
  ┌───────────────────────┼───────────────────────┬──────────────────────┐
  │                       │                       │                      │
  nba_etl/          nfl_etl/              nhl_etl/         weather_toronto_etls/
  │                       │                       │                      │
  │  NBA Stats API        │  NFLVerse             │  NHL API             │  Environment Canada
  │  → staging.*          │  ESPN / Odds API      │  → staging.*         │  → observations
  │  → projections.*      │  → stg_* / dim_*      │                      │  → nwp forecasts
  │  → features           │  → fact_* / agg_*     │                      │
  │                       │                       │                      │
  └───────┬───────────────┴───────────┬───────────┴──────────────────────┘
          │                           │
          ▼                           ▼
     PostgreSQL (100.68.208.24)    Dagster Daemon (100.103.143.89)
     ├── nba_research                 Schedules, sensors, runs
     ├── nfl_analytics_dev
     └── ...
```

## NBA ETL — Pipeline Detail

The NBA pipeline feeds the **nba_predictions** distributional projection engine.

| Schedule | Cron (ET) | Job | What it does |
|----------|-----------|-----|--------------|
| `daily_nba_ingestion` | 10:00 AM daily | `daily_ingestion_job` | Fetches recent player/team game stats, schedule, injury report from NBA Stats API |
| `daily_nba_features` | 10:30 AM daily | `feature_computation_job` | Computes player rolling features (5/10/20 game windows) and team defensive features |
| `live_game_tracking` | Every 5 min, 7 PM–1 AM | `live_tracking_job` | Live scoreboard + box scores during games (off by default) |

**Data flow:**

```
NBA Stats API → staging.player_game_stats     (1.4M+ rows)
              → staging.games                  (72K+ rows)
              → projections.upcoming_games
              → projections.injury_reports
                        ↓
              → projections.player_rolling_features
              → projections.team_defensive_features
                        ↓  (consumed by nba_predictions on local GPU)
              → projections.projected_stat_lines
                   NB marginals, Gaussian Copula, ZINB,
                   rate decomposition, regime detection,
                   quantile intervals (p10–p90), DD%/TD%
```

## NFL ETL — Pipeline Detail

| Source | Data | Cost |
|--------|------|------|
| **NFLVerse** | Historical schedules, stats, play-by-play (2010–present) | Free |
| **ESPN API** | Live schedule, teams, injuries | Free |
| **Open-Meteo** | Weather forecasts by lat/lon | Free |
| **The Odds API** | Multi-book betting lines | 500 req/month free |

## Workspace Structure

```
dagster_workspace/
├── workspace.yaml              # Dagster workspace config (lists active modules)
├── .gitmodules                 # Submodule definitions
├── nba_etl/                    # [submodule] NBA ingestion + features
│   ├── nba_etl/
│   │   ├── __init__.py         # Dagster Definitions
│   │   ├── assets/             # ingestion, features, live
│   │   ├── backend/db/         # SQLAlchemy models, session, persistence
│   │   ├── data_sources/       # NBA Stats API client (curl-based)
│   │   ├── models/             # Feature engineering SQL
│   │   ├── jobs.py
│   │   └── schedules.py
│   ├── scripts/init_db.py      # One-time schema setup
│   └── pyproject.toml
├── nfl_etl/                    # [submodule] NFL analytics
├── nhl_etl/                    # [submodule] NHL analytics
├── weather_toronto_etls/       # [submodule] Weather data
├── history/                    # Dagster run history
├── logs/                       # Application logs
└── storage/                    # Dagster storage
```

## Infrastructure

| Service | Host | Port |
|---------|------|------|
| **PostgreSQL** | 100.68.208.24 | 5432 |
| **Dagster Daemon** | 100.103.143.89 | 3000 |
| **Qdrant** (vectors) | 100.68.208.24 | 6333 |
| **Redis** (cache) | 100.68.208.24 | 6379 |

## Submodule Cheat Sheet

```bash
# Pull latest for all submodules
git submodule update --remote --merge

# Pull latest for one submodule
cd nba_etl && git pull origin main && cd ..

# After cloning — init submodules
git submodule update --init --recursive

# Check submodule status
git submodule status
```

