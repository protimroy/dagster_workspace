# Dagster Workspace

Central workspace for Dagster-powered sports analytics and weather data pipelines. Each ETL project is a **git submodule** with its own virtual environment managed by [uv](https://docs.astral.sh/uv/).

## Projects

| Submodule | Database | Status | Description |
|-----------|----------|--------|-------------|
| [**mlb_etl**](./mlb_etl/) | `mlb_research` | Active | MLB three-stage pipeline (live → intermediary → historical), Statcast, Originator analytics engine |
| [**nba_etl**](./nba_etl/) | `nba_research` | Active | NBA player stats ingestion, rolling feature computation, schedule/injury tracking |
| [**nfl_etl**](./nfl_etl/) | `nfl_analytics_dev` | Experimental | NFL historical data, live schedule/weather/injury/odds, game projections |
| [**nhl_etl**](./nhl_etl/) | `nhl_research` | Active | NHL game data and stats |
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
  ┌──────────┬────────────┼───────────────────────┬──────────────────────┐
  │          │            │                       │                      │
  mlb_etl/ nba_etl/    nfl_etl/            nhl_etl/       weather_toronto_etls/
  │          │            │                       │                      │
  │ MLB API  │ NBA Stats  │  NFLVerse             │  NHL API             │  Environment Canada
  │ Savant   │  → stg.*   │  ESPN / Odds API      │  → staging.*         │  → observations
  │ Open-Meteo → proj.*   │  → stg_* / dim_*      │                      │  → nwp forecasts
  │ ESPN     │  → feat.*  │  → fact_* / agg_*     │                      │
  │          │            │                       │                      │
  └────┬─────┴────────────┴───────────┬───────────┴──────────────────────┘
       │                              │
       ▼                              ▼
  PostgreSQL (100.68.208.24)     Dagster Daemon (100.103.143.89)
  ├── mlb_research                  Schedules, sensors, runs
  ├── nba_research
  ├── nfl_analytics_dev
  └── ...
```

## MLB ETL — Pipeline Detail

Three-stage architecture: **Live** (pollable) → **Intermediary** (48 h hold for stat corrections) → **Historical** (immutable).

| Source | Data | Cost |
|--------|------|------|
| **MLB Stats API** | Schedule, rosters, game feeds, standings, boxscores, umpires | Free |
| **ESPN API** | Live scoreboard / game state detection | Free |
| **Baseball Savant** | Pitch-level Statcast (via pybaseball) | Free |
| **Open-Meteo** | Park-specific weather forecasts | Free |

| Schedule | Cron (ET) | Job | What it does |
|----------|-----------|-----|--------------|
| `mlb_reference_schedule` | 10:00 AM | `mlb_reference_job` | Teams, players, seasons |
| `mlb_pregame_schedule` | 10:30 AM | `mlb_pregame_job` | Schedule, probable pitchers, bullpen, umpires |
| `mlb_originator_schedule` | 11:00 AM | `mlb_originator_job` | Platoon splits, BvP, umpire zones, game lines, prop projections |
| `mlb_postgame_night_schedule` | 11:00 PM | `mlb_postgame_ingestion_job` | Promote Final games to intermediary |
| `mlb_historical_promotion_schedule` | 3:00 AM | `mlb_historical_promotion_job` | Promote verified intermediary → historical |
| `mlb_stat_corrections_schedule` | Every 6 h | `mlb_stat_corrections_job` | Re-fetch boxscores, diff stat changes |

**30 assets** across 5 groups (`mlb_pregame`, `mlb_live`, `mlb_postgame`, `mlb_projections`, `mlb_analytics`), 13 jobs, 13 schedules.

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
├── mlb_etl/                    # [submodule] MLB three-stage pipeline + Originator
│   ├── __init__.py             # Dagster Definitions (load_assets_from_modules)
│   ├── data_sources.py         # MLB Stats API, ESPN, Statcast clients
│   ├── assets/                 # pregame, live, postgame, originator, backtesting
│   ├── backend/db/             # SQLAlchemy models (38), session, persistence
│   ├── scripts/                # init_db, backfill_*, check_tables
│   ├── jobs.py
│   └── schedules.py
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

