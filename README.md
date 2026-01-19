# Dagster Workspace 🏈⚙️

Central workspace for Dagster-powered NFL data pipelines.

## Projects

### [NFL ETL](./nfl_etl/)

A comprehensive NFL analytics platform that collects historical and live data, stores it in PostgreSQL, and generates game projections with betting recommendations.

**Goal**: Build a data-driven edge for NFL betting by:
1. Loading 15+ years of historical NFL data from nflverse
2. Fetching real-time schedule, weather, injury, and betting data  
3. Analyzing patterns, trends, and statistical anomalies
4. Generating actionable picks with confidence ratings

## Quick Start

```bash
cd nfl_etl

# Install dependencies with UV
uv sync

# Start Dagster
dagster dev

# Open http://localhost:3000
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NFL Analytics Data Platform                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   DATA SOURCES              STAGING (stg_*)           DIMENSIONS (dim_*)    │
│   ────────────              ───────────────           ──────────────────    │
│                                                                             │
│   NFLVerse ──────────────►  stg_nflverse_schedules   ──►  dim_teams        │
│   (Historical 2010-2026)    stg_nflverse_weekly_stats     dim_players      │
│                             stg_nflverse_rosters          dim_seasons      │
│                             stg_nflverse_pbp                               │
│                                                                             │
│   ESPN API ──────────────►  stg_espn_injuries                              │
│   (Live Data)                                                               │
│                                                                             │
│   The Odds API ──────────►  stg_odds_api_lines                             │
│   (Betting Lines)                                                           │
│                                                                             │
│   Open-Meteo ────────────►  stg_weather                                    │
│   (Weather)                                                                 │
│                                      │                                      │
│                                      ▼                                      │
│                            FACTS (fact_*)                                   │
│                            ───────────────                                  │
│                            fact_games                                       │
│                            fact_player_game_stats                          │
│                            fact_betting_lines                              │
│                            fact_user_bets                                  │
│                                      │                                      │
│                                      ▼                                      │
│                            AGGREGATES (agg_*)                              │
│                            ───────────────────                             │
│                            agg_team_season_stats                           │
│                            agg_player_season_stats                         │
│                            agg_records                                     │
│                            agg_betting_trends                              │
│                            agg_insights ──────► Qdrant (embeddings)        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Data | Cost |
|--------|------|------|
| **NFLVerse (nfl-data-py)** | Historical schedules, stats, play-by-play (2010-present) | Free |
| **ESPN API** | Live schedule, teams, injuries | Free (public) |
| **Open-Meteo** | Weather forecasts by lat/lon | Free |
| **The Odds API** | Multi-book betting lines | 500 req/month free |

## Database Schema

The database uses a layered architecture:

| Layer | Prefix | Purpose |
|-------|--------|---------|
| **Reference** | `ref_` | Static lookups (divisions, positions) |
| **Dimension** | `dim_` | Master entities (teams, players, seasons) |
| **Staging** | `stg_` | Raw data as imported from sources |
| **Fact** | `fact_` | Processed event data (games, stats, bets) |
| **Aggregate** | `agg_` | Pre-computed analytics (trends, records, insights) |

## Workspace Structure

```
dagster_workspace/
├── workspace.yaml           # Dagster workspace config
├── nfl_etl/                 # NFL Analytics ETL package
│   ├── assets/              # Dagster assets (ETL pipelines)
│   │   ├── historical_data_loader.py  # NFLVerse → staging
│   │   ├── schedule_etl.py            # ESPN schedule
│   │   ├── weather_etl.py             # Weather forecasts
│   │   ├── injury_etl.py              # Injury reports
│   │   ├── betting_etl.py             # Betting lines
│   │   ├── projection_engine.py       # Game projections
│   │   └── stats/                     # Comprehensive stats
│   ├── backend/             # Database layer
│   │   └── db/              # SQLAlchemy models & persistence
│   ├── scripts/             # SQL schema files
│   │   └── schema_v2.sql    # Complete database schema
│   ├── data_sources.py      # API clients
│   ├── jobs.py              # Dagster jobs
│   └── schedules.py         # Automated schedules
├── history/                 # Dagster run history
├── logs/                    # Application logs
└── storage/                 # Dagster storage
```

## Database Connection

```
Host: 100.68.208.24
Port: 5432
Database: nfl_analytics_dev
User: protim
```

Additional services:
- **Qdrant** (vectors): 100.68.208.24:6333
- **Redis** (cache): 100.68.208.24:6379

