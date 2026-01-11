# Dagster Workspace 🏈⚙️

Central workspace for Dagster-powered data pipelines.

## Projects

### [NFL ETL](./nfl_etl/)

A production-ready NFL analytics platform that fetches real data from public APIs, stores it in PostgreSQL, and generates game projections with betting recommendations.

**Goal**: Build a data-driven edge for NFL betting by:
1. Fetching real-time schedule, weather, injury, and betting data
2. Analyzing the impact of each factor on game outcomes
3. Generating actionable picks with confidence ratings

**Current Focus**: Wild Card Weekend 2026 (January 11, 2026)

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
┌─────────────────────────────────────────────────────────────────────┐
│                       Data Pipeline Flow                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ESPN API ──┐                                                       │
│              │                                                       │
│   Open-Meteo ├──▶ Dagster ETL ──▶ PostgreSQL ──▶ Projections        │
│   (Weather)  │    Assets                        & Betting Card       │
│              │                                                       │
│   The Odds  ─┘                                                       │
│   API                                                                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Sources (All Free/Public)

| Source | Data | Cost |
|--------|------|------|
| ESPN API | Schedule, teams, injuries, game data | Free (public) |
| Open-Meteo | Weather forecasts by lat/lon | Free |
| The Odds API | Multi-book betting lines | 500 req/month free |

## Workspace Structure

```
dagster_workspace/
├── workspace.yaml          # Dagster workspace config
├── nfl_etl/               # NFL Analytics ETL package
│   ├── assets/            # Dagster assets (ETL pipelines)
│   ├── backend/           # Database models & connections
│   ├── data_sources.py    # API clients (ESPN, Weather, Odds)
│   ├── jobs.py           # Dagster jobs
│   └── schedules.py      # Automated schedules
├── history/               # Dagster run history
├── logs/                  # Application logs
└── storage/              # Dagster storage
```

