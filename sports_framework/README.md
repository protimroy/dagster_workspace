# Sports Betting ETL Framework

A modern, extensible Dagster-based ETL framework for sports data with a focus on sports betting analytics.

## Features

- **Multi-sport support**: NFL, NBA, NHL, MLB (easy to add more)
- **Betting-focused**: Lines, odds, injuries, weather, projections
- **Plugin architecture**: Easy to add new data sources
- **Production-ready**: Rate limiting, data quality, error handling
- **Efficient**: Incremental loading, connection pooling, caching
- **Type-safe**: Pydantic models, type hints throughout

## Quick Start

```bash
# Install dependencies
pip install -e .

# Set environment variables
cp .env.example .env
# Edit .env with your API keys

# Run a specific sport
export SPORT=nfl
dagster dev -m sports_framework

# Run all sports
dagster dev -m sports_framework.all_sports
```

## Architecture

```
sports_framework/
├── core/                    # Shared framework code
│   ├── config.py           # Configuration management
│   ├── models/             # Pydantic data models
│   ├── api_clients/        # API client base classes
│   ├── persistence/        # Database utilities
│   └── quality/            # Data quality checks
├── sports/                  # Sport-specific implementations
│   ├── nfl/                # NFL ETL
│   ├── nba/                # NBA ETL
│   ├── nhl/                # NHL ETL
│   └── mlb/                # MLB ETL
├── common/                  # Shared assets
│   ├── betting/            # Betting lines, odds
│   ├── injuries/           # Injury reports
│   ├── weather/            # Weather data
│   └── projections/        # Game projections
└── utils/                   # Utilities
    ├── rate_limiter.py     # API rate limiting
    ├── cache.py            # Caching layer
    └── logger.py           # Structured logging
```

## Adding a New Sport

1. Create sport directory: `sports/{sport_name}/`
2. Implement the sport interface:

```python
from sports_framework.core.base import SportETL

class NBAETL(SportETL):
    sport_code = "nba"
    
    def get_schedule(self, date: date) -> List[Game]:
        # Implementation
        pass
    
    def get_betting_lines(self, game_id: str) -> BettingLines:
        # Implementation
        pass
```

3. Register in `sports/__init__.py`

## Adding a New Data Source

1. Create API client in `core/api_clients/`
2. Implement the `DataSource` interface
3. Use in sport-specific ETLs

```python
from sports_framework.core.api_clients import DataSource

class NewDataSource(DataSource):
    def fetch_games(self, **kwargs) -> List[Dict]:
        # Implementation
        pass
```

## Configuration

All configuration via environment variables:

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/sports_betting

# API Keys
ODDS_API_KEY=your_key_here
OPENWEATHER_API_KEY=your_key_here

# Feature flags
ENABLE_MOCK_DATA=false
ENABLE_RATE_LIMITING=true
ENABLE_DATA_QUALITY_CHECKS=true

# Sports to enable
ENABLED_SPORTS=nfl,nba,nhl,mlb
```

## Data Models

### Core Models
- `Game`: Schedule, teams, venue, status
- `Team`: Team information, metadata
- `Player`: Player profiles, positions
- `BettingLines`: Spreads, totals, moneylines
- `InjuryReport`: Injury status, impact
- `WeatherForecast`: Conditions, impact scores
- `GameProjection`: Predicted scores, edges

### Betting-Specific
- `LineMovement`: Historical line changes
- `BettingTrend`: Public betting percentages
- `SharpMoney`: Professional bettor indicators
- `ValueBet`: Recommended bets with edge

## Scheduling

- **Game days**: Hourly updates for lines, injuries, weather
- **Off days**: Daily schedule refresh
- **Historical**: Weekly full refresh (Tuesdays)
- **Projections**: Every 6 hours

## Data Quality

Every asset runs quality checks:
- Schema validation
- Null value detection
- Range validation
- Freshness checks
- Uniqueness constraints

## Rate Limiting

Built-in rate limiting per API:
- ESPN: 8 requests/second
- Odds API: 2 requests/second (adjust for your tier)
- OpenWeather: 60 requests/minute
- SportsRadar: 1 request/second

## Monitoring

- Dagster asset materialization tracking
- Data quality metrics
- API call success rates
- Pipeline execution times
- Data freshness monitoring

## Development

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest tests/

# Type checking
mypy sports_framework/

# Linting
ruff check sports_framework/
```

## Production Deployment

```bash
# Build Docker image
docker build -t sports-etl .

# Run with Docker Compose
docker-compose up -d

# Or deploy to Dagster Cloud
dagster-cloud deploy
```