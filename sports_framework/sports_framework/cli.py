"""
CLI Interface
============

Command-line interface for running the sports framework.
"""

import click
import os
from datetime import date
from dotenv import load_dotenv

from sports_framework.core.config import settings
from sports_framework.sports.nfl.nfl_etl import get_nfl_etl

# Load environment variables
load_dotenv()


@click.group()
def cli():
    """Sports Betting ETL Framework CLI."""
    pass


@cli.command()
@click.option('--sport', type=click.Choice(['nfl', 'nba', 'nhl', 'mlb']), default='nfl',
              help='Sport to run ETL for')
@click.option('--date', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(date.today()), help='Date to fetch data for')
@click.option('--asset', type=click.Choice(['schedule', 'teams', 'players', 'injuries', 'stats', 'all']),
              default='all', help='Specific asset to run')
def run(sport: str, date, asset: str):
    """Run ETL for a specific sport and date."""
    click.echo(f"Running {sport.upper()} ETL for {date.date()}...")
    
    if sport == 'nfl':
        _run_nfl(date.date(), asset)
    else:
        click.echo(f"{sport.upper()} not implemented yet")
        return
    
    click.echo("✓ ETL completed successfully")


def _run_nfl(target_date: date, asset: str):
    """Run NFL ETL."""
    etl = get_nfl_etl()
    
    if asset in ['schedule', 'all']:
        click.echo("Fetching schedule...")
        games = etl.fetch_schedule(target_date)
        click.echo(f"  Found {len(games)} games")
    
    if asset in ['teams', 'all']:
        click.echo("Fetching teams...")
        teams = etl.fetch_teams()
        click.echo(f"  Found {len(teams)} teams")
    
    if asset in ['players', 'all']:
        click.echo("Fetching players...")
        players = etl.fetch_players()
        click.echo(f"  Found {len(players)} players")
    
    if asset in ['injuries', 'all']:
        click.echo("Fetching injury reports...")
        injuries = etl.fetch_injury_reports()
        click.echo(f"  Found {len(injuries)} injuries")
    
    if asset in ['stats', 'all']:
        click.echo("Fetching team stats...")
        # This would need team IDs
        click.echo("  (Team stats require team IDs from schedule)")


@cli.command()
def validate():
    """Validate configuration and API connections."""
    click.echo("Validating configuration...")
    
    # Check configuration
    is_valid = settings.validate()
    
    if not is_valid:
        click.echo("⚠️  Configuration warnings found (see above)")
    else:
        click.echo("✓ Configuration valid")
    
    # Check API connections
    click.echo("\nChecking API connections...")
    
    from sports_framework.core.api_clients import APIClientFactory
    
    # Check ESPN
    try:
        espn = APIClientFactory.get_client("espn")
        if espn.is_available():
            click.echo("✓ ESPN API: Available")
        else:
            click.echo("✗ ESPN API: Not available")
    except Exception as e:
        click.echo(f"✗ ESPN API: Error - {e}")
    
    # Check Odds API
    try:
        odds = APIClientFactory.get_client("odds_api")
        if odds.is_available():
            click.echo("✓ Odds API: Available")
        else:
            click.echo("✗ Odds API: Not available (check API key)")
    except Exception as e:
        click.echo(f"✗ Odds API: Error - {e}")
    
    # Check OpenWeather
    try:
        weather = APIClientFactory.get_client("openweather")
        if weather.is_available():
            click.echo("✓ OpenWeather API: Available")
        else:
            click.echo("✗ OpenWeather API: Not available (check API key)")
    except Exception as e:
        click.echo(f"✗ OpenWeather API: Error - {e}")


@cli.command()
@click.option('--sport', type=click.Choice(['nfl', 'nba', 'nhl', 'mlb']), default='nfl',
              help='Sport to setup database for')
def init_db(sport: str):
    """Initialize database schema."""
    click.echo(f"Initializing database for {sport.upper()}...")
    
    # This would create the necessary tables
    # For now, just show what would be created
    tables = [
        f"{sport}_games",
        f"{sport}_teams",
        f"{sport}_players",
        f"{sport}_injury_reports",
        f"{sport}_team_stats",
        f"{sport}_player_stats",
        "betting_lines",
        "weather_forecasts",
    ]
    
    click.echo("Would create tables:")
    for table in tables:
        click.echo(f"  - {table}")
    
    click.echo("\nRun these SQL commands to create tables:")
    click.echo("See: sports_framework/scripts/create_schema.sql")


@cli.command()
def dagster_ui():
    """Launch Dagster UI."""
    click.echo("Launching Dagster UI...")
    os.system("dagster dev -m sports_framework.definitions")


if __name__ == "__main__":
    cli()