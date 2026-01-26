"""
NFL Dagster Assets
=================

Dagster assets for NFL ETL pipeline.
"""

from datetime import date, datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import logging

from sports_framework.sports.nfl.nfl_etl import get_nfl_etl
from sports_framework.core.models import Game, Team, Player, BettingLines, InjuryReport, WeatherForecast
from sports_framework.utils.persistence import save_dataframe
from sports_framework.utils.data_quality import SportsDataQualitySuites

logger = logging.getLogger(__name__)


# =============================================================================
# SCHEDULE ASSETS
# =============================================================================

@asset(
    description="NFL games schedule with teams, dates, and venues",
    group_name="nfl_schedule",
    compute_kind="api",
    freshness_policy=None,  # Update frequently during season
)
def nfl_schedule(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch NFL schedule for today and next 7 days."""
    context.log.info("Fetching NFL schedule...")
    
    etl = get_nfl_etl()
    today = date.today()
    
    # Fetch today + next 7 days
    games = []
    for i in range(8):
        target_date = today + timedelta(days=i)
        day_games = etl.fetch_schedule(target_date)
        games.extend(day_games)
    
    context.log.info(f"Fetched {len(games)} games")
    
    # Convert to DataFrame
    if not games:
        context.log.warning("No games found")
        return Output(pd.DataFrame(), metadata={"games_found": 0})
    
    df = pd.DataFrame([game.model_dump() for game in games])
    
    # Data quality checks
    if context.resources.data_quality_enabled:
        checker = SportsDataQualitySuites.check_schedule(df, "nfl_schedule")
        checker.raise_on_errors()
        
        context.add_output_metadata({
            "quality_report": MetadataValue.json(checker.get_summary())
        })
    
    # Save to database
    save_dataframe(
        df,
        table_name="nfl_games",
        unique_keys=["game_id"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "games_found": len(df),
            "date_range": f"{today} to {today + timedelta(days=7)}",
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(
    description="NFL teams with venue and metadata information",
    group_name="nfl_teams",
    compute_kind="api",
)
def nfl_teams(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch all NFL teams."""
    context.log.info("Fetching NFL teams...")
    
    etl = get_nfl_etl()
    teams = etl.fetch_teams()
    
    if not teams:
        # Extract teams from schedule if available
        context.log.warning("No teams fetched, extracting from schedule...")
        # This would fetch from schedule asset
        teams = []
    
    df = pd.DataFrame([team.model_dump() for team in teams])
    
    save_dataframe(
        df,
        table_name="nfl_teams",
        unique_keys=["team_id"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "teams_found": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )


# =============================================================================
# PLAYER ASSETS
# =============================================================================

@asset(
    description="NFL players with current team and status",
    group_name="nfl_players",
    compute_kind="api",
)
def nfl_players(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch NFL players."""
    context.log.info("Fetching NFL players...")
    
    etl = get_nfl_etl()
    players = etl.fetch_players()
    
    df = pd.DataFrame([player.model_dump() for player in players])
    
    save_dataframe(
        df,
        table_name="nfl_players",
        unique_keys=["player_id"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "players_found": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )


# =============================================================================
# INJURY ASSETS
# =============================================================================

@asset(
    description="NFL injury reports with player status and impact",
    group_name="nfl_injuries",
    compute_kind="api",
)
def nfl_injury_reports(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch NFL injury reports."""
    context.log.info("Fetching NFL injury reports...")
    
    etl = get_nfl_etl()
    injuries = etl.fetch_injury_reports()
    
    df = pd.DataFrame([injury.model_dump() for injury in injuries])
    
    # Data quality checks
    if context.resources.data_quality_enabled and not df.empty:
        checker = SportsDataQualitySuites.check_injury_reports(df, "nfl_injury_reports")
        checker.raise_on_errors()
    
    save_dataframe(
        df,
        table_name="nfl_injury_reports",
        unique_keys=["player_id", "report_date"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "injuries_found": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )


# =============================================================================
# TEAM STATS ASSETS
# =============================================================================

@asset(
    description="NFL team statistics for offense, defense, and special teams",
    group_name="nfl_team_stats",
    compute_kind="api",
)
def nfl_team_stats(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch NFL team statistics."""
    context.log.info("Fetching NFL team statistics...")
    
    etl = get_nfl_etl()
    
    # Get teams from schedule or teams table
    # For now, fetch from schedule
    today = date.today()
    games = etl.fetch_schedule(today)
    
    team_stats = []
    team_ids = set()
    
    for game in games:
        team_ids.add(game.home_team_id)
        team_ids.add(game.away_team_id)
    
    for team_id in team_ids:
        stats = etl.fetch_team_stats(team_id, today.year)
        if stats:
            stats["team_id"] = team_id
            stats["season"] = today.year
            team_stats.append(stats)
    
    df = pd.DataFrame(team_stats)
    
    # Data quality checks
    if context.resources.data_quality_enabled and not df.empty:
        checker = SportsDataQualitySuites.check_team_stats(df, "nfl_team_stats")
        checker.raise_on_errors()
    
    save_dataframe(
        df,
        table_name="nfl_team_stats",
        unique_keys=["team_id", "season"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "teams_found": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )


# =============================================================================
# PLAYER STATS ASSETS
# =============================================================================

@asset(
    description="NFL player statistics for passing, rushing, receiving",
    group_name="nfl_player_stats",
    compute_kind="api",
)
def nfl_player_stats(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch NFL player statistics."""
    context.log.info("Fetching NFL player statistics...")
    
    etl = get_nfl_etl()
    
    # Get players
    players = etl.fetch_players()
    
    player_stats = []
    for player in players[:100]:  # Limit for performance
        stats = etl.fetch_player_stats(player.player_id, date.today().year)
        if stats:
            stats["player_id"] = player.player_id
            stats["season"] = date.today().year
            player_stats.append(stats)
    
    df = pd.DataFrame(player_stats)
    
    save_dataframe(
        df,
        table_name="nfl_player_stats",
        unique_keys=["player_id", "season"],
        if_exists="append",
        sport="nfl",
        source="espn"
    )
    
    return Output(
        df,
        metadata={
            "players_found": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )