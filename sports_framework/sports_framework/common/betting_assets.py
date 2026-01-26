"""
Common Betting Assets
====================

Shared betting-related assets used across all sports.
"""

from datetime import date, timedelta
from typing import List
import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import logging

from sports_framework.core.api_clients import OddsAPIClient, APIClientFactory
from sports_framework.core.models import SportCode, BettingLines
from sports_framework.utils.persistence import save_dataframe
from sports_framework.utils.data_quality import SportsDataQualitySuites

logger = logging.getLogger(__name__)


@asset(
    description="Betting lines from multiple sportsbooks for all sports",
    group_name="betting",
    compute_kind="api",
    freshness_policy=None,
)
def all_sports_betting_lines(
    context: AssetExecutionContext,
    nfl_schedule: pd.DataFrame,
    # nba_schedule: pd.DataFrame,  # Add when NBA is implemented
    # nhl_schedule: pd.DataFrame,  # Add when NHL is implemented
    # mlb_schedule: pd.DataFrame,  # Add when MLB is implemented
) -> Output[pd.DataFrame]:
    """Fetch betting lines for all upcoming games across all sports."""
    context.log.info("Fetching betting lines for all sports...")
    
    client = OddsAPIClient()
    
    if not client.is_available():
        context.log.error("Odds API not available - check API key")
        return Output(pd.DataFrame(), metadata={"lines_found": 0, "error": "API not available"})
    
    all_lines = []
    
    # Get unique game dates from schedules
    schedules = [
        ("nfl", nfl_schedule),
        # ("nba", nba_schedule),
        # ("nhl", nhl_schedule),
        # ("mlb", mlb_schedule),
    ]
    
    for sport_name, schedule_df in schedules:
        if schedule_df.empty:
            continue
        
        # Get unique dates from schedule
        schedule_df['game_date'] = pd.to_datetime(schedule_df['game_date'])
        unique_dates = schedule_df['game_date'].dt.date.unique()
        
        # Fetch lines for each date
        for game_date in unique_dates:
            context.log.info(f"Fetching lines for {sport_name} on {game_date}")
            
            sport_code = getattr(SportCode, sport_name.upper())
            lines = client.fetch_lines(sport_code, game_date)
            
            all_lines.extend(lines)
    
    context.log.info(f"Fetched {len(all_lines)} betting lines")
    
    if not all_lines:
        return Output(pd.DataFrame(), metadata={"lines_found": 0})
    
    # Convert to DataFrame
    df = pd.DataFrame([line.model_dump() for line in all_lines])
    
    # Data quality checks
    if context.resources.data_quality_enabled and not df.empty:
        checker = SportsDataQualitySuites.check_betting_lines(df, "all_sports_betting_lines")
        checker.raise_on_errors()
    
    # Save to database
    save_dataframe(
        df,
        table_name="betting_lines",
        unique_keys=["game_id", "sportsbook"],
        if_exists="append",
        source="odds_api"
    )
    
    return Output(
        df,
        metadata={
            "lines_found": len(df),
            "sportsbooks": df['sportsbook'].nunique(),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(
    description="Best available betting lines across all sportsbooks",
    group_name="betting",
    compute_kind="python",
)
def best_betting_lines(
    context: AssetExecutionContext,
    all_sports_betting_lines: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """Find the best available lines across all sportsbooks."""
    context.log.info("Finding best betting lines...")
    
    if all_sports_betting_lines.empty:
        return Output(pd.DataFrame(), metadata={"lines_found": 0})
    
    # Find best line for each game and bet type
    best_lines = []
    
    for game_id in all_sports_betting_lines['game_id'].unique():
        game_lines = all_sports_betting_lines[all_sports_betting_lines['game_id'] == game_id]
        
        # Best spread (most favorable to underdog)
        if 'home_spread' in game_lines.columns:
            best_spread_idx = game_lines['home_spread'].abs().idxmin()
            best_lines.append(game_lines.loc[best_spread_idx])
        
        # Best total (highest total for overs)
        if 'total' in game_lines.columns:
            best_total_idx = game_lines['total'].idxmax()
            best_lines.append(game_lines.loc[best_total_idx])
    
    df = pd.DataFrame(best_lines)
    
    # Remove duplicates (if both spread and total selected same line)
    df = df.drop_duplicates(subset=['game_id', 'sportsbook'])
    
    # Save to database
    save_dataframe(
        df,
        table_name="best_betting_lines",
        unique_keys=["game_id", "bet_type"],
        if_exists="append",
        source="calculated"
    )
    
    return Output(
        df,
        metadata={
            "best_lines_found": len(df),
            "games_covered": df['game_id'].nunique(),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )