"""
NFL Dagster Jobs
===============

Job definitions for NFL ETL pipeline.
"""

from dagster import define_asset_job, AssetSelection, ScheduleDefinition, DefaultScheduleStatus

# =============================================================================
# JOBS
# =============================================================================

# Full NFL pipeline - runs all assets
nfl_full_pipeline = define_asset_job(
    name="nfl_full_pipeline",
    description="Complete NFL ETL: schedule, teams, players, injuries, stats",
    selection=AssetSelection.groups(
        "nfl_schedule",
        "nfl_teams",
        "nfl_players",
        "nfl_injuries",
        "nfl_team_stats",
        "nfl_player_stats",
    ),
    tags={
        "sport": "nfl",
        "frequency": "daily",
        "priority": "high",
    }
)

# Individual component jobs
nfl_schedule_job = define_asset_job(
    name="nfl_schedule_job",
    description="Fetch NFL schedule only",
    selection=AssetSelection.groups("nfl_schedule"),
    tags={"sport": "nfl", "type": "schedule"}
)

nfl_teams_job = define_asset_job(
    name="nfl_teams_job",
    description="Fetch NFL teams only",
    selection=AssetSelection.groups("nfl_teams"),
    tags={"sport": "nfl", "type": "teams"}
)

nfl_injuries_job = define_asset_job(
    name="nfl_injuries_job",
    description="Fetch NFL injury reports only",
    selection=AssetSelection.groups("nfl_injuries"),
    tags={"sport": "nfl", "type": "injuries"}
)

nfl_stats_job = define_asset_job(
    name="nfl_stats_job",
    description="Fetch NFL team and player statistics",
    selection=AssetSelection.groups("nfl_team_stats", "nfl_player_stats"),
    tags={"sport": "nfl", "type": "stats"}
)

# =============================================================================
# SCHEDULES
# =============================================================================

# Daily schedule update - 6 AM ET
daily_nfl_schedule = ScheduleDefinition(
    name="daily_nfl_schedule",
    job=nfl_schedule_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    description="Update NFL schedule daily at 6 AM ET",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Game day updates - every hour during game days (Thu, Sun, Mon)
game_day_nfl_updates = ScheduleDefinition(
    name="game_day_nfl_updates",
    job=nfl_full_pipeline,
    cron_schedule="0 9-23 * * 0,3,4",  # 9 AM to 11 PM on Sun(0), Wed(3), Thu(4)
    execution_timezone="America/New_York",
    description="Frequent updates on NFL game days",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Weekly stats refresh - Tuesday mornings (after Monday night games)
weekly_nfl_stats = ScheduleDefinition(
    name="weekly_nfl_stats",
    job=nfl_stats_job,
    cron_schedule="0 8 * * 2",  # Tuesday 8 AM
    execution_timezone="America/New_York",
    description="Weekly stats refresh on Tuesday mornings",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Injury reports - 3x daily on game days
nfl_injury_updates = ScheduleDefinition(
    name="nfl_injury_updates",
    job=nfl_injuries_job,
    cron_schedule="0 8,14,18 * * 0,3,4",  # 8 AM, 2 PM, 6 PM on game days
    execution_timezone="America/New_York",
    description="Injury report updates on game days",
    default_status=DefaultScheduleStatus.STOPPED,
)