"""
Dagster Definitions
==================

Main entry point for Dagster. Exports all assets, jobs, schedules, and resources.
"""

from dagster import Definitions, load_assets_from_modules, ResourceDefinition
import os

from sports_framework.core.config import settings
from sports_framework.sports.nfl import assets as nfl_assets, jobs as nfl_jobs
from sports_framework.common import betting_assets, weather_assets
from sports_framework.betting import betting_intelligence_assets
from sports_framework.utils.persistence import get_engine


# =============================================================================
# RESOURCES
# =============================================================================

def get_data_quality_enabled():
    """Check if data quality checks are enabled."""
    return settings.enable_data_quality_checks


# =============================================================================
# LOAD ASSETS
# =============================================================================

# Load all asset modules
all_asset_modules = [
    nfl_assets,
    betting_assets,
    weather_assets,
    betting_intelligence_assets,
]

all_assets = []
for module in all_asset_modules:
    try:
        assets = load_assets_from_modules([module])
        all_assets.extend(assets)
    except Exception as e:
        print(f"Warning: Could not load assets from {module.__name__}: {e}")


# =============================================================================
# DEFINE JOBS AND SCHEDULES
# =============================================================================

all_jobs = [
    # NFL jobs
    nfl_jobs.nfl_full_pipeline,
    nfl_jobs.nfl_schedule_job,
    nfl_jobs.nfl_teams_job,
    nfl_jobs.nfl_injuries_job,
    nfl_jobs.nfl_stats_job,
]

all_schedules = [
    # NFL schedules
    nfl_jobs.daily_nfl_schedule,
    nfl_jobs.game_day_nfl_updates,
    nfl_jobs.weekly_nfl_stats,
    nfl_jobs.nfl_injury_updates,
]


# =============================================================================
# CREATE DEFINITIONS
# =============================================================================

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "data_quality_enabled": ResourceDefinition.hardcoded_resource(
            get_data_quality_enabled()
        ),
        "database_engine": ResourceDefinition.hardcoded_resource(
            get_engine()
        ),
    },
)