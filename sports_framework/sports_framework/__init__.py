"""
Sports Betting ETL Framework
=============================

A modern, extensible framework for sports data ETL with a focus on sports betting.
"""

__version__ = "0.1.0"
__author__ = "Sports Analytics Team"

from sports_framework.core.config import settings
from sports_framework.core.models import (
    Game,
    Team,
    Player,
    BettingLines,
    InjuryReport,
    WeatherForecast,
    GameProjection,
)

__all__ = [
    "settings",
    "Game",
    "Team", 
    "Player",
    "BettingLines",
    "InjuryReport",
    "WeatherForecast",
    "GameProjection",
]