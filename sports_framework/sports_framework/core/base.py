"""
Base Classes
============

Abstract base classes for sports ETL implementations.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from sports_framework.core.models import (
    Game, Team, Player, BettingLines, InjuryReport, 
    WeatherForecast, GameProjection, SportCode
)


class SportETL(ABC):
    """
    Abstract base class for sport-specific ETL implementations.
    
    Each sport (NFL, NBA, NHL, MLB) must implement this interface.
    """
    
    @property
    @abstractmethod
    def sport_code(self) -> SportCode:
        """Sport code (e.g., SportCode.NFL)."""
        pass
    
    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable sport name (e.g., 'NFL')."""
        pass
    
    # =============================================================================
    # SCHEDULE METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_schedule(self, target_date: date) -> List[Game]:
        """
        Fetch games for a specific date.
        
        Args:
            target_date: Date to fetch games for
            
        Returns:
            List of Game objects
        """
        pass
    
    @abstractmethod
    def fetch_schedule_range(self, start_date: date, end_date: date) -> List[Game]:
        """
        Fetch games for a date range.
        
        Args:
            start_date: Start of range (inclusive)
            end_date: End of range (inclusive)
            
        Returns:
            List of Game objects
        """
        pass
    
    # =============================================================================
    # TEAM METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_teams(self) -> List[Team]:
        """
        Fetch all teams for the sport.
        
        Returns:
            List of Team objects
        """
        pass
    
    @abstractmethod
    def fetch_team_stats(self, team_id: str, season: int) -> Dict[str, Any]:
        """
        Fetch statistics for a specific team.
        
        Args:
            team_id: Team identifier
            season: Season year
            
        Returns:
            Dictionary of team statistics
        """
        pass
    
    # =============================================================================
    # PLAYER METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_players(self, team_id: Optional[str] = None) -> List[Player]:
        """
        Fetch players.
        
        Args:
            team_id: Optional team ID to filter by
            
        Returns:
            List of Player objects
        """
        pass
    
    @abstractmethod
    def fetch_player_stats(self, player_id: str, season: int) -> Dict[str, Any]:
        """
        Fetch statistics for a specific player.
        
        Args:
            player_id: Player identifier
            season: Season year
            
        Returns:
            Dictionary of player statistics
        """
        pass
    
    # =============================================================================
    # BETTING METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_betting_lines(self, game_id: str) -> Optional[BettingLines]:
        """
        Fetch betting lines for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            BettingLines object or None if not available
        """
        pass
    
    @abstractmethod
    def fetch_all_betting_lines(self, target_date: date) -> List[BettingLines]:
        """
        Fetch betting lines for all games on a date.
        
        Args:
            target_date: Date to fetch lines for
            
        Returns:
            List of BettingLines objects
        """
        pass
    
    # =============================================================================
    # INJURY METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_injury_reports(self, team_id: Optional[str] = None) -> List[InjuryReport]:
        """
        Fetch injury reports.
        
        Args:
            team_id: Optional team ID to filter by
            
        Returns:
            List of InjuryReport objects
        """
        pass
    
    # =============================================================================
    # WEATHER METHODS
    # =============================================================================
    
    @abstractmethod
    def fetch_weather_forecast(self, game_id: str, venue_lat: float, venue_lon: float) -> Optional[WeatherForecast]:
        """
        Fetch weather forecast for a game.
        
        Args:
            game_id: Game identifier
            venue_lat: Venue latitude
            venue_lon: Venue longitude
            
        Returns:
            WeatherForecast object or None if not available
        """
        pass
    
    # =============================================================================
    # PROJECTION METHODS
    # =============================================================================
    
    @abstractmethod
    def generate_projection(self, game_id: str) -> Optional[GameProjection]:
        """
        Generate a betting projection for a game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            GameProjection object or None if cannot generate
        """
        pass
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def get_season(self, target_date: date) -> int:
        """
        Get season year for a date.
        
        Args:
            target_date: Date to get season for
            
        Returns:
            Season year
        """
        # Default implementation: year of date
        # Override in sport-specific classes if needed
        return target_date.year
    
    def is_game_day(self, target_date: date) -> bool:
        """
        Check if date is a game day for this sport.
        
        Args:
            target_date: Date to check
            
        Returns:
            True if games are typically played on this day
        """
        # Default: check if it's a common game day
        # Override in sport-specific classes
        return target_date.weekday() in [0, 3, 6]  # Mon, Thu, Sun
    
    def get_current_week(self, target_date: date) -> int:
        """
        Get current week number for a date.
        
        Args:
            target_date: Date to get week for
            
        Returns:
            Week number (sport-specific)
        """
        # Override in sport-specific classes
        raise NotImplementedError(f"get_current_week not implemented for {self.sport_code}")


class DataSource(ABC):
    """
    Abstract base class for data sources (APIs, files, etc.).
    """
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the data source (e.g., 'espn', 'odds_api')."""
        pass
    
    @property
    @abstractmethod
    def requires_api_key(self) -> bool:
        """Whether this data source requires an API key."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if data source is available.
        
        Returns:
            True if source can be used
        """
        pass
    
    @abstractmethod
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch data from the source.
        
        Args:
            endpoint: API endpoint or resource
            params: Optional parameters
            
        Returns:
            Dictionary of response data
        """
        pass


class BettingDataSource(DataSource):
    """
    Base class for betting data sources.
    """
    
    @abstractmethod
    def fetch_lines(self, sport_code: SportCode, date: date) -> List[BettingLines]:
        """
        Fetch betting lines for a sport and date.
        
        Args:
            sport_code: Sport to fetch lines for
            date: Date to fetch lines for
            
        Returns:
            List of betting lines
        """
        pass
    
    @abstractmethod
    def fetch_line_movements(self, game_id: str) -> List[Dict[str, Any]]:
        """
        Fetch historical line movements for a game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of line movement events
        """
        pass