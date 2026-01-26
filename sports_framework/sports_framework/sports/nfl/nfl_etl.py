"""
NFL ETL Implementation
=====================

Concrete implementation of SportETL for NFL.
"""

from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from sports_framework.core.base import SportETL
from sports_framework.core.models import (
    SportCode, Game, Team, Player, BettingLines, 
    InjuryReport, WeatherForecast, GameProjection
)
from sports_framework.core.api_clients import ESPNClient, APIClientFactory


class NFLETL(SportETL):
    """NFL ETL implementation."""
    
    def __init__(self):
        self.espn_client = APIClientFactory.get_client("espn")
    
    @property
    def sport_code(self) -> SportCode:
        return SportCode.NFL
    
    @property
    def display_name(self) -> str:
        return "NFL"
    
    def fetch_schedule(self, target_date: date) -> List[Game]:
        """Fetch NFL schedule for a date."""
        data = self.espn_client.fetch_scoreboard(self.sport_code, target_date)
        
        if not data or data.get("mock"):
            return []
        
        games = []
        for event in data.get("events", []):
            game = self._parse_game(event, target_date)
            if game:
                games.append(game)
        
        return games
    
    def fetch_schedule_range(self, start_date: date, end_date: date) -> List[Game]:
        """Fetch NFL schedule for a date range."""
        # ESPN doesn't support range queries well, so fetch day by day
        games = []
        current_date = start_date
        
        while current_date <= end_date:
            day_games = self.fetch_schedule(current_date)
            games.extend(day_games)
            current_date += pd.Timedelta(days=1)
        
        return games
    
    def fetch_teams(self) -> List[Team]:
        """Fetch all NFL teams."""
        # ESPN doesn't have a single teams endpoint, so extract from games
        # In a real implementation, you'd cache this or fetch from a dedicated source
        return []
    
    def fetch_team_stats(self, team_id: str, season: int) -> Dict[str, Any]:
        """Fetch NFL team statistics."""
        data = self.espn_client.fetch_team_stats(self.sport_code, team_id)
        return data
    
    def fetch_players(self, team_id: Optional[str] = None) -> List[Player]:
        """Fetch NFL players."""
        # Implementation would fetch from ESPN or other source
        return []
    
    def fetch_player_stats(self, player_id: str, season: int) -> Dict[str, Any]:
        """Fetch NFL player statistics."""
        # Implementation would fetch from ESPN or other source
        return {}
    
    def fetch_betting_lines(self, game_id: str) -> Optional[BettingLines]:
        """Fetch betting lines for an NFL game."""
        # This would use the Odds API client
        return None
    
    def fetch_all_betting_lines(self, target_date: date) -> List[BettingLines]:
        """Fetch all betting lines for NFL games on a date."""
        # This would use the Odds API client
        return []
    
    def fetch_injury_reports(self, team_id: Optional[str] = None) -> List[InjuryReport]:
        """Fetch NFL injury reports."""
        # Implementation would fetch from ESPN or other source
        return []
    
    def fetch_weather_forecast(self, game_id: str, venue_lat: float, venue_lon: float) -> Optional[WeatherForecast]:
        """Fetch weather forecast for an NFL game."""
        # This would use the OpenWeather client
        return None
    
    def generate_projection(self, game_id: str) -> Optional[GameProjection]:
        """Generate betting projection for an NFL game."""
        # Implementation would use multiple data sources
        return None
    
    def get_current_week(self, target_date: date) -> int:
        """Get current NFL week for a date."""
        # NFL season typically starts in early September
        season_start = date(target_date.year, 9, 1)
        
        # Find first Thursday (first week of season)
        while season_start.weekday() != 3:  # Thursday
            season_start += pd.Timedelta(days=1)
        
        # Calculate week number
        if target_date < season_start:
            return 0  # Preseason
        
        days_since_start = (target_date - season_start).days
        week = (days_since_start // 7) + 1
        
        return min(week, 18)  # Max 18 weeks (including playoffs)
    
    def is_game_day(self, target_date: date) -> bool:
        """Check if date is an NFL game day."""
        # NFL games: Thursday, Sunday, Monday
        return target_date.weekday() in [3, 6, 0]  # Thu, Sun, Mon
    
    def _parse_game(self, event: Dict[str, Any], game_date: date) -> Optional[Game]:
        """Parse ESPN event into Game object."""
        try:
            competition = event.get("competitions", [{}])[0]
            
            # Get teams
            competitors = competition.get("competitors", [])
            if len(competitors) < 2:
                return None
            
            home_team = None
            away_team = None
            
            for team in competitors:
                if team.get("homeAway") == "home":
                    home_team = team
                elif team.get("homeAway") == "away":
                    away_team = team
            
            if not home_team or not away_team:
                return None
            
            # Parse scores
            home_score = None
            away_score = None
            is_completed = False
            
            if home_team.get("score"):
                home_score = int(home_team["score"])
            if away_team.get("score"):
                away_score = int(away_team["score"])
            
            status = event.get("status", {}).get("type", {}).get("name", "")
            if status == "STATUS_FINAL":
                is_completed = True
            
            return Game(
                game_id=event.get("id"),
                game_date=game_date,
                game_time=self._parse_time(event.get("date")),
                home_team_id=home_team.get("team", {}).get("abbreviation"),
                away_team_id=away_team.get("team", {}).get("abbreviation"),
                season=game_date.year,
                week=self.get_current_week(game_date),
                venue_name=competition.get("venue", {}).get("fullName"),
                status=self._parse_status(status),
                is_completed=is_completed,
                home_score=home_score,
                away_score=away_score,
                sport=self.sport_code,
                source="espn",
            )
        
        except Exception as e:
            logger.error(f"Error parsing game: {e}")
            return None
    
    def _parse_time(self, date_str: str) -> Optional[datetime]:
        """Parse time from ESPN date string."""
        if not date_str:
            return None
        
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None
    
    def _parse_status(self, status: str) -> str:
        """Parse ESPN status to GameStatus."""
        status_map = {
            "STATUS_SCHEDULED": "scheduled",
            "STATUS_IN_PROGRESS": "live",
            "STATUS_HALFTIME": "live",
            "STATUS_FINAL": "final",
            "STATUS_POSTPONED": "postponed",
            "STATUS_CANCELED": "cancelled",
        }
        return status_map.get(status, "scheduled")


# Global NFL ETL instance
_nfl_etl: Optional[NFLETL] = None


def get_nfl_etl() -> NFLETL:
    """Get or create NFL ETL instance."""
    global _nfl_etl
    if _nfl_etl is None:
        _nfl_etl = NFLETL()
    return _nfl_etl