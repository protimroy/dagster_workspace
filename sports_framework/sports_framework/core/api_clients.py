"""
API Client Implementations
==========================

Concrete implementations of data sources.
"""

import time
from typing import Dict, Any, Optional, List
from datetime import date
import httpx
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from sports_framework.core.base import DataSource, BettingDataSource
from sports_framework.core.models import SportCode, BettingLines
from sports_framework.core.config import settings
from sports_framework.utils.rate_limiter import RateLimiter


# =============================================================================
# ESPN API CLIENT
# =============================================================================

class ESPNClient(DataSource):
    """
    ESPN API client for sports data.
    
    Free, no API key required. Rate limit: ~10 requests/second.
    Provides: schedules, scores, team stats, player info
    """
    
    BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"
    
    def __init__(self):
        self.rate_limiter = RateLimiter(calls_per_second=settings.espn_rate_limit)
        self.client = httpx.Client(timeout=30.0)
    
    @property
    def source_name(self) -> str:
        return "espn"
    
    @property
    def requires_api_key(self) -> bool:
        return False
    
    def is_available(self) -> bool:
        """Check if ESPN API is available."""
        try:
            response = self.client.get(f"{self.BASE_URL}/football/nfl/scoreboard", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch data from ESPN API.
        
        Args:
            endpoint: API endpoint (e.g., "football/nfl/scoreboard")
            params: Query parameters
            
        Returns:
            API response data
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
        except Exception as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
    
    def _get_mock_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return mock data for development."""
        if not settings.enable_mock_data:
            raise RuntimeError("Mock data not enabled. Set ENABLE_MOCK_DATA=true to enable.")
        
        # Return minimal valid response
        return {
            "mock": True,
            "endpoint": endpoint,
            "params": params,
            "timestamp": time.time()
        }
    
    def fetch_scoreboard(self, sport_code: SportCode, date: date) -> Dict[str, Any]:
        """Fetch scoreboard for a date."""
        sport_path = self._get_sport_path(sport_code)
        endpoint = f"{sport_path}/scoreboard"
        params = {"dates": date.strftime("%Y%m%d")}
        
        return self.fetch_data(endpoint, params)
    
    def fetch_schedule(self, sport_code: SportCode, start_date: date, end_date: date) -> Dict[str, Any]:
        """Fetch schedule for a date range."""
        sport_path = self._get_sport_path(sport_code)
        endpoint = f"{sport_path}/scoreboard"
        params = {
            "startDate": start_date.strftime("%Y%m%d"),
            "endDate": end_date.strftime("%Y%m%d")
        }
        
        return self.fetch_data(endpoint, params)
    
    def fetch_team_info(self, sport_code: SportCode, team_id: str) -> Dict[str, Any]:
        """Fetch team information."""
        sport_path = self._get_sport_path(sport_code)
        endpoint = f"{sport_path}/teams/{team_id}"
        
        return self.fetch_data(endpoint)
    
    def fetch_team_stats(self, sport_code: SportCode, team_id: str) -> Dict[str, Any]:
        """Fetch team statistics."""
        sport_path = self._get_sport_path(sport_code)
        endpoint = f"{sport_path}/teams/{team_id}/statistics"
        
        return self.fetch_data(endpoint)
    
    def _get_sport_path(self, sport_code: SportCode) -> str:
        """Get ESPN sport path from sport code."""
        sport_paths = {
            SportCode.NFL: "football/nfl",
            SportCode.NBA: "basketball/nba",
            SportCode.NHL: "hockey/nhl",
            SportCode.MLB: "baseball/mlb",
        }
        return sport_paths.get(sport_code, "football/nfl")


# =============================================================================
# THE ODDS API CLIENT
# =============================================================================

class OddsAPIClient(BettingDataSource):
    """
    The Odds API client for betting lines.
    
    Requires API key. Rate limit depends on tier.
    Provides: betting lines from multiple sportsbooks
    """
    
    BASE_URL = "https://api.the-odds-api.com/v4"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.odds_api_key
        self.rate_limiter = RateLimiter(calls_per_second=settings.odds_api_rate_limit)
        self.client = httpx.Client(timeout=30.0)
    
    @property
    def source_name(self) -> str:
        return "odds_api"
    
    @property
    def requires_api_key(self) -> bool:
        return True
    
    def is_available(self) -> bool:
        """Check if Odds API is available."""
        if not self.api_key:
            return False
        
        try:
            response = self.client.get(
                f"{self.BASE_URL}/sports",
                params={"apiKey": self.api_key},
                timeout=5.0
            )
            return response.status_code == 200
        except Exception:
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch data from Odds API."""
        self.rate_limiter.wait_if_needed()
        
        if not self.api_key:
            raise ValueError("Odds API key not configured")
        
        params = params or {}
        params["apiKey"] = self.api_key
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
        except Exception as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
    
    def _get_mock_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return mock betting data."""
        if not settings.enable_mock_data:
            raise RuntimeError("Mock data not enabled. Set ENABLE_MOCK_DATA=true to enable.")
        
        return {
            "mock": True,
            "endpoint": endpoint,
            "params": params,
            "timestamp": time.time(),
            "data": []
        }
    
    def fetch_lines(self, sport_code: SportCode, date: date) -> List[BettingLines]:
        """Fetch betting lines for a sport and date."""
        sport_key = self._get_sport_key(sport_code)
        
        endpoint = f"sports/{sport_key}/odds"
        params = {
            "regions": "us",
            "markets": "h2h,spreads,totals",
            "dateFormat": "iso",
            "date": date.isoformat()
        }
        
        data = self.fetch_data(endpoint, params)
        
        if not data or isinstance(data, dict) and data.get("mock"):
            return []
        
        return self._parse_betting_lines(data)
    
    def fetch_line_movements(self, game_id: str) -> List[Dict[str, Any]]:
        """Fetch line movements for a game."""
        # The Odds API doesn't provide historical movements in the free tier
        # This would require a premium subscription
        
        if settings.enable_mock_data:
            return self._get_mock_line_movements(game_id)
        
        return []
    
    def _parse_betting_lines(self, data: List[Dict[str, Any]]) -> List[BettingLines]:
        """Parse Odds API response into BettingLines objects."""
        lines_list = []
        
        for event in data:
            game_id = event.get("id")
            
            for bookmaker in event.get("bookmakers", []):
                sportsbook = bookmaker.get("title")
                
                lines = BettingLines(
                    game_id=game_id,
                    sportsbook=sportsbook,
                    sport=self.sport_code,
                    source=self.source_name,
                    last_updated=datetime.now(),
                )
                
                for market in bookmaker.get("markets", []):
                    market_key = market.get("key")
                    
                    if market_key == "spreads":
                        # Parse spread
                        outcomes = market.get("outcomes", [])
                        for outcome in outcomes:
                            if outcome.get("name") == event.get("home_team"):
                                lines.home_spread = outcome.get("point")
                                lines.home_spread_odds = outcome.get("price")
                            else:
                                lines.away_spread = outcome.get("point")
                                lines.away_spread_odds = outcome.get("price")
                    
                    elif market_key == "totals":
                        # Parse total
                        outcomes = market.get("outcomes", [])
                        for outcome in outcomes:
                            if outcome.get("name") == "Over":
                                lines.total = outcome.get("point")
                                lines.over_odds = outcome.get("price")
                            else:
                                lines.under_odds = outcome.get("price")
                    
                    elif market_key == "h2h":
                        # Parse moneyline
                        outcomes = market.get("outcomes", [])
                        for outcome in outcomes:
                            if outcome.get("name") == event.get("home_team"):
                                lines.home_moneyline = outcome.get("price")
                            else:
                                lines.away_moneyline = outcome.get("price")
                
                lines_list.append(lines)
        
        return lines_list
    
    def _get_sport_key(self, sport_code: SportCode) -> str:
        """Get Odds API sport key from sport code."""
        sport_keys = {
            SportCode.NFL: "americanfootball_nfl",
            SportCode.NBA: "basketball_nba",
            SportCode.NHL: "icehockey_nhl",
            SportCode.MLB: "baseball_mlb",
        }
        return sport_keys.get(sport_code, "americanfootball_nfl")
    
    def _get_mock_line_movements(self, game_id: str) -> List[Dict[str, Any]]:
        """Return mock line movements for development."""
        return [
            {
                "game_id": game_id,
                "sportsbook": "Mock Book",
                "bet_type": "spread",
                "old_line": -3.0,
                "new_line": -3.5,
                "old_odds": -110,
                "new_odds": -110,
                "movement_time": datetime.now(),
                "reason": "injury"
            }
        ]


# =============================================================================
# OPENWEATHER API CLIENT
# =============================================================================

class OpenWeatherClient(DataSource):
    """
    OpenWeather API client for weather data.
    
    Requires API key for production. Free tier: 60 calls/minute.
    Provides: weather forecasts, current conditions
    """
    
    BASE_URL = "https://api.openweathermap.org/data/2.5"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.openweather_api_key
        self.rate_limiter = RateLimiter(calls_per_second=settings.openweather_rate_limit / 60)
        self.client = httpx.Client(timeout=30.0)
    
    @property
    def source_name(self) -> str:
        return "openweather"
    
    @property
    def requires_api_key(self) -> bool:
        return True
    
    def is_available(self) -> bool:
        """Check if OpenWeather API is available."""
        if not self.api_key:
            return False
        
        try:
            response = self.client.get(
                f"{self.BASE_URL}/weather",
                params={"lat": 40.7128, "lon": -74.0060, "appid": self.api_key},
                timeout=5.0
            )
            return response.status_code == 200
        except Exception:
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch data from OpenWeather API."""
        self.rate_limiter.wait_if_needed()
        
        if not self.api_key:
            raise ValueError("OpenWeather API key not configured")
        
        params = params or {}
        params["appid"] = self.api_key
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
        except Exception as e:
            if settings.enable_mock_data:
                return self._get_mock_data(endpoint, params)
            raise
    
    def _get_mock_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return mock weather data."""
        if not settings.enable_mock_data:
            raise RuntimeError("Mock data not enabled. Set ENABLE_MOCK_DATA=true to enable.")
        
        return {
            "mock": True,
            "endpoint": endpoint,
            "params": params,
            "timestamp": time.time(),
            "weather": {
                "temp": 72,
                "humidity": 50,
                "wind_speed": 5,
                "conditions": "clear"
            }
        }
    
    def fetch_forecast(self, lat: float, lon: float, hours_ahead: int = 24) -> Dict[str, Any]:
        """Fetch weather forecast for a location."""
        endpoint = "forecast"
        params = {
            "lat": lat,
            "lon": lon,
            "units": "imperial"  # Fahrenheit, mph
        }
        
        return self.fetch_data(endpoint, params)
    
    def fetch_current_weather(self, lat: float, lon: float) -> Dict[str, Any]:
        """Fetch current weather for a location."""
        endpoint = "weather"
        params = {
            "lat": lat,
            "lon": lon,
            "units": "imperial"
        }
        
        return self.fetch_data(endpoint, params)


# =============================================================================
# FACTORY FOR CREATING CLIENTS
# =============================================================================

class APIClientFactory:
    """Factory for creating API clients."""
    
    _clients = {}
    
    @classmethod
    def get_client(cls, source_name: str, **kwargs) -> DataSource:
        """Get or create an API client."""
        if source_name not in cls._clients:
            if source_name == "espn":
                cls._clients[source_name] = ESPNClient()
            elif source_name == "odds_api":
                cls._clients[source_name] = OddsAPIClient(**kwargs)
            elif source_name == "openweather":
                cls._clients[source_name] = OpenWeatherClient(**kwargs)
            else:
                raise ValueError(f"Unknown data source: {source_name}")
        
        return cls._clients[source_name]
    
    @classmethod
    def reset_client(cls, source_name: str):
        """Reset a client (useful for testing)."""
        if source_name in cls._clients:
            del cls._clients[source_name]