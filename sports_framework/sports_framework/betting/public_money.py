"""
Public Betting Percentages
=========================
Track public betting data to identify contrarian opportunities.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
import time

from sports_framework.utils.persistence import execute_query, save_dataframe
from sports_framework.core.config import settings

logger = logging.getLogger(__name__)


class PublicMoneyTracker:
    """Track public betting percentages from various sources."""
    
    def __init__(self):
        # Note: Most public betting data requires paid subscriptions
        # Action Network, Sports Insights, etc.
        self.sources = ["action_network", "sports_insights", "draftkings"]
        self.public_bias_threshold = 70  # 70% on one side = heavy public bias
        
    def fetch_public_percentages(
        self, 
        game_id: str, 
        sport: str = "nfl"
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch public betting percentages for a game.
        
        Returns:
            {
                "game_id": str,
                "sport": str,
                "bets_on_home": float,  # % of bets on home
                "money_on_home": float,  # % of money on home
                "bets_on_away": float,  # % of bets on away
                "money_on_away": float,  # % of money on away
                "public_consensus": str,  # "HEAVY_HOME", "HEAVY_AWAY", "BALANCED"
                "sharp_contrarian": bool,  # True if money differs from bets
                "recommendation": str,  # "FADE_PUBLIC", "FOLLOW_PUBLIC", "NO_SIGNAL"
            }
        """
        
        # Try to fetch from database first (if we already have it)
        cached = execute_query("""
            SELECT * FROM public_betting_data
            WHERE game_id = :game_id
                AND fetched_at > NOW() - INTERVAL '2 hours'
            ORDER BY fetched_at DESC
            LIMIT 1
        """, {"game_id": game_id})
        
        if not cached.empty:
            return cached.iloc[0].to_dict()
        
        # If no cached data, fetch from API (requires subscription)
        # For now, return mock data structure
        # In production, replace with actual API calls
        
        if settings.enable_mock_data:
            return self._get_mock_public_data(game_id, sport)
        
        # Try to fetch from available sources
        for source in self.sources:
            data = self._fetch_from_source(source, game_id, sport)
            if data:
                return data
        
        return None
    
    def _fetch_from_source(
        self, 
        source: str, 
        game_id: str, 
        sport: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch public data from a specific source."""
        
        if source == "action_network":
            return self._fetch_action_network(game_id, sport)
        elif source == "sports_insights":
            return self._fetch_sports_insights(game_id, sport)
        elif source == "draftkings":
            return self._fetch_draftkings(game_id, sport)
        
        return None
    
    def _fetch_action_network(self, game_id: str, sport: str) -> Optional[Dict[str, Any]]:
        """Fetch from Action Network (requires API key)."""
        # Requires: Action Network API subscription ($50-100/month)
        # API: https://api.actionnetwork.com/
        
        api_key = settings.action_network_api_key
        if not api_key:
            return None
        
        try:
            # Implementation would go here
            # For now, return None
            return None
        except Exception as e:
            logger.error(f"Error fetching from Action Network: {e}")
            return None
    
    def _fetch_sports_insights(self, game_id: str, sport: str) -> Optional[Dict[str, Any]]:
        """Fetch from Sports Insights (requires subscription)."""
        # Requires: Sports Insights subscription
        # API: https://www.sportsinsights.com/
        
        try:
            # Implementation would go here
            return None
        except Exception as e:
            logger.error(f"Error fetching from Sports Insights: {e}")
            return None
    
    def _fetch_draftkings(self, game_id: str, sport: str) -> Optional[Dict[str, Any]]:
        """Scrape public percentages from DraftKings (check ToS)."""
        # Note: Web scraping may violate terms of service
        # Use at your own risk
        
        try:
            # Implementation would go here
            return None
        except Exception as e:
            logger.error(f"Error fetching from DraftKings: {e}")
            return None
    
    def _get_mock_public_data(self, game_id: str, sport: str) -> Dict[str, Any]:
        """Return mock public betting data for development."""
        if not settings.enable_mock_data:
            return None
        
        # Simulate heavy public bias on favorites
        import random
        
        # Randomly decide which side is favorite
        is_home_favorite = random.random() > 0.5
        
        if is_home_favorite:
            # Heavy public on home (favorite)
            bets_on_home = random.uniform(65, 85)
            money_on_home = random.uniform(55, 75)  # Less money than bets = sharp on away
            
            # Check for reverse line movement
            sharp_contrarian = money_on_home < bets_on_home
        else:
            # Heavy public on away (favorite)
            bets_on_home = random.uniform(15, 35)
            money_on_home = random.uniform(25, 45)  # More money than bets = sharp on home
            
            sharp_contrarian = money_on_home > bets_on_home
        
        # Determine consensus
        if bets_on_home > 70:
            consensus = "HEAVY_HOME"
        elif bets_on_home < 30:
            consensus = "HEAVY_AWAY"
        else:
            consensus = "BALANCED"
        
        # Generate recommendation
        if sharp_contrarian:
            # Money differs from bets = sharp contrarian play
            if money_on_home < bets_on_home:
                recommendation = "FADE_PUBLIC_HOME"  # Sharp on away
            else:
                recommendation = "FADE_PUBLIC_AWAY"  # Sharp on home
        elif bets_on_home > 70 or bets_on_home < 30:
            # Heavy public bias without sharp money = fade opportunity
            recommendation = "FADE_PUBLIC"
        else:
            recommendation = "NO_SIGNAL"
        
        return {
            "game_id": game_id,
            "sport": sport,
            "bets_on_home": round(bets_on_home, 1),
            "money_on_home": round(money_on_home, 1),
            "bets_on_away": round(100 - bets_on_home, 1),
            "money_on_away": round(100 - money_on_home, 1),
            "public_consensus": consensus,
            "sharp_contrarian": sharp_contrarian,
            "recommendation": recommendation,
            "fetched_at": datetime.now(),
        }
    
    def analyze_public_bias(self, game_id: str) -> Dict[str, Any]:
        """Analyze public betting bias for a game."""
        data = self.fetch_public_percentages(game_id)
        
        if not data:
            return {"game_id": game_id, "analysis": "no_data"}
        
        # Calculate bias metrics
        bet_bias = abs(data['bets_on_home'] - 50)
        money_bias = abs(data['money_on_home'] - 50)
        
        # Sharp money indicator
        # If money is more balanced than bets = sharp money on contrarian side
        sharp_money_indicator = money_bias < bet_bias
        
        # Fade opportunity
        fade_opportunity = bet_bias > 20 and not data['sharp_contrarian']
        
        return {
            "game_id": game_id,
            "bet_bias": bet_bias,
            "money_bias": money_bias,
            "sharp_money_indicator": sharp_money_indicator,
            "fade_opportunity": fade_opportunity,
            "recommendation": data['recommendation'],
        }
    
    def get_heavily_bet_games(self, sport: str = "nfl") -> pd.DataFrame:
        """Get games with heavy public betting."""
        # Fetch recent public data
        public_data = execute_query("""
            SELECT * FROM public_betting_data
            WHERE sport = :sport
                AND fetched_at > NOW() - INTERVAL '6 hours'
        """, {"sport": sport})
        
        if public_data.empty:
            return pd.DataFrame()
        
        # Filter for heavy bias
        heavy_bias = public_data[
            (public_data['bets_on_home'] > 70) |
            (public_data['bets_on_home'] < 30)
        ].copy()
        
        return heavy_bias


# Global tracker instance
_tracker: Optional[PublicMoneyTracker] = None


def get_public_money_tracker() -> PublicMoneyTracker:
    """Get or create public money tracker."""
    global _tracker
    if _tracker is None:
        _tracker = PublicMoneyTracker()
    return _tracker


# Example usage:
if __name__ == "__main__":
    tracker = get_public_money_tracker()
    
    # Analyze specific game
    data = tracker.fetch_public_percentages("2025_01_25_KC_LV")
    if data:
        print(f"Public consensus: {data['public_consensus']}")
        print(f"Sharp contrarian: {data['sharp_contrarian']}")
        print(f"Recommendation: {data['recommendation']}")
    
    # Get heavily bet games
    heavy_games = tracker.get_heavily_bet_games("nfl")
    print(f"Found {len(heavy_games)} heavily bet games")