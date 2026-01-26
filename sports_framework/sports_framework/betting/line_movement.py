"""
Line Movement Intelligence
=========================
Track line movements to identify sharp money and value opportunities.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from sports_framework.utils.persistence import execute_query, save_dataframe
from sports_framework.core.config import settings

logger = logging.getLogger(__name__)


class LineMovementTracker:
    """Track line movements to identify sharp money."""
    
    def __init__(self):
        self.movement_threshold = 0.5  # Half point is significant
        self.key_numbers = [3, 7, 10, 14]  # NFL key numbers
        self.poll_interval_minutes = 30  # Check every 30 minutes
        
    def analyze_movement(
        self, 
        game_id: str, 
        sport: str = "nfl"
    ) -> Dict[str, Any]:
        """
        Analyze line movement for a game.
        
        Returns:
            Dictionary with movement analysis including sharp side detection
        """
        # Get line history for this game
        line_history = execute_query("""
            SELECT 
                game_id,
                sportsbook,
                home_spread,
                total,
                home_moneyline,
                last_updated,
                loaded_at
            FROM betting_lines
            WHERE game_id = :game_id
                AND sport = :sport
                AND last_updated > NOW() - INTERVAL '48 hours'
            ORDER BY last_updated ASC
        """, {"game_id": game_id, "sport": sport})
        
        if line_history.empty:
            return {"game_id": game_id, "analysis": "no_data"}
        
        # Get opening lines (first record)
        opening = line_history.iloc[0]
        
        # Get current lines (most recent)
        current = line_history.iloc[-1]
        
        # Calculate movement
        spread_movement = current['home_spread'] - opening['home_spread']
        total_movement = current['total'] - opening['total']
        
        # Check for key number crosses
        key_number_crossed = self._check_key_number_cross(
            opening['home_spread'],
            current['home_spread']
        )
        
        # Determine sharp side
        # If line moves toward favorite despite heavy public on underdog = sharp on favorite
        sharp_side = self._determine_sharp_side(
            spread_movement,
            game_id,
            sport
        )
        
        return {
            "game_id": game_id,
            "sport": sport,
            "opening_spread": opening['home_spread'],
            "current_spread": current['home_spread'],
            "spread_movement": spread_movement,
            "opening_total": opening['total'],
            "current_total": current['total'],
            "total_movement": total_movement,
            "key_number_crossed": key_number_crossed,
            "sharp_side": sharp_side,
            "significance": self._rate_significance(
                spread_movement, total_movement, key_number_crossed
            ),
            "recommendation": self._generate_recommendation(
                sharp_side, spread_movement, key_number_crossed
            ),
        }
    
    def _check_key_number_cross(self, opening: float, current: float) -> Dict[str, Any]:
        """Check if line crossed key numbers (3, 7, 10, 14)."""
        crossed = []
        
        for key_num in self.key_numbers:
            # Check if opening was below and current is above (or vice versa)
            if (opening < key_num <= current) or (opening > key_num >= current):
                crossed.append({
                    "number": key_num,
                    "direction": "up" if current > opening else "down",
                    "significance": "HIGH" if key_num in [3, 7] else "MEDIUM"
                })
        
        return {
            "crossed": len(crossed) > 0,
            "key_numbers_crossed": crossed,
        }
    
    def _determine_sharp_side(
        self, 
        spread_movement: float, 
        game_id: str, 
        sport: str
    ) -> str:
        """Determine which side sharp money is on."""
        # If line moves toward favorite = sharp on favorite
        # If line moves toward underdog = sharp on underdog
        
        # Get public betting % if available
        public_data = execute_query("""
            SELECT bet_pct_home, money_pct_home
            FROM public_betting_data
            WHERE game_id = :game_id
        """, {"game_id": game_id})
        
        if not public_data.empty:
            bet_pct_home = public_data.iloc[0]['bet_pct_home']
            money_pct_home = public_data.iloc[0]['money_pct_home']
            
            # Reverse line movement detection
            # If public is heavy on home but line moves toward away = sharp on away
            if bet_pct_home > 60 and spread_movement < 0:
                return "away"  # Sharp on away despite public on home
            elif bet_pct_home < 40 and spread_movement > 0:
                return "home"  # Sharp on home despite public on away
        
        # Default: line movement direction indicates sharp side
        return "favorite" if spread_movement < 0 else "underdog"
    
    def _rate_significance(
        self, 
        spread_movement: float, 
        total_movement: float,
        key_number_cross: Dict[str, Any]
    ) -> str:
        """Rate the significance of line movement."""
        score = 0
        
        # Spread movement
        if abs(spread_movement) >= 2:
            score += 3  # Large move
        elif abs(spread_movement) >= 1:
            score += 2  # Medium move
        elif abs(spread_movement) >= 0.5:
            score += 1  # Small move
        
        # Total movement
        if abs(total_movement) >= 3:
            score += 2
        elif abs(total_movement) >= 1.5:
            score += 1
        
        # Key number cross
        if key_number_cross["crossed"]:
            high_sig_crosses = [k for k in key_number_cross["key_numbers_crossed"] 
                              if k["significance"] == "HIGH"]
            score += len(high_sig_crosses) * 2
            score += len(key_number_cross["key_numbers_crossed"]) * 1
        
        # Rate significance
        if score >= 5:
            return "HIGH"
        elif score >= 3:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _generate_recommendation(
        self, 
        sharp_side: str, 
        spread_movement: float,
        key_number_cross: Dict[str, Any]
    ) -> str:
        """Generate betting recommendation based on movement."""
        if key_number_cross["crossed"]:
            high_sig = any(k["significance"] == "HIGH" 
                          for k in key_number_cross["key_numbers_crossed"])
            if high_sig:
                return f"FADE_PUBLIC_{sharp_side.upper()}" if spread_movement > 0 else f"FOLLOW_SHARP_{sharp_side.upper()}"
        
        if abs(spread_movement) >= 1:
            return f"FOLLOW_SHARP_{sharp_side.upper()}"
        
        return "NO_CLEAR_SIGNAL"
    
    def get_significant_movements(
        self, 
        sport: str = "nfl",
        min_significance: str = "MEDIUM"
    ) -> pd.DataFrame:
        """Get all significant line movements for a sport."""
        significance_levels = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
        min_level = significance_levels.get(min_significance, 2)
        
        # Get recent line movements
        movements = execute_query("""
            SELECT 
                game_id,
                sportsbook,
                home_spread,
                total,
                last_updated,
                LAG(home_spread) OVER (PARTITION BY game_id, sportsbook ORDER BY last_updated) as prev_spread,
                LAG(total) OVER (PARTITION BY game_id, sportsbook ORDER BY last_updated) as prev_total
            FROM betting_lines
            WHERE sport = :sport
                AND last_updated > NOW() - INTERVAL '24 hours'
        """, {"sport": sport})
        
        if movements.empty:
            return pd.DataFrame()
        
        # Calculate movements
        movements['spread_movement'] = movements['home_spread'] - movements['prev_spread']
        movements['total_movement'] = movements['total'] - movements['prev_total']
        
        # Filter significant movements
        significant = movements[
            (abs(movements['spread_movement']) >= 0.5) |
            (abs(movements['total_movement']) >= 1.0)
        ].copy()
        
        # Add significance rating
        significant['significance'] = significant.apply(
            lambda row: self._rate_significance(
                row['spread_movement'], 
                row['total_movement'],
                self._check_key_number_cross(row['prev_spread'], row['home_spread'])
            ),
            axis=1
        )
        
        # Filter by minimum significance
        level_map = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
        significant = significant[
            significant['significance'].map(level_map) >= min_level
        ]
        
        return significant


# Global tracker instance
_tracker: Optional[LineMovementTracker] = None


def get_line_movement_tracker() -> LineMovementTracker:
    """Get or create line movement tracker."""
    global _tracker
    if _tracker is None:
        _tracker = LineMovementTracker()
    return _tracker


# Example usage:
if __name__ == "__main__":
    tracker = get_line_movement_tracker()
    
    # Analyze specific game
    analysis = tracker.analyze_movement("2025_01_25_KC_LV")
    print(f"Sharp side: {analysis['sharp_side']}")
    print(f"Significance: {analysis['significance']}")
    print(f"Recommendation: {analysis['recommendation']}")
    
    # Get all significant movements
    movements = tracker.get_significant_movements("nfl", "MEDIUM")
    print(f"Found {len(movements)} significant movements")