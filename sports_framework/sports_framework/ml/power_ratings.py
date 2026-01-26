"""
Power Rating Model
=================
Simple power rating system for NFL teams based on statistical performance.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from datetime import date
import logging

from sports_framework.utils.persistence import execute_query
from sports_framework.core.config import settings

logger = logging.getLogger(__name__)


class PowerRatingCalculator:
    """Calculate power ratings for NFL teams."""
    
    def __init__(self):
        # Weights for different components
        self.weights = {
            'ppd': 0.40,      # Points per game differential
            'yppd': 0.30,     # Yards per play differential
            'to_margin': 0.20,  # Turnover margin
            'sos': 0.10,      # Strength of schedule
        }
        
        # Home field advantage (points)
        self.home_field_advantage = 2.5
        
    def calculate_power_rating(self, team_id: str, season: int) -> float:
        """
        Calculate power rating for a team (0-100 scale, 50 = average).
        
        Args:
            team_id: Team abbreviation (e.g., 'KC')
            season: Season year (e.g., 2025)
        
        Returns:
            Power rating (0-100)
        """
        
        # Get team stats
        stats = execute_query("""
            SELECT 
                points_for,
                points_against,
                total_yards_per_game,
                yards_allowed_per_game,
                turnovers,
                turnovers_forced,
                strength_of_schedule
            FROM nfl_team_stats
            WHERE team_id = :team_id 
                AND season = :season
        """, {"team_id": team_id, "season": season})
        
        if stats.empty:
            logger.warning(f"No stats found for {team_id} in {season}")
            return 50.0  # Default to average
        
        row = stats.iloc[0]
        
        # 1. Points per game differential (PPD)
        # Scale: -100 to +100 -> 0 to 100
        ppd = (row['points_for'] - row['points_against']) / 17  # Per game
        ppd_rating = (ppd + 100) / 2  # Normalize to 0-100
        ppd_rating = np.clip(ppd_rating, 0, 100)
        
        # 2. Yards per play differential (YPPD)
        # Scale: -10 to +10 -> 0 to 100
        yppd = (row['total_yards_per_game'] - row['yards_allowed_per_game']) / 17
        yppd_per_play = yppd / 60  # Rough plays per game
        yppd_rating = (yppd_per_play + 10) / 0.2  # Normalize to 0-100
        yppd_rating = np.clip(yppd_rating, 0, 100)
        
        # 3. Turnover margin
        # Scale: -20 to +20 -> 0 to 100
        to_margin = (row['turnovers_forced'] - row['turnovers']) / 17  # Per game
        to_rating = (to_margin + 20) / 0.4  # Normalize to 0-100
        to_rating = np.clip(to_rating, 0, 100)
        
        # 4. Strength of schedule
        # Scale: 0 to 1 -> 0 to 100
        sos_rating = row.get('strength_of_schedule', 0.5) * 100
        
        # Weighted average
        rating = (
            ppd_rating * self.weights['ppd'] +
            yppd_rating * self.weights['yppd'] +
            to_rating * self.weights['to_margin'] +
            sos_rating * self.weights['sos']
        )
        
        logger.info(f"{team_id} power rating: {rating:.1f} (PPD: {ppd_rating:.1f}, YPPD: {yppd_rating:.1f}, TO: {to_rating:.1f}, SOS: {sos_rating:.1f})")
        
        return rating
    
    def project_spread(
        self, 
        home_team: str, 
        away_team: str, 
        season: int
    ) -> Dict[str, Any]:
        """
        Project spread based on power ratings.
        
        Formula: (Home Rating - Away Rating) / 2 + Home Field Advantage
        
        Args:
            home_team: Home team ID
            away_team: Away team ID
            season: Season year
        
        Returns:
            Dictionary with projected spread and ratings
        """
        
        # Get power ratings
        home_rating = self.calculate_power_rating(home_team, season)
        away_rating = self.calculate_power_rating(away_team, season)
        
        # Rating difference
        rating_diff = home_rating - away_rating
        
        # Convert to spread (each rating point = 0.5 spread points)
        spread = rating_diff / 2
        
        # Add home field advantage
        spread += self.home_field_advantage
        
        # Round to nearest half point
        spread = round(spread * 2) / 2
        
        return {
            "home_team": home_team,
            "away_team": away_team,
            "home_rating": home_rating,
            "away_rating": away_rating,
            "rating_difference": rating_diff,
            "projected_spread": spread,
            "home_field_advantage": self.home_field_advantage,
        }
    
    def project_total(
        self, 
        home_team: str, 
        away_team: str, 
        season: int
    ) -> float:
        """
        Project total based on team offensive and defensive ratings.
        """
        
        # Get team stats
        home_stats = execute_query("""
            SELECT points_for, points_against
            FROM nfl_team_stats
            WHERE team_id = :team_id AND season = :season
        """, {"team_id": home_team, "season": season})
        
        away_stats = execute_query("""
            SELECT points_for, points_against
            FROM nfl_team_stats
            WHERE team_id = :team_id AND season = :season
        """, {"team_id": away_team, "season": season})
        
        if home_stats.empty or away_stats.empty:
            return 45.0  # Default NFL total
        
        # Project score
        # Home team: (home offense + away defense) / 2
        home_score = (home_stats.iloc[0]['points_for'] + 
                     away_stats.iloc[0]['points_against']) / 2 / 17
        
        # Away team: (away offense + home defense) / 2
        away_score = (away_stats.iloc[0]['points_for'] + 
                     home_stats.iloc[0]['points_against']) / 2 / 17
        
        total = home_score + away_score
        
        # Round to nearest half point
        total = round(total * 2) / 2
        
        return total
    
    def find_value_bets(
        self, 
        season: int, 
        min_edge: float = 1.0
    ) -> pd.DataFrame:
        """
        Find games where model projects different line than market.
        
        Args:
            season: Season year
            min_edge: Minimum point difference to consider value
        
        Returns:
            DataFrame with value opportunities
        """
        
        # Get upcoming games
        games = execute_query("""
            SELECT 
                g.game_id,
                g.home_team_id,
                g.away_team_id,
                g.game_date,
                bl.home_spread as market_spread,
                bl.total as market_total
            FROM nfl_games g
            JOIN betting_lines bl ON g.game_id = bl.game_id
            WHERE g.season = :season
                AND g.game_date >= CURRENT_DATE
                AND g.is_completed = false
                AND bl.sportsbook = 'Consensus'
        """, {"season": season})
        
        if games.empty:
            return pd.DataFrame()
        
        value_bets = []
        
        for _, game in games.iterrows():
            # Project spread
            projection = self.project_spread(
                game['home_team_id'],
                game['away_team_id'],
                season
            )
            
            # Calculate edge
            if pd.notna(game['market_spread']):
                spread_edge = abs(projection['projected_spread'] - game['market_spread'])
                
                if spread_edge >= min_edge:
                    value_bets.append({
                        'game_id': game['game_id'],
                        'home_team': game['home_team_id'],
                        'away_team': game['away_team_id'],
                        'game_date': game['game_date'],
                        'market_spread': game['market_spread'],
                        'projected_spread': projection['projected_spread'],
                        'spread_edge': spread_edge,
                        'bet_type': 'spread',
                        'recommended_side': 'home' if projection['projected_spread'] < game['market_spread'] else 'away',
                    })
            
            # Project total
            if pd.notna(game['market_total']):
                projected_total = self.project_total(
                    game['home_team_id'],
                    game['away_team_id'],
                    season
                )
                
                total_edge = abs(projected_total - game['market_total'])
                
                if total_edge >= min_edge:
                    value_bets.append({
                        'game_id': game['game_id'],
                        'home_team': game['home_team_id'],
                        'away_team': game['away_team_id'],
                        'game_date': game['game_date'],
                        'market_total': game['market_total'],
                        'projected_total': projected_total,
                        'total_edge': total_edge,
                        'bet_type': 'total',
                        'recommended_side': 'over' if projected_total > game['market_total'] else 'under',
                    })
        
        df = pd.DataFrame(value_bets)
        
        if not df.empty:
            df = df.sort_values('spread_edge' if 'spread_edge' in df.columns else 'total_edge', 
                               ascending=False)
        
        return df


# Global calculator instance
_calculator: Optional[PowerRatingCalculator] = None


def get_power_rating_calculator() -> PowerRatingCalculator:
    """Get or create power rating calculator."""
    global _calculator
    if _calculator is None:
        _calculator = PowerRatingCalculator()
    return _calculator


# Example usage:
if __name__ == "__main__":
    calc = get_power_rating_calculator()
    
    # Calculate power rating
    rating = calc.calculate_power_rating("KC", 2025)
    print(f"Chiefs power rating: {rating:.1f}")
    
    # Project spread
    projection = calc.project_spread("KC", "LV", 2025)
    print(f"Projected spread: Chiefs {projection['projected_spread']:.1f}")
    
    # Find value bets
    value_bets = calc.find_value_bets(2025, min_edge=1.0)
    print(f"Found {len(value_bets)} value bets")
    if not value_bets.empty:
        print(value_bets.head())