"""
Kelly Criterion Calculator
=========================
Optimal bet sizing for maximizing bankroll growth.
"""

import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class KellyCriterion:
    """Kelly Criterion calculator for optimal bet sizing."""
    
    def __init__(self):
        # Conservative fraction (quarter Kelly is standard for betting)
        self.kelly_fraction = 0.25
        
        # Minimum edge to bet (2% is reasonable threshold)
        self.min_edge_threshold = 0.02
        
        # Maximum bet size (5% of bankroll is conservative)
        self.max_bet_pct = 0.05
        
    def calculate_bet_size(
        self,
        probability_win: float,
        odds: float,
        bankroll: float,
        edge_threshold: float = None,
        kelly_fraction: float = None,
    ) -> Dict[str, Any]:
        """
        Calculate optimal bet size using Kelly Criterion.
        
        Formula: f* = (bp - q) / b
        where:
          b = decimal odds - 1 (net odds)
          p = probability of winning
          q = probability of losing (1 - p)
        
        Args:
            probability_win: Your estimated probability (0-1)
            odds: American odds (e.g., -110, +150) or decimal odds (e.g., 1.91, 2.5)
            bankroll: Current bankroll in dollars
            edge_threshold: Minimum edge to bet (default: 0.02 = 2%)
            kelly_fraction: Conservative fraction (default: 0.25 = quarter Kelly)
        
        Returns:
            Dictionary with bet sizing information
        """
        
        # Use defaults if not provided
        edge_threshold = edge_threshold or self.min_edge_threshold
        kelly_fraction = kelly_fraction or self.kelly_fraction
        
        # Convert American odds to decimal if needed
        if odds > 100:  # Positive odds (e.g., +150)
            odds_decimal = (odds / 100) + 1
        elif odds < 0:  # Negative odds (e.g., -110)
            odds_decimal = (100 / abs(odds)) + 1
        else:  # Already decimal
            odds_decimal = odds
        
        # Calculate implied probability from odds
        implied_prob = 1 / odds_decimal
        
        # Calculate your edge
        edge = probability_win - implied_prob
        
        # Don't bet if edge is too small
        if edge < edge_threshold:
            return {
                "bet_size": 0.0,
                "edge_pct": edge * 100,
                "kelly_fraction": 0.0,
                "recommended_fraction": 0.0,
                "should_bet": False,
                "reason": f"Edge too small: {edge*100:.2f}% (need {edge_threshold*100}%)",
                "bankroll": bankroll,
                "implied_prob": implied_prob * 100,
                "your_prob": probability_win * 100,
            }
        
        # Calculate Kelly fraction
        # f* = (bp - q) / b
        b = odds_decimal - 1  # Net odds received on win
        p = probability_win
        q = 1 - p
        
        kelly_frac = (b * p - q) / b
        
        # Don't bet if Kelly fraction is negative
        if kelly_frac <= 0:
            return {
                "bet_size": 0.0,
                "edge_pct": edge * 100,
                "kelly_fraction": kelly_frac,
                "recommended_fraction": 0.0,
                "should_bet": False,
                "reason": "Kelly fraction negative (negative expected value)",
                "bankroll": bankroll,
                "implied_prob": implied_prob * 100,
                "your_prob": probability_win * 100,
            }
        
        # Use conservative fraction (quarter Kelly)
        recommended_fraction = kelly_frac * kelly_fraction
        
        # Apply maximum bet size cap
        recommended_fraction = min(recommended_fraction, self.max_bet_pct)
        
        # Calculate bet size
        bet_size = bankroll * recommended_fraction
        
        return {
            "bet_size": round(bet_size, 2),
            "edge_pct": edge * 100,
            "kelly_fraction": kelly_frac,
            "recommended_fraction": recommended_fraction,
            "should_bet": True,
            "bankroll": bankroll,
            "implied_prob": implied_prob * 100,
            "your_prob": probability_win * 100,
            "expected_growth": kelly_frac * edge * 100,  # Expected bankroll growth
        }
    
    def calculate_multiple_bets(
        self,
        bets: List[Dict[str, Any]],
        bankroll: float,
        simultaneous_bets: int = 1,
    ) -> pd.DataFrame:
        """
        Calculate Kelly bet sizes for multiple simultaneous bets.
        
        When betting multiple games at once, need to adjust for correlation
        and risk of simultaneous losses.
        
        Args:
            bets: List of bet dictionaries with 'probability' and 'odds'
            bankroll: Current bankroll
            simultaneous_bets: Number of bets placed simultaneously
        
        Returns:
            DataFrame with bet sizing for each wager
        """
        
        results = []
        total_fraction = 0.0
        
        for i, bet in enumerate(bets):
            # Calculate individual Kelly size
            kelly_result = self.calculate_bet_size(
                probability_win=bet['probability'],
                odds=bet['odds'],
                bankroll=bankroll,
            )
            
            # Adjust for simultaneous bets (reduce size)
            if simultaneous_bets > 1:
                adjustment = 1 / simultaneous_bets ** 0.5  # Square root scaling
                kelly_result['bet_size'] *= adjustment
                kelly_result['recommended_fraction'] *= adjustment
            
            # Track total fraction of bankroll at risk
            total_fraction += kelly_result['recommended_fraction']
            
            results.append({
                'bet_id': bet.get('bet_id', f'bet_{i}'),
                'game_id': bet.get('game_id', 'unknown'),
                'bet_type': bet.get('bet_type', 'unknown'),
                **kelly_result,
            }])
        
        df = pd.DataFrame(results)
        
        # If total fraction exceeds 50% of bankroll, scale down all bets
        if total_fraction > 0.5:
            scale_factor = 0.5 / total_fraction
            df['bet_size'] *= scale_factor
            df['recommended_fraction'] *= scale_factor
            df['scaled_down'] = True
        else:
            df['scaled_down'] = False
        
        return df
    
    def get_bet_sizing_recommendations(
        self,
        bankroll: float,
        min_edge: float = 0.02,
        max_bets: int = 5,
    ) -> pd.DataFrame:
        """
        Get bet sizing recommendations for all identified value bets.
        
        Args:
            bankroll: Current bankroll
            min_edge: Minimum edge to consider
            max_bets: Maximum number of bets to place
        
        Returns:
            DataFrame with recommended bets and sizing
        """
        
        # Get value bets from power rating model
        from sports_framework.ml.power_ratings import get_power_rating_calculator
        
        calc = get_power_rating_calculator()
        value_bets = calc.find_value_bets(2025, min_edge=min_edge)
        
        if value_bets.empty:
            return pd.DataFrame()
        
        # Convert to bet format for Kelly calculator
        bets = []
        for _, bet in value_bets.iterrows():
            # Estimate win probability from edge
            # Rough conversion: 1 point edge ≈ 2.5% win probability advantage
            if 'spread_edge' in bet:
                prob_advantage = bet['spread_edge'] * 0.025
                base_prob = 0.50  # 50% for spread bets
                win_prob = base_prob + prob_advantage
            else:
                win_prob = 0.55  # Default for value bets
            
            # Cap probability at reasonable bounds
            win_prob = max(0.45, min(0.65, win_prob))
            
            bets.append({
                'bet_id': f"{bet['game_id']}_{bet['bet_type']}",
                'game_id': bet['game_id'],
                'bet_type': bet['bet_type'],
                'probability': win_prob,
                'odds': -110,  # Standard -110 odds
                'edge': bet.get('spread_edge', bet.get('total_edge', 0)),
            })
        
        # Limit to top N bets
        bets = bets[:max_bets]
        
        # Calculate Kelly sizing
        recommendations = self.calculate_multiple_bets(bets, bankroll)
        
        return recommendations


# Global calculator instance
_calculator: Optional[KellyCriterion] = None


def get_kelly_calculator() -> KellyCriterion:
    """Get or create Kelly calculator."""
    global _calculator
    if _calculator is None:
        _calculator = KellyCriterion()
    return _calculator


# Example usage:
if __name__ == "__main__":
    kelly = get_kelly_calculator()
    
    # Single bet example
    result = kelly.calculate_bet_size(
        probability_win=0.55,  # You estimate 55% win rate
        odds=-110,  # Standard -110 odds
        bankroll=10000,  # $10,000 bankroll
    )
    
    if result['should_bet']:
        print(f"✓ Bet ${result['bet_size']:.2f}")
        print(f"  Edge: {result['edge_pct']:.2f}%")
        print(f"  Kelly fraction: {result['kelly_fraction']:.3f}")
        print(f"  Recommended fraction: {result['recommended_fraction']:.3f}")
    else:
        print(f"✗ Don't bet: {result['reason']}")
    
    # Multiple bets example
    bets = [
        {'bet_id': 'bet1', 'probability': 0.55, 'odds': -110},
        {'bet_id': 'bet2', 'probability': 0.58, 'odds': -110},
        {'bet_id': 'bet3', 'probability': 0.52, 'odds': -110},
    ]
    
    multiple = kelly.calculate_multiple_bets(bets, bankroll=10000)
    print(f"\nMultiple bets total: ${multiple['bet_size'].sum():.2f}")
    print(f"Total bankroll fraction: {multiple['recommended_fraction'].sum():.3f}")