"""
Closing Line Value (CLV) Tracker
================================
The #1 metric for proving you have a real betting edge.

CLV = Line you bet at - Closing line
Positive CLV = You beat the market
Negative CLV = Market beat you
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from sports_framework.utils.persistence import execute_query, save_dataframe

logger = logging.getLogger(__name__)


class CLVTracker:
    """Track Closing Line Value to measure betting skill."""
    
    def __init__(self):
        # Minimum CLV to consider a good bet (in points)
        self.min_clv_spread = 0.5  # Half point on spread = significant
        self.min_clv_total = 1.0   # Full point on total = significant
        
        # Track CLV by various dimensions
        self.clv_benchmarks = {
            'excellent': 1.0,  # 1+ point CLV = elite
            'good': 0.5,       # 0.5-1.0 = profitable
            'poor': -0.5,      # -0.5 to 0.5 = marginal
            'bad': -1.0,       # -1.0 or worse = losing
        }
    
    def record_bet_placement(
        self,
        bet_id: str,
        game_id: str,
        bet_type: str,
        line_at_bet: float,
        odds_at_bet: float,
        stake: float,
        timestamp: datetime,
        sportsbook: str,
    ) -> str:
        """
        Record when and where you placed a bet.
        
        Args:
            bet_id: Unique bet identifier
            game_id: Game identifier
            bet_type: 'spread', 'total', 'moneyline'
            line_at_bet: The line you got
            odds_at_bet: The odds you got
            stake: Amount wagered
            timestamp: When you placed the bet
            sportsbook: Which sportsbook
        
        Returns:
            Bet placement ID
        """
        
        placement_id = f"placement_{bet_id}_{int(timestamp.timestamp())}"
        
        placement_data = pd.DataFrame([{
            'placement_id': placement_id,
            'bet_id': bet_id,
            'game_id': game_id,
            'bet_type': bet_type,
            'line_at_bet': line_at_bet,
            'odds_at_bet': odds_at_bet,
            'stake': stake,
            'placed_at': timestamp,
            'sportsbook': sportsbook,
            'closing_line': None,  # To be updated later
            'closing_odds': None,  # To be updated later
            'clv_points': None,    # To be calculated
            'clv_percentage': None,  # To be calculated
        }])
        
        save_dataframe(
            placement_data,
            table_name="bet_placements",
            unique_keys=["placement_id"],
        )
        
        logger.info(f"Recorded bet placement: {placement_id} at {line_at_bet}")
        
        return placement_id
    
    def update_closing_line(
        self,
        placement_id: str,
        closing_line: float,
        closing_odds: float,
        timestamp: datetime,
    ):
        """
        Update with closing line (line at game time).
        
        Args:
            placement_id: Bet placement ID
            closing_line: Line at game time
            closing_odds: Odds at game time
            timestamp: When line closed
        """
        
        # Calculate CLV
        placement = execute_query("""
            SELECT line_at_bet, bet_type
            FROM bet_placements
            WHERE placement_id = :placement_id
        """, {"placement_id": placement_id})
        
        if placement.empty:
            logger.error(f"Placement {placement_id} not found")
            return
        
        line_at_bet = placement.iloc[0]['line_at_bet']
        bet_type = placement.iloc[0]['bet_type']
        
        # Calculate CLV in points
        if bet_type == 'spread':
            clv_points = line_at_bet - closing_line
        elif bet_type == 'total':
            clv_points = closing_line - line_at_bet  # Reverse for totals
        else:  # moneyline
            clv_points = 0  # Harder to quantify for moneyline
        
        # Calculate CLV in percentage terms (approximate)
        # 1 point on spread ≈ 2.5% edge
        # 1 point on total ≈ 1.5% edge
        if bet_type == 'spread':
            clv_percentage = clv_points * 2.5
        elif bet_type == 'total':
            clv_percentage = clv_points * 1.5
        else:
            clv_percentage = 0
        
        # Update database
        execute_query("""
            UPDATE bet_placements
            SET closing_line = :closing_line,
                closing_odds = :closing_odds,
                clv_points = :clv_points,
                clv_percentage = :clv_percentage,
                closed_at = :timestamp
            WHERE placement_id = :placement_id
        """, {
            "placement_id": placement_id,
            "closing_line": closing_line,
            "closing_odds": closing_odds,
            "clv_points": clv_points,
            "clv_percentage": clv_percentage,
            "timestamp": timestamp,
        })
        
        logger.info(f"Updated closing line for {placement_id}: CLV = {clv_points:.2f} points ({clv_percentage:.2f}%)")
    
    def calculate_clv_for_bet(self, bet_id: str) -> Optional[Dict[str, Any]]:
        """Calculate CLV for all placements of a bet."""
        
        placements = execute_query("""
            SELECT 
                placement_id,
                line_at_bet,
                closing_line,
                clv_points,
                clv_percentage,
                sportsbook
            FROM bet_placements
            WHERE bet_id = :bet_id
                AND closing_line IS NOT NULL
        """, {"bet_id": bet_id})
        
        if placements.empty:
            return None
        
        # Average CLV across all placements
        avg_clv_points = placements['clv_points'].mean()
        avg_clv_percentage = placements['clv_percentage'].mean()
        
        # Categorize CLV
        category = self._categorize_clv(avg_clv_points)
        
        return {
            "bet_id": bet_id,
            "num_placements": len(placements),
            "avg_clv_points": avg_clv_points,
            "avg_clv_percentage": avg_clv_percentage,
            "category": category,
            "placements": placements.to_dict('records'),
        }
    
    def _categorize_clv(self, clv_points: float) -> str:
        """Categorize CLV performance."""
        if clv_points >= self.clv_benchmarks['excellent']:
            return "EXCELLENT"
        elif clv_points >= self.clv_benchmarks['good']:
            return "GOOD"
        elif clv_points >= self.clv_benchmarks['poor']:
            return "POOR"
        elif clv_points >= self.clv_benchmarks['bad']:
            return "BAD"
        else:
            return "TERRIBLE"
    
    def get_clv_summary(self, days_back: int = 30) -> Dict[str, Any]:
        """Get CLV summary for recent bets."""
        
        since_date = datetime.now() - timedelta(days=days_back)
        
        placements = execute_query("""
            SELECT 
                bp.bet_id,
                pb.game_id,
                pb.bet_type,
                pb.result,
                bp.line_at_bet,
                bp.closing_line,
                bp.clv_points,
                bp.clv_percentage,
                bp.sportsbook,
                pb.placed_at
            FROM bet_placements bp
            JOIN placed_bets pb ON bp.bet_id = pb.bet_id
            WHERE pb.placed_at >= :since_date
                AND bp.closing_line IS NOT NULL
        """, {"since_date": since_date})
        
        if placements.empty:
            return {
                "period_days": days_back,
                "total_placements": 0,
                "avg_clv_points": 0,
                "avg_clv_percentage": 0,
                "category": "NO_DATA",
            }
        
        # Overall CLV
        avg_clv_points = placements['clv_points'].mean()
        avg_clv_percentage = placements['clv_percentage'].mean()
        
        # By bet type
        type_clv = placements.groupby('bet_type').agg({
            'clv_points': 'mean',
            'clv_percentage': 'mean',
            'bet_id': 'count',
        }).rename(columns={'bet_id': 'num_placements'})
        
        # By sportsbook
        book_clv = placements.groupby('sportsbook').agg({
            'clv_points': 'mean',
            'clv_percentage': 'mean',
            'bet_id': 'count',
        }).rename(columns={'bet_id': 'num_placements'})
        
        # Correlation with results
        winning_bets = placements[placements['result'] == 'win']
        losing_bets = placements[placements['result'] == 'lose']
        
        win_clv = winning_bets['clv_points'].mean() if not winning_bets.empty else 0
        lose_clv = losing_bets['clv_points'].mean() if not losing_bets.empty else 0
        
        return {
            "period_days": days_back,
            "total_placements": len(placements),
            "avg_clv_points": avg_clv_points,
            "avg_clv_percentage": avg_clv_percentage,
            "category": self._categorize_clv(avg_clv_points),
            "by_type": type_clv.to_dict(),
            "by_book": book_clv.to_dict(),
            "win_clv": win_clv,
            "lose_clv": lose_clv,
            "clv_correlation": "POSITIVE" if win_clv > lose_clv else "NEGATIVE",
        }
    
    def generate_clv_report(self, days_back: int = 30) -> str:
        """Generate a CLV performance report."""
        
        summary = self.get_clv_summary(days_back)
        
        if summary['total_placements'] == 0:
            return f"No CLV data available for last {days_back} days"
        
        report = f"""
Closing Line Value Report (Last {days_back} Days)
{'='*60}

Overall Performance:
- Total Placements: {summary['total_placements']}
- Average CLV: {summary['avg_clv_points']:.2f} points ({summary['avg_clv_percentage']:.2f}%)
- Category: {summary['category']}
- CLV Correlation: {summary['clv_correlation']}

Key Insights:
"""
        
        if summary['clv_correlation'] == 'POSITIVE':
            report += "✓ Positive correlation: Higher CLV = more wins\n"
        else:
            report += "⚠ Negative correlation: CLV not predictive (possible variance)\n"
        
        if summary['avg_clv_points'] >= 0.5:
            report += "✓ Excellent CLV: You're consistently beating the market\n"
        elif summary['avg_clv_points'] >= 0:
            report += "⚠ Marginal CLV: Work on timing and line shopping\n"
        else:
            report += "✗ Poor CLV: You're losing to the closing line (need improvement)\n"
        
        report += f"""
By Bet Type:
"""
        
        for bet_type, data in summary['by_type'].items():
            report += f"- {bet_type}: {data['clv_points']:.2f} points ({data['num_placements']} bets)\n"
        
        report += f"""
By Sportsbook:
"""
        
        for book, data in summary['by_book'].items():
            report += f"- {book}: {data['clv_points']:.2f} points ({data['num_placements']} bets)\n"
        
        report += f"""
Recommendations:
"""
        
        if summary['avg_clv_points'] < 0.5:
            report += "1. Improve timing: Bet earlier to get better lines\n"
            report += "2. Line shopping: Use multiple sportsbooks\n"
            report += "3. Steam chasing: Follow sharp money faster\n"
        
        if summary['clv_correlation'] != 'POSITIVE':
            report += "4. Review strategy: CLV should correlate with wins\n"
        
        report += "5. Focus on bet types with best CLV\n"
        
        return report


# Global tracker instance
_tracker: Optional[CLVTracker] = None


def get_clv_tracker() -> CLVTracker:
    """Get or create CLV tracker."""
    global _tracker
    if _tracker is None:
        _tracker = CLVTracker()
    return _tracker


# Example usage:
if __name__ == "__main__":
    tracker = get_clv_tracker()
    
    # Record a bet placement
    placement_id = tracker.record_bet_placement(
        bet_id="bet_123",
        game_id="2025_01_25_KC_LV",
        bet_type="spread",
        line_at_bet=-6.5,
        odds_at_bet=-110,
        stake=450.0,
        timestamp=datetime.now(),
        sportsbook="DraftKings",
    )
    
    # Update with closing line (after game starts)
    tracker.update_closing_line(
        placement_id=placement_id,
        closing_line=-7.5,
        closing_odds=-110,
        timestamp=datetime.now() + timedelta(hours=2),
    )
    
    # Calculate CLV
    clv = tracker.calculate_clv_for_bet("bet_123")
    print(f"CLV: {clv['avg_clv_points']:.2f} points ({clv['category']})")
    
    # Generate report
    report = tracker.generate_clv_report(30)
    print(report)