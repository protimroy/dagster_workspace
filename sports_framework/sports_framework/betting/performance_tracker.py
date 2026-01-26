"""
Betting Performance Tracker
==========================
Track ROI, win rates, and identify profitable patterns.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from sports_framework.utils.persistence import execute_query, save_dataframe

logger = logging.getLogger(__name__)


class BettingPerformanceTracker:
    """Track betting performance and ROI over time."""
    
    def __init__(self):
        self.min_sample_size = 10  # Minimum bets to analyze pattern
        
    def record_bet(
        self,
        game_id: str,
        bet_type: str,
        bet_side: str,
        line: float,
        odds: float,
        stake: float,
        your_probability: float,
        market_probability: float,
        edge: float,
        model_used: str = "power_ratings",
        notes: str = "",
    ) -> str:
        """
        Record a bet to track performance.
        
        Args:
            game_id: Game identifier
            bet_type: 'spread', 'total', 'moneyline'
            bet_side: 'home', 'away', 'over', 'under'
            line: The line/total at time of bet
            odds: American odds (e.g., -110)
            stake: Amount wagered
            your_probability: Your estimated win probability
            market_probability: Market implied probability
            edge: Your edge over market (decimal, e.g., 0.05 = 5%)
            model_used: Which model generated the pick
            notes: Any additional notes
        
        Returns:
            Unique bet ID
        """
        import uuid
        
        bet_id = f"bet_{game_id}_{bet_type}_{int(datetime.now().timestamp())}"
        
        # Save to database
        bet_data = pd.DataFrame([{
            'bet_id': bet_id,
            'game_id': game_id,
            'bet_type': bet_type,
            'bet_side': bet_side,
            'line': line,
            'odds': odds,
            'stake': stake,
            'your_probability': your_probability,
            'market_probability': market_probability,
            'edge': edge,
            'model_used': model_used,
            'notes': notes,
            'placed_at': datetime.now(),
            'result': None,  # To be updated after game
            'profit': None,
            'settled_at': None,
        }])
        
        save_dataframe(bet_data, table_name="placed_bets", unique_keys=["bet_id"])
        
        logger.info(f"Recorded bet {bet_id}: {bet_type} {bet_side} on {game_id}")
        
        return bet_id
    
    def update_bet_result(
        self,
        bet_id: str,
        result: str,
        actual_score_home: int,
        actual_score_away: int,
    ):
        """
        Update bet result after game ends.
        
        Args:
            bet_id: Unique bet identifier
            result: 'win', 'lose', or 'push'
            actual_score_home: Final home team score
            actual_score_away: Final away team score
        """
        
        # Get bet details
        bet = execute_query("""
            SELECT * FROM placed_bets
            WHERE bet_id = :bet_id
        """, {"bet_id": bet_id})
        
        if bet.empty:
            logger.error(f"Bet {bet_id} not found")
            return
        
        bet = bet.iloc[0]
        
        # Calculate profit
        if result == 'win':
            # Profit = stake * (odds / 100) for positive odds
            # Profit = stake * (100 / abs(odds)) for negative odds
            if bet['odds'] > 0:
                profit = bet['stake'] * (bet['odds'] / 100)
            else:
                profit = bet['stake'] * (100 / abs(bet['odds']))
        elif result == 'lose':
            profit = -bet['stake']
        else:  # push
            profit = 0.0
        
        # Update bet record
        execute_query("""
            UPDATE placed_bets
            SET result = :result,
                profit = :profit,
                settled_at = :settled_at,
                actual_score_home = :actual_score_home,
                actual_score_away = :actual_score_away
            WHERE bet_id = :bet_id
        """, {
            "bet_id": bet_id,
            "result": result,
            "profit": profit,
            "settled_at": datetime.now(),
            "actual_score_home": actual_score_home,
            "actual_score_away": actual_score_away,
        })
        
        logger.info(f"Updated bet {bet_id}: {result}, profit: ${profit:.2f}")
    
    def get_roi_summary(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Get ROI summary for recent bets.
        
        Args:
            days_back: Number of days to analyze (default: 30)
        
        Returns:
            Dictionary with ROI metrics
        """
        
        since_date = datetime.now() - timedelta(days=days_back)
        
        bets = execute_query("""
            SELECT 
                bet_type,
                bet_side,
                model_used,
                result,
                stake,
                profit,
                edge,
                placed_at
            FROM placed_bets
            WHERE settled_at IS NOT NULL
                AND settled_at >= :since_date
            ORDER BY settled_at DESC
        """, {"since_date": since_date})
        
        if bets.empty:
            return {
                "period_days": days_back,
                "total_bets": 0,
                "roi_pct": 0.0,
                "win_rate": 0.0,
                "total_profit": 0.0,
                "total_staked": 0.0,
            }
        
        # Overall metrics
        total_bets = len(bets)
        wins = len(bets[bets['result'] == 'win'])
        losses = len(bets[bets['result'] == 'lose'])
        pushes = len(bets[bets['result'] == 'push'])
        
        total_staked = bets['stake'].sum()
        total_profit = bets['profit'].sum()
        roi_pct = (total_profit / total_staked) * 100 if total_staked > 0 else 0
        win_rate = (wins / total_bets) * 100 if total_bets > 0 else 0
        
        # By bet type
        type_performance = bets.groupby('bet_type').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        type_performance['roi_pct'] = (type_performance['profit'] / type_performance['stake']) * 100
        type_performance['win_rate'] = bets[bets['result'] == 'win'].groupby('bet_type').size() / type_performance['bets'] * 100
        
        # By model
        model_performance = bets.groupby('model_used').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        model_performance['roi_pct'] = (model_performance['profit'] / model_performance['stake']) * 100
        
        # Edge analysis (did higher edge bets perform better?)
        bets['edge_bin'] = pd.cut(bets['edge'], bins=[0, 0.02, 0.05, 0.10, 1.0], 
                                 labels=['low', 'medium', 'high', 'very_high'])
        edge_performance = bets.groupby('edge_bin').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        edge_performance['roi_pct'] = (edge_performance['profit'] / edge_performance['stake']) * 100
        
        return {
            "period_days": days_back,
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": win_rate,
            "total_staked": total_staked,
            "total_profit": total_profit,
            "roi_pct": roi_pct,
            "by_type": type_performance.to_dict(),
            "by_model": model_performance.to_dict(),
            "by_edge": edge_performance.to_dict(),
        }
    
    def identify_profitable_patterns(self, min_bets: int = 20) -> pd.DataFrame:
        """
        Identify which betting patterns are most profitable.
        
        Args:
            min_bets: Minimum number of bets to consider a pattern
        
        Returns:
            DataFrame with profitable patterns
        """
        
        # Get all settled bets
        bets = execute_query("""
            SELECT 
                bet_type,
                bet_side,
                model_used,
                edge,
                result,
                profit,
                stake,
                EXTRACT(dow FROM placed_at) as day_of_week,
                EXTRACT(hour FROM placed_at) as hour_of_day
            FROM placed_bets
            WHERE settled_at IS NOT NULL
        """)
        
        if bets.empty or len(bets) < min_bets:
            return pd.DataFrame()
        
        patterns = []
        
        # By bet type
        type_perf = bets.groupby('bet_type').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        type_perf = type_perf[type_perf['bets'] >= min_bets]
        type_perf['roi_pct'] = (type_perf['profit'] / type_perf['stake']) * 100
        type_perf['win_rate'] = bets[bets['result'] == 'win'].groupby('bet_type').size() / type_perf['bets'] * 100
        
        for bet_type, data in type_perf.iterrows():
            patterns.append({
                'pattern_type': 'bet_type',
                'pattern_value': bet_type,
                'roi_pct': data['roi_pct'],
                'win_rate': data['win_rate'],
                'bets': data['bets'],
                'profit': data['profit'],
            })
        
        # By model
        model_perf = bets.groupby('model_used').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        model_perf = model_perf[model_perf['bets'] >= min_bets]
        model_perf['roi_pct'] = (model_perf['profit'] / model_perf['stake']) * 100
        
        for model, data in model_perf.iterrows():
            patterns.append({
                'pattern_type': 'model',
                'pattern_value': model,
                'roi_pct': data['roi_pct'],
                'win_rate': bets[bets['result'] == 'win'].groupby('model_used').size().get(model, 0) / data['bets'] * 100,
                'bets': data['bets'],
                'profit': data['profit'],
            })
        
        # By edge range
        bets['edge_bin'] = pd.cut(bets['edge'], bins=[0, 0.02, 0.05, 0.10, 1.0], 
                                 labels=['low', 'medium', 'high', 'very_high'])
        edge_perf = bets.groupby('edge_bin').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        edge_perf = edge_perf[edge_perf['bets'] >= min_bets // 4]  # Lower threshold for edge bins
        edge_perf['roi_pct'] = (edge_perf['profit'] / edge_perf['stake']) * 100
        
        for edge_bin, data in edge_perf.iterrows():
            patterns.append({
                'pattern_type': 'edge_range',
                'pattern_value': str(edge_bin),
                'roi_pct': data['roi_pct'],
                'win_rate': bets[(bets['result'] == 'win') & (bets['edge_bin'] == edge_bin)].groupby('edge_bin').size().get(edge_bin, 0) / data['bets'] * 100,
                'bets': data['bets'],
                'profit': data['profit'],
            })
        
        # By day of week
        dow_perf = bets.groupby('day_of_week').agg({
            'profit': 'sum',
            'stake': 'sum',
            'result': 'count',
        }).rename(columns={'result': 'bets'})
        dow_perf = dow_perf[dow_perf['bets'] >= min_bets // 7]  # Lower threshold for DOW
        dow_perf['roi_pct'] = (dow_perf['profit'] / dow_perf['stake']) * 100
        
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        for dow, data in dow_perf.iterrows():
            patterns.append({
                'pattern_type': 'day_of_week',
                'pattern_value': day_names[int(dow)],
                'roi_pct': data['roi_pct'],
                'win_rate': bets[(bets['result'] == 'win') & (bets['day_of_week'] == dow)].groupby('day_of_week').size().get(dow, 0) / data['bets'] * 100,
                'bets': data['bets'],
                'profit': data['profit'],
            })
        
        df = pd.DataFrame(patterns)
        
        if not df.empty:
            df = df.sort_values('roi_pct', ascending=False)
        
        return df
    
    def generate_performance_report(self, days_back: int = 30) -> str:
        """Generate a text report of betting performance."""
        
        summary = self.get_roi_summary(days_back)
        patterns = self.identify_profitable_patterns()
        
        report = f"""
Betting Performance Report (Last {days_back} Days)
{'='*50}

Overall Performance:
- Total Bets: {summary['total_bets']}
- Win Rate: {summary['win_rate']:.1f}%
- ROI: {summary['roi_pct']:.2f}%
- Profit: ${summary['total_profit']:.2f}
- Total Staked: ${summary['total_staked']:.2f}

Profitable Patterns:
"""
        
        if not patterns.empty:
            for _, pattern in patterns.head(10).iterrows():
                report += f"- {pattern['pattern_type']}: {pattern['pattern_value']} "
                report += f"({pattern['roi_pct']:.1f}% ROI, {pattern['bets']} bets)\n"
        else:
            report += "- No patterns with sufficient sample size\n"
        
        report += f"""
Recommendations:
1. Focus on bet types with highest ROI
2. Consider increasing bet size on high-edge opportunities
3. Review losing patterns and potentially eliminate them
4. Maintain discipline with Kelly criterion sizing
"""
        
        return report


# Global tracker instance
_tracker: Optional[BettingPerformanceTracker] = None


def get_performance_tracker() -> BettingPerformanceTracker:
    """Get or create performance tracker."""
    global _tracker
    if _tracker is None:
        _tracker = BettingPerformanceTracker()
    return _tracker


# Example usage:
if __name__ == "__main__":
    tracker = get_performance_tracker()
    
    # Record a bet
    bet_id = tracker.record_bet(
        game_id="2025_01_25_KC_LV",
        bet_type="spread",
        bet_side="home",
        line=-7.5,
        odds=-110,
        stake=450.0,
        your_probability=0.55,
        market_probability=0.524,
        edge=0.026,
    )
    print(f"Recorded bet: {bet_id}")
    
    # Update result after game
    tracker.update_bet_result(bet_id, "win", 31, 20)
    
    # Get ROI summary
    summary = tracker.get_roi_summary(30)
    print(f"ROI: {summary['roi_pct']:.2f}%")
    
    # Identify patterns
    patterns = tracker.identify_profitable_patterns()
    print(f"Found {len(patterns)} patterns")
    
    # Generate report
    report = tracker.generate_performance_report()
    print(report)