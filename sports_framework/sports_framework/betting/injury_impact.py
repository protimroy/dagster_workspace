"""
Injury Impact Quantification
===========================
Quantify the impact of injuries on game outcomes and betting lines.
"""

import pandas as pd
from typing import Dict, Any, List
from datetime import date
import logging

from sports_framework.core.models import InjuryReport, InjuryStatus
from sports_framework.utils.persistence import execute_query

logger = logging.getLogger(__name__)


class InjuryImpactCalculator:
    """Calculate the impact of injuries on team performance."""
    
    def __init__(self):
        # Base impact scores by position (in spread points)
        self.position_impact = {
            'QB': 6.0,      # Quarterback is most important
            'LT': 2.5,      # Left tackle protects blind side
            'CB': 1.5,      # Cornerback (top coverage)
            'EDGE': 1.5,    # Edge rusher (pass rush)
            'WR': 1.0,      # Wide receiver
            'RB': 1.0,      # Running back
            'DT': 1.0,      # Defensive tackle
            'S': 1.0,       # Safety
            'LB': 0.5,      # Linebacker
            'C': 0.5,       # Center
            'RG': 0.3,      # Right guard
            'LG': 0.3,      # Left guard
            'RT': 0.3,      # Right tackle
            'TE': 0.5,      # Tight end
            'K': 0.5,       # Kicker
            'P': 0.2,       # Punter
        }
        
        # Status multipliers (how much of impact applies)
        self.status_multiplier = {
            InjuryStatus.OUT: 1.0,        # Full impact if out
            InjuryStatus.DOUBTFUL: 0.7,   # 70% impact if doubtful
            InjuryStatus.QUESTIONABLE: 0.3,  # 30% impact if questionable
            InjuryStatus.HEALTHY: 0.0,    # No impact if healthy
        }
        
        # Starter vs backup multiplier
        self.starter_multiplier = 1.0
        self.backup_multiplier = 0.3
        
    def calculate_injury_impact(
        self, 
        injuries: List[InjuryReport]
    ) -> Dict[str, float]:
        """
        Calculate total injury impact for a team.
        
        Args:
            injuries: List of injury reports for the team
        
        Returns:
            Dictionary with impact breakdown
        """
        total_impact = 0.0
        impacts = []
        
        for injury in injuries:
            impact = self._calculate_single_injury_impact(injury)
            total_impact += impact
            
            impacts.append({
                "player_id": injury.player_id,
                "position": injury.position,
                "injury_type": injury.injury_type,
                "status": injury.status,
                "impact_points": impact,
                "is_starter": injury.is_starter,
            })
        
        # Cap total impact (can't be more than ~10 points)
        total_impact = min(total_impact, 10.0)
        
        return {
            "total_impact": total_impact,
            "num_injuries": len(injuries),
            "key_injuries": [i for i in impacts if i["impact_points"] >= 1.0],
            "all_impacts": impacts,
        }
    
    def _calculate_single_injury_impact(self, injury: InjuryReport) -> float:
        """Calculate impact of a single injury."""
        
        # Get base impact for position
        base_impact = self.position_impact.get(injury.position, 0.5)
        
        # Adjust for starter vs backup
        starter_mult = self.starter_multiplier if injury.is_starter else self.backup_multiplier
        base_impact *= starter_mult
        
        # Adjust for injury status
        status_mult = self.status_multiplier.get(injury.status, 0.5)
        base_impact *= status_mult
        
        # Adjust for position depth (if available)
        if injury.position_depth:
            # Deeper depth = less impact (better backup)
            depth_mult = max(0.3, 1.0 - (injury.position_depth * 0.1))
            base_impact *= depth_mult
        
        return round(base_impact, 2)
    
    def get_team_injury_impact(
        self, 
        team_id: str, 
        target_date: date
    ) -> Dict[str, Any]:
        """
        Get injury impact for a team on a specific date.
        
        Args:
            team_id: Team abbreviation
            target_date: Date to check injuries
        
        Returns:
            Injury impact summary
        """
        
        # Get active injuries for this team
        injuries = execute_query("""
            SELECT 
                player_id,
                injury_type,
                body_part,
                status,
                is_starter,
                position_depth,
                report_date
            FROM nfl_injury_reports
            WHERE team_id = :team_id
                AND report_date <= :target_date
                AND (expected_return IS NULL OR expected_return > :target_date)
                AND status IN ('out', 'doubtful', 'questionable')
            ORDER BY report_date DESC
        """, {"team_id": team_id, "target_date": target_date})
        
        if injuries.empty:
            return {
                "team_id": team_id,
                "total_impact": 0.0,
                "num_injuries": 0,
                "key_injuries": [],
                "status": "healthy",
            }
        
        # Convert to InjuryReport objects
        injury_reports = []
        for _, row in injuries.iterrows():
            injury_reports.append(InjuryReport(
                player_id=row['player_id'],
                team_id=team_id,
                injury_type=row['injury_type'],
                body_part=row['body_part'],
                status=row['status'],
                is_starter=row['is_starter'],
                position_depth=row.get('position_depth'),
                report_date=row['report_date'],
                sport="nfl",
                source="injury_reports",
            ))
        
        # Calculate impact
        impact = self.calculate_injury_impact(injury_reports)
        
        # Determine status
        if impact["total_impact"] >= 5.0:
            status = "critical"
        elif impact["total_impact"] >= 3.0:
            status = "significant"
        elif impact["total_impact"] >= 1.0:
            status = "moderate"
        else:
            status = "minor"
        
        impact["status"] = status
        impact["team_id"] = team_id
        
        return impact
    
    def compare_injury_impact(
        self, 
        home_team: str, 
        away_team: str, 
        game_date: date
    ) -> Dict[str, Any]:
        """
        Compare injury impact between two teams.
        
        Args:
            home_team: Home team ID
            away_team: Away team ID
            game_date: Game date
        
        Returns:
            Comparison of injury impacts
        """
        
        home_impact = self.get_team_injury_impact(home_team, game_date)
        away_impact = self.get_team_injury_impact(away_team, game_date)
        
        # Calculate net impact (positive = home team more impacted)
        net_impact = home_impact["total_impact"] - away_impact["total_impact"]
        
        # Determine which side has advantage
        if net_impact > 2.0:
            advantage = "away"  # Away team has injury advantage
            advantage_points = abs(net_impact)
        elif net_impact < -2.0:
            advantage = "home"  # Home team has injury advantage
            advantage_points = abs(net_impact)
        else:
            advantage = "even"
            advantage_points = 0.0
        
        return {
            "home_team": home_team,
            "away_team": away_team,
            "home_impact": home_impact["total_impact"],
            "away_impact": away_impact["total_impact"],
            "net_impact": net_impact,
            "advantage": advantage,
            "advantage_points": advantage_points,
            "home_key_injuries": home_impact["key_injuries"],
            "away_key_injuries": away_impact["key_injuries"],
        }
    
    def get_position_depth_chart(self, team_id: str, position: str) -> List[Dict[str, Any]]:
        """
        Get depth chart for a position to assess backup quality.
        
        Args:
            team_id: Team ID
            position: Position (e.g., 'QB', 'WR')
        
        Returns:
            List of players at position with depth ranking
        """
        
        # This would ideally come from a depth chart API or database
        # For now, return mock structure
        
        depth_chart = execute_query("""
            SELECT 
                player_id,
                full_name,
                position,
                jersey_number,
                years_experience,
                is_starter
            FROM nfl_players
            WHERE team_id = :team_id
                AND position = :position
                AND status = 'active'
            ORDER BY is_starter DESC, years_experience DESC
        """, {"team_id": team_id, "position": position})
        
        if depth_chart.empty:
            return []
        
        # Add depth ranking
        depth_chart['depth_rank'] = range(1, len(depth_chart) + 1)
        
        return depth_chart.to_dict('records')


# Global calculator instance
_calculator: Optional[InjuryImpactCalculator] = None


def get_injury_impact_calculator() -> InjuryImpactCalculator:
    """Get or create injury impact calculator."""
    global _calculator
    if _calculator is None:
        _calculator = InjuryImpactCalculator()
    return _calculator


# Example usage:
if __name__ == "__main__":
    calc = get_injury_impact_calculator()
    
    # Get team injury impact
    impact = calc.get_team_injury_impact("KC", date(2025, 1, 25))
    print(f"Chiefs injury impact: {impact['total_impact']} points")
    
    # Compare two teams
    comparison = calc.compare_injury_impact("KC", "LV", date(2025, 1, 25))
    print(f"Injury advantage: {comparison['advantage']} ({comparison['advantage_points']} points)")
    
    # Get depth chart
    depth = calc.get_position_depth_chart("KC", "QB")
    print(f"Chiefs QB depth: {len(depth)} players")