"""
Betting Intelligence Assets
==========================
Dagster assets that combine all betting intelligence components.
"""

import pandas as pd
from datetime import date, datetime
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import logging

from sports_framework.betting.line_movement import get_line_movement_tracker
from sports_framework.betting.public_money import get_public_money_tracker
from sports_framework.betting.injury_impact import get_injury_impact_calculator
from sports_framework.ml.power_ratings import get_power_rating_calculator
from sports_framework.betting.kelly_criterion import get_kelly_calculator
from sports_framework.betting.performance_tracker import get_performance_tracker
from sports_framework.utils.persistence import execute_query, save_dataframe

logger = logging.getLogger(__name__)


@asset(
    description="Comprehensive betting analysis combining line movement, public money, injuries, and power ratings",
    group_name="betting_intelligence",
    compute_kind="python",
)
def nfl_betting_analysis(
    context: AssetExecutionContext,
    nfl_schedule: pd.DataFrame,
    real_time_betting_lines: pd.DataFrame,
    nfl_injury_reports: pd.DataFrame,
    nfl_team_stats: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    Generate comprehensive betting analysis for each game.
    
    Combines:
    - Line movement analysis (sharp money detection)
    - Public betting percentages (fade the public)
    - Injury impact quantification
    - Power rating projections
    - Kelly criterion bet sizing
    """
    
    context.log.info("Generating comprehensive betting analysis...")
    
    # Initialize all calculators
    line_tracker = get_line_movement_tracker()
    public_tracker = get_public_money_tracker()
    injury_calc = get_injury_impact_calculator()
    power_calc = get_power_rating_calculator()
    kelly_calc = get_kelly_calculator()
    
    analyses = []
    
    # Get upcoming games
    upcoming = nfl_schedule[
        pd.to_datetime(nfl_schedule['game_date']) >= pd.Timestamp(date.today())
    ].copy()
    
    if upcoming.empty:
        context.log.warning("No upcoming games found")
        return Output(pd.DataFrame(), metadata={"games_analyzed": 0})
    
    context.log.info(f"Analyzing {len(upcoming)} upcoming games")
    
    for _, game in upcoming.iterrows():
        game_id = game['game_id']
        home_team = game['home_team_id']
        away_team = game['away_team_id']
        game_date = pd.to_datetime(game['game_date']).date()
        
        context.log.info(f"\nAnalyzing: {away_team} @ {home_team}")
        
        analysis = {
            'game_id': game_id,
            'home_team': home_team,
            'away_team': away_team,
            'game_date': game_date,
            'analysis_timestamp': datetime.now(),
        }
        
        # 1. LINE MOVEMENT ANALYSIS
        try:
            line_analysis = line_tracker.analyze_movement(game_id, "nfl")
            analysis.update({
                'line_movement_significance': line_analysis.get('significance', 'LOW'),
                'sharp_side': line_analysis.get('sharp_side', 'unknown'),
                'spread_movement': line_analysis.get('spread_movement', 0),
                'key_number_crossed': line_analysis.get('key_number_crossed', {}).get('crossed', False),
                'line_recommendation': line_analysis.get('recommendation', 'NO_SIGNAL'),
            })
            context.log.info(f"  ✓ Line movement: {analysis['line_movement_significance']}")
        except Exception as e:
            context.log.error(f"  ✗ Line movement error: {e}")
            analysis.update({
                'line_movement_significance': 'ERROR',
                'sharp_side': 'unknown',
                'line_recommendation': 'NO_SIGNAL',
            })
        
        # 2. PUBLIC BETTING ANALYSIS
        try:
            public_data = public_tracker.fetch_public_percentages(game_id, "nfl")
            if public_data:
                analysis.update({
                    'public_consensus': public_data.get('public_consensus', 'BALANCED'),
                    'bets_on_home': public_data.get('bets_on_home', 50),
                    'money_on_home': public_data.get('money_on_home', 50),
                    'sharp_contrarian': public_data.get('sharp_contrarian', False),
                    'public_recommendation': public_data.get('recommendation', 'NO_SIGNAL'),
                })
                context.log.info(f"  ✓ Public betting: {analysis['public_consensus']}")
            else:
                analysis.update({
                    'public_consensus': 'NO_DATA',
                    'public_recommendation': 'NO_SIGNAL',
                })
        except Exception as e:
            context.log.error(f"  ✗ Public betting error: {e}")
            analysis.update({
                'public_consensus': 'ERROR',
                'public_recommendation': 'NO_SIGNAL',
            })
        
        # 3. INJURY IMPACT ANALYSIS
        try:
            injury_comparison = injury_calc.compare_injury_impact(
                home_team, away_team, game_date
            )
            analysis.update({
                'home_injury_impact': injury_comparison['home_impact'],
                'away_injury_impact': injury_comparison['away_impact'],
                'injury_advantage': injury_comparison['advantage'],
                'injury_advantage_points': injury_comparison['advantage_points'],
            })
            context.log.info(f"  ✓ Injury impact: {injury_comparison['advantage']} advantage")
        except Exception as e:
            context.log.error(f"  ✗ Injury analysis error: {e}")
            analysis.update({
                'home_injury_impact': 0,
                'away_injury_impact': 0,
                'injury_advantage': 'even',
                'injury_advantage_points': 0,
            })
        
        # 4. POWER RATING PROJECTIONS
        try:
            spread_projection = power_calc.project_spread(home_team, away_team, game_date.year)
            total_projection = power_calc.project_total(home_team, away_team, game_date.year)
            
            # Get market lines
            market_lines = real_time_betting_lines[
                real_time_betting_lines['game_id'] == game_id
            ]
            
            if not market_lines.empty:
                market_spread = market_lines.iloc[0]['home_spread']
                market_total = market_lines.iloc[0]['total']
                
                # Calculate edges
                spread_edge = abs(spread_projection['projected_spread'] - market_spread)
                total_edge = abs(total_projection - market_total) if market_total else 0
                
                analysis.update({
                    'projected_spread': spread_projection['projected_spread'],
                    'market_spread': market_spread,
                    'spread_edge': spread_edge,
                    'projected_total': total_projection,
                    'market_total': market_total,
                    'total_edge': total_edge,
                    'home_power_rating': spread_projection['home_rating'],
                    'away_power_rating': spread_projection['away_rating'],
                })
                context.log.info(f"  ✓ Power ratings: {spread_edge:.1f}pt edge on spread")
            else:
                analysis.update({
                    'projected_spread': spread_projection['projected_spread'],
                    'market_spread': None,
                    'spread_edge': 0,
                    'projected_total': total_projection,
                    'market_total': None,
                    'total_edge': 0,
                    'home_power_rating': spread_projection['home_rating'],
                    'away_power_rating': spread_projection['away_rating'],
                })
        except Exception as e:
            context.log.error(f"  ✗ Power rating error: {e}")
            analysis.update({
                'projected_spread': 0,
                'market_spread': None,
                'spread_edge': 0,
                'projected_total': 0,
                'market_total': None,
                'total_edge': 0,
                'home_power_rating': 50,
                'away_power_rating': 50,
            })
        
        # 5. KELLY CRITERION BET SIZING
        try:
            # Estimate win probability from edge
            # Rough conversion: 1 point edge ≈ 2.5% win probability advantage
            if analysis.get('spread_edge', 0) > 0:
                prob_advantage = analysis['spread_edge'] * 0.025
                win_prob = 0.50 + prob_advantage  # Base 50% for spread bets
                win_prob = max(0.45, min(0.65, win_prob))  # Cap at reasonable bounds
                
                # Get bankroll (would come from config or database)
                bankroll = 10000  # Default $10,000 bankroll
                
                kelly_result = kelly_calc.calculate_bet_size(
                    probability_win=win_prob,
                    odds=-110,  # Standard -110 odds
                    bankroll=bankroll,
                    edge_threshold=0.02,  # 2% minimum edge
                )
                
                analysis.update({
                    'kelly_bet_size': kelly_result['bet_size'],
                    'kelly_edge_pct': kelly_result['edge_pct'],
                    'kelly_recommended_fraction': kelly_result['recommended_fraction'],
                    'kelly_should_bet': kelly_result['should_bet'],
                    'estimated_win_prob': win_prob * 100,
                })
                
                if kelly_result['should_bet']:
                    context.log.info(f"  ✓ Kelly: Bet ${kelly_result['bet_size']:.2f} ({kelly_result['edge_pct']:.1f}% edge)")
                else:
                    context.log.info(f"  ✓ Kelly: No bet ({kelly_result['reason']})")
            else:
                analysis.update({
                    'kelly_bet_size': 0,
                    'kelly_edge_pct': 0,
                    'kelly_recommended_fraction': 0,
                    'kelly_should_bet': False,
                    'estimated_win_prob': 50,
                })
        except Exception as e:
            context.log.error(f"  ✗ Kelly calculation error: {e}")
            analysis.update({
                'kelly_bet_size': 0,
                'kelly_edge_pct': 0,
                'kelly_recommended_fraction': 0,
                'kelly_should_bet': False,
                'estimated_win_prob': 50,
            })
        
        # 6. FINAL RECOMMENDATION
        try:
            analysis['final_recommendation'] = generate_final_recommendation(analysis)
            analysis['confidence'] = calculate_confidence(analysis)
            
            context.log.info(f"  ✓ Final: {analysis['final_recommendation']} (confidence: {analysis['confidence']:.1f}%)")
        except Exception as e:
            context.log.error(f"  ✗ Final recommendation error: {e}")
            analysis['final_recommendation'] = 'NO_BET'
            analysis['confidence'] = 0.0
        
        analyses.append(analysis)
    
    # Convert to DataFrame
    df = pd.DataFrame(analyses)
    
    # Save to database
    save_dataframe(
        df,
        table_name="nfl_betting_analysis",
        unique_keys=["game_id", "analysis_timestamp"],
        if_exists="append",
    )
    
    # Generate summary metrics
    total_games = len(df)
    bet_opportunities = len(df[df['kelly_should_bet'] == True])
    high_confidence = len(df[df['confidence'] >= 70])
    
    # Find best bets
    best_bets = df[df['kelly_should_bet'] == True].nlargest(3, 'confidence')
    
    return Output(
        df,
        metadata={
            "games_analyzed": total_games,
            "bet_opportunities": bet_opportunities,
            "high_confidence_bets": high_confidence,
            "avg_edge": df['spread_edge'].mean() if 'spread_edge' in df.columns else 0,
            "best_bets": MetadataValue.json(best_bets.to_dict('records') if not best_bets.empty else {}),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data",
        }
    )


def generate_final_recommendation(analysis: Dict[str, Any]) -> str:
    """Generate final betting recommendation based on all factors."""
    
    signals = []
    
    # Line movement signal
    if analysis.get('line_movement_significance') == 'HIGH':
        if analysis.get('line_recommendation') == 'FOLLOW_SHARP_HOME':
            signals.append(('line', 'home', 3))
        elif analysis.get('line_recommendation') == 'FOLLOW_SHARP_AWAY':
            signals.append(('line', 'away', 3))
    
    # Public money signal
    if analysis.get('public_recommendation') == 'FADE_PUBLIC_HOME':
        signals.append(('public', 'away', 2))
    elif analysis.get('public_recommendation') == 'FADE_PUBLIC_AWAY':
        signals.append(('public', 'home', 2))
    
    # Injury signal
    if analysis.get('injury_advantage') == 'home':
        signals.append(('injury', 'home', 2))
    elif analysis.get('injury_advantage') == 'away':
        signals.append(('injury', 'away', 2))
    
    # Power rating signal
    if analysis.get('spread_edge', 0) >= 1.0:
        # Model shows value
        if analysis.get('projected_spread', 0) < analysis.get('market_spread', 0):
            signals.append(('model', 'home', 2))
        else:
            signals.append(('model', 'away', 2))
    
    # Count signals by side
    home_signals = sum(weight for _, side, weight in signals if side == 'home')
    away_signals = sum(weight for _, side, weight in signals if side == 'away')
    
    # Generate recommendation
    if home_signals >= 5 and away_signals <= 2:
        return "BET_HOME"
    elif away_signals >= 5 and home_signals <= 2:
        return "BET_AWAY"
    elif home_signals >= 3 and home_signals > away_signals + 2:
        return "LEAN_HOME"
    elif away_signals >= 3 and away_signals > home_signals + 2:
        return "LEAN_AWAY"
    else:
        return "NO_BET"


def calculate_confidence(analysis: Dict[str, Any]) -> float:
    """Calculate confidence score (0-100)."""
    
    score = 0
    
    # Line movement (max 30 points)
    if analysis.get('line_movement_significance') == 'HIGH':
        score += 30
    elif analysis.get('line_movement_significance') == 'MEDIUM':
        score += 15
    
    # Public money (max 20 points)
    if analysis.get('public_recommendation') in ['FADE_PUBLIC_HOME', 'FADE_PUBLIC_AWAY']:
        score += 20
    
    # Injury impact (max 20 points)
    if analysis.get('injury_advantage_points', 0) >= 3:
        score += 20
    elif analysis.get('injury_advantage_points', 0) >= 1:
        score += 10
    
    # Model edge (max 20 points)
    if analysis.get('spread_edge', 0) >= 2:
        score += 20
    elif analysis.get('spread_edge', 0) >= 1:
        score += 10
    
    # Kelly validation (max 10 points)
    if analysis.get('kelly_should_bet', False):
        score += 10
    
    return min(100, score)


@asset(
    description="Track ROI and performance of betting recommendations",
    group_name="betting_performance",
    compute_kind="python",
)
def betting_performance_tracker(
    context: AssetExecutionContext,
    nfl_betting_analysis: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """Track performance of betting recommendations."""
    
    context.log.info("Tracking betting performance...")
    
    tracker = get_performance_tracker()
    
    # Get completed games from last 30 days
    completed_games = execute_query("""
        SELECT 
            game_id,
            home_team_id,
            away_team_id,
            home_score,
            away_score,
            game_date
        FROM nfl_games
        WHERE is_completed = true
            AND game_date >= CURRENT_DATE - INTERVAL '30 days'
    """)
    
    if completed_games.empty:
        context.log.info("No completed games to track")
        return Output(pd.DataFrame(), metadata={"bets_tracked": 0})
    
    # Get our previous analyses for these games
    analyses = execute_query("""
        SELECT DISTINCT ON (game_id)
            game_id,
            final_recommendation,
            confidence,
            projected_spread,
            market_spread,
            kelly_bet_size,
            analysis_timestamp
        FROM nfl_betting_analysis
        WHERE game_id = ANY(:game_ids)
        ORDER BY game_id, analysis_timestamp DESC
    """, {"game_ids": completed_games['game_id'].tolist()})
    
    if analyses.empty:
        context.log.info("No analyses found for completed games")
        return Output(pd.DataFrame(), metadata={"bets_tracked": 0})
    
    # Merge with game results
    merged = completed_games.merge(analyses, on='game_id', how='inner')
    
    # Determine bet outcomes
    tracked_bets = []
    
    for _, row in merged.iterrows():
        # Determine actual result
        home_margin = row['home_score'] - row['away_score']
        
        # Check if our recommendation was correct
        if pd.notna(row['market_spread']):
            home_covered = (home_margin + row['market_spread']) > 0
            
            if row['final_recommendation'] == 'BET_HOME' and home_covered:
                result = 'win'
            elif row['final_recommendation'] == 'BET_AWAY' and not home_covered:
                result = 'win'
            elif row['final_recommendation'] in ['BET_HOME', 'BET_AWAY']:
                result = 'lose'
            else:
                result = 'no_bet'
        else:
            result = 'no_bet'
        
        # Calculate profit if we had bet
        if result in ['win', 'lose'] and pd.notna(row['kelly_bet_size']):
            if result == 'win':
                # Standard -110 odds
                profit = row['kelly_bet_size'] * 0.909  # 100/110
            else:
                profit = -row['kelly_bet_size']
        else:
            profit = 0.0
        
        tracked_bets.append({
            'game_id': row['game_id'],
            'game_date': row['game_date'],
            'recommendation': row['final_recommendation'],
            'confidence': row['confidence'],
            'result': result,
            'profit': profit,
            'home_margin': home_margin,
            'market_spread': row['market_spread'],
            'projected_spread': row['projected_spread'],
            'kelly_bet_size': row['kelly_bet_size'],
        })
    
    df = pd.DataFrame(tracked_bets)
    
    # Save to database
    save_dataframe(
        df,
        table_name="betting_performance",
        unique_keys=["game_id"],
        if_exists="append",
    )
    
    # Calculate metrics
    actual_bets = df[df['result'].isin(['win', 'lose'])]
    
    if not actual_bets.empty:
        win_rate = (actual_bets['result'] == 'win').mean() * 100
        total_profit = actual_bets['profit'].sum()
        total_staked = actual_bets['kelly_bet_size'].sum()
        roi_pct = (total_profit / total_staked) * 100 if total_staked > 0 else 0
        
        # By confidence
        high_conf = actual_bets[actual_bets['confidence'] >= 70]
        low_conf = actual_bets[actual_bets['confidence'] < 70]
        
        high_conf_win_rate = (high_conf['result'] == 'win').mean() * 100 if not high_conf.empty else 0
        low_conf_win_rate = (low_conf['result'] == 'win').mean() * 100 if not low_conf.empty else 0
        
        context.log.info(f"Performance: {win_rate:.1f}% win rate, {roi_pct:.2f}% ROI")
        context.log.info(f"High confidence: {high_conf_win_rate:.1f}% win rate")
        context.log.info(f"Low confidence: {low_conf_win_rate:.1f}% win rate")
    else:
        win_rate = 0
        total_profit = 0
        roi_pct = 0
        high_conf_win_rate = 0
        low_conf_win_rate = 0
    
    return Output(
        df,
        metadata={
            "games_tracked": len(df),
            "actual_bets": len(actual_bets),
            "win_rate": win_rate,
            "roi_pct": roi_pct,
            "total_profit": total_profit,
            "high_conf_win_rate": high_conf_win_rate,
            "low_conf_win_rate": low_conf_win_rate,
        }
    )


# Add to __init__.py to make these assets available
# In sports_framework/betting/__init__.py:
# from . import betting_intelligence_assets