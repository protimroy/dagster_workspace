"""
Core Data Models
===============

Pydantic models for all sports data entities.
"""

from datetime import date, datetime, time
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# ENUMS
# =============================================================================

class SportCode(str, Enum):
    """Supported sports."""
    NFL = "nfl"
    NBA = "nba"
    NHL = "nhl"
    MLB = "mlb"


class GameStatus(str, Enum):
    """Game status."""
    SCHEDULED = "scheduled"
    LIVE = "live"
    FINAL = "final"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"


class BetType(str, Enum):
    """Types of bets."""
    SPREAD = "spread"
    TOTAL = "total"
    MONEYLINE = "moneyline"
    PROP = "prop"


class InjuryStatus(str, Enum):
    """Injury status."""
    HEALTHY = "healthy"
    QUESTIONABLE = "questionable"
    DOUBTFUL = "doubtful"
    OUT = "out"
    IR = "ir"  # Injured Reserve


class WeatherImpact(str, Enum):
    """Weather impact severity."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"


# =============================================================================
# BASE MODELS
# =============================================================================

class BaseSportModel(BaseModel):
    """Base model with common configuration."""
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        use_enum_values=True,
    )
    
    sport: SportCode = Field(description="Sport code")
    loaded_at: datetime = Field(default_factory=datetime.now, description="When data was loaded")
    source: str = Field(description="Data source (e.g., 'espn', 'odds_api')")


# =============================================================================
# TEAM MODELS
# =============================================================================

class Team(BaseSportModel):
    """Team information."""
    
    team_id: str = Field(description="Unique team identifier")
    name: str = Field(description="Full team name")
    abbreviation: str = Field(description="Team abbreviation (e.g., 'KC')")
    city: str = Field(description="City name")
    conference: Optional[str] = Field(default=None, description="Conference (if applicable)")
    division: Optional[str] = Field(default=None, description="Division (if applicable)")
    
    # Venue information
    venue_name: Optional[str] = Field(default=None, description="Home stadium/arena")
    venue_city: Optional[str] = Field(default=None, description="Venue city")
    venue_state: Optional[str] = Field(default=None, description="Venue state")
    venue_capacity: Optional[int] = Field(default=None, description="Venue capacity")
    
    # Location for weather
    venue_lat: Optional[float] = Field(default=None, description="Venue latitude")
    venue_lon: Optional[float] = Field(default=None, description="Venue longitude")
    is_dome: bool = Field(default=False, description="Whether venue is indoor")
    
    # Metadata
    primary_color: Optional[str] = Field(default=None, description="Team primary color")
    secondary_color: Optional[str] = Field(default=None, description="Team secondary color")
    logo_url: Optional[str] = Field(default=None, description="Team logo URL")
    
    # Current season stats (optional)
    wins: Optional[int] = Field(default=None, description="Current season wins")
    losses: Optional[int] = Field(default=None, description="Current season losses")
    win_pct: Optional[float] = Field(default=None, description="Win percentage")


# =============================================================================
# PLAYER MODELS
# =============================================================================

class Player(BaseSportModel):
    """Player information."""
    
    player_id: str = Field(description="Unique player identifier")
    team_id: Optional[str] = Field(default=None, description="Current team ID")
    
    # Personal info
    first_name: str = Field(description="First name")
    last_name: str = Field(description="Last name")
    full_name: str = Field(description="Full name")
    jersey_number: Optional[int] = Field(default=None, description="Jersey number")
    
    # Position
    position: str = Field(description="Primary position")
    position_category: Optional[str] = Field(default=None, description="Position category (e.g., 'offense')")
    
    # Physical attributes
    height: Optional[str] = Field(default=None, description="Height (e.g., '6-2')")
    weight: Optional[int] = Field(default=None, description="Weight in pounds")
    age: Optional[int] = Field(default=None, description="Age")
    
    # Status
    status: str = Field(default="active", description="Player status")
    injury_status: Optional[InjuryStatus] = Field(default=None, description="Injury status")
    
    # Experience
    years_experience: Optional[int] = Field(default=None, description="Years in league")
    is_rookie: bool = Field(default=False, description="Whether player is a rookie")
    
    # Metadata
    headshot_url: Optional[str] = Field(default=None, description="Player headshot URL")


# =============================================================================
# GAME MODELS
# =============================================================================

class Game(BaseSportModel):
    """Game schedule and results."""
    
    game_id: str = Field(description="Unique game identifier")
    game_date: date = Field(description="Game date")
    game_time: Optional[time] = Field(default=None, description="Game time")
    game_datetime: Optional[datetime] = Field(default=None, description="Full game datetime")
    
    # Teams
    home_team_id: str = Field(description="Home team ID")
    away_team_id: str = Field(description="Away team ID")
    
    # Season info
    season: int = Field(description="Season year")
    week: Optional[int] = Field(default=None, description="Week number")
    game_type: str = Field(default="regular", description="Game type (regular, playoff, etc.)")
    
    # Venue
    venue_id: Optional[str] = Field(default=None, description="Venue ID")
    venue_name: Optional[str] = Field(default=None, description="Venue name")
    is_neutral_site: bool = Field(default=False, description="Whether neutral site")
    
    # Status
    status: GameStatus = Field(default=GameStatus.SCHEDULED, description="Game status")
    is_completed: bool = Field(default=False, description="Whether game is finished")
    
    # Scores (if completed)
    home_score: Optional[int] = Field(default=None, description="Home team score")
    away_score: Optional[int] = Field(default=None, description="Away team score")
    
    # Periods (sport-specific)
    current_period: Optional[int] = Field(default=None, description="Current period")
    time_remaining: Optional[str] = Field(default=None, description="Time remaining")
    
    # Metadata
    attendance: Optional[int] = Field(default=None, description="Attendance")
    duration: Optional[str] = Field(default=None, description="Game duration")
    
    # Betting context
    is_primetime: bool = Field(default=False, description="Whether primetime game")
    is_division_game: bool = Field(default=False, description="Whether division game")
    is_conference_game: bool = Field(default=False, description="Whether conference game")


# =============================================================================
# BETTING MODELS
# =============================================================================

class BettingLine(BaseSportModel):
    """Betting line for a specific market."""
    
    game_id: str = Field(description="Game ID")
    sportsbook: str = Field(description="Sportsbook name")
    bet_type: BetType = Field(description="Type of bet")
    
    # Line details
    team_id: Optional[str] = Field(default=None, description="Team ID (for spread/moneyline)")
    line: Optional[float] = Field(default=None, description="Line value (spread or total)")
    odds: int = Field(description="American odds (e.g., -110)")
    
    # Metadata
    last_updated: datetime = Field(description="When line was last updated")
    is_open: bool = Field(default=True, description="Whether line is still open")
    
    # Volume (if available)
    public_bets_pct: Optional[float] = Field(default=None, description="Public betting percentage")
    public_money_pct: Optional[float] = Field(default=None, description="Public money percentage")
    sharp_money_pct: Optional[float] = Field(default=None, description="Sharp money percentage")


class BettingLines(BaseSportModel):
    """Collection of betting lines for a game."""
    
    game_id: str = Field(description="Game ID")
    sportsbook: str = Field(description="Sportsbook name")
    
    # Spread
    home_spread: Optional[float] = Field(default=None, description="Home team spread")
    home_spread_odds: Optional[int] = Field(default=None, description="Home spread odds")
    away_spread: Optional[float] = Field(default=None, description="Away team spread")
    away_spread_odds: Optional[int] = Field(default=None, description="Away spread odds")
    
    # Total
    total: Optional[float] = Field(default=None, description="Over/under total")
    over_odds: Optional[int] = Field(default=None, description="Over odds")
    under_odds: Optional[int] = Field(default=None, description="Under odds")
    
    # Moneyline
    home_moneyline: Optional[int] = Field(default=None, description="Home moneyline")
    away_moneyline: Optional[int] = Field(default=None, description="Away moneyline")
    
    # Metadata
    last_updated: datetime = Field(description="When lines were last updated")
    is_open: bool = Field(default=True, description="Whether betting is open")


class LineMovement(BaseSportModel):
    """Historical line movement."""
    
    game_id: str = Field(description="Game ID")
    sportsbook: str = Field(description="Sportsbook name")
    bet_type: BetType = Field(description="Type of bet")
    
    # Movement details
    old_line: Optional[float] = Field(default=None, description="Previous line")
    new_line: Optional[float] = Field(default=None, description="New line")
    old_odds: Optional[int] = Field(default=None, description="Previous odds")
    new_odds: Optional[int] = Field(default=None, description="New odds")
    
    # Context
    movement_time: datetime = Field(description="When movement occurred")
    reason: Optional[str] = Field(default=None, description="Reason for movement (e.g., 'injury')")


# =============================================================================
# INJURY MODELS
# =============================================================================

class InjuryReport(BaseSportModel):
    """Injury report entry."""
    
    player_id: str = Field(description="Player ID")
    team_id: str = Field(description="Team ID")
    
    # Injury details
    injury_type: str = Field(description="Type of injury")
    body_part: Optional[str] = Field(default=None, description="Body part injured")
    severity: Optional[str] = Field(default=None, description="Injury severity")
    
    # Status
    status: InjuryStatus = Field(description="Current injury status")
    practice_status: Optional[str] = Field(default=None, description="Practice participation")
    
    # Timeline
    report_date: date = Field(description="Date of report")
    expected_return: Optional[date] = Field(default=None, description="Expected return date")
    
    # Impact
    impact_score: Optional[int] = Field(default=None, description="Impact on team (1-10)")
    is_starter: bool = Field(default=False, description="Whether player is a starter")
    position_depth: Optional[int] = Field(default=None, description="Position depth chart rank")


# =============================================================================
# WEATHER MODELS
# =============================================================================

class WeatherForecast(BaseSportModel):
    """Weather forecast for a game."""
    
    game_id: str = Field(description="Game ID")
    venue_id: Optional[str] = Field(default=None, description="Venue ID")
    
    # Conditions
    temperature_f: Optional[float] = Field(default=None, description="Temperature (Fahrenheit)")
    feels_like_f: Optional[float] = Field(default=None, description="Feels like temperature")
    humidity_pct: Optional[float] = Field(default=None, description="Humidity percentage")
    wind_speed_mph: Optional[float] = Field(default=None, description="Wind speed (MPH)")
    wind_gusts_mph: Optional[float] = Field(default=None, description="Wind gusts (MPH)")
    wind_direction: Optional[str] = Field(default=None, description="Wind direction")
    
    # Precipitation
    precipitation_pct: Optional[float] = Field(default=None, description="Precipitation chance (%)")
    precipitation_type: Optional[str] = Field(default=None, description="Precipitation type")
    precipitation_amount: Optional[float] = Field(default=None, description="Precipitation amount (inches)")
    
    # Visibility and conditions
    visibility_mi: Optional[float] = Field(default=None, description="Visibility (miles)")
    conditions: Optional[str] = Field(default=None, description="Weather conditions")
    
    # Impact assessment
    passing_impact_score: Optional[float] = Field(default=None, description="Impact on passing game (0-100)")
    kicking_impact_score: Optional[float] = Field(default=None, description="Impact on kicking game (0-100)")
    overall_impact: Optional[WeatherImpact] = Field(default=None, description="Overall impact")
    
    # Betting implications
    under_favorable: bool = Field(default=False, description="Whether conditions favor under")
    rushing_favorable: bool = Field(default=False, description="Whether conditions favor rushing")
    
    # Forecast metadata
    forecast_hour: datetime = Field(description="Hour this forecast applies to")
    is_dome: bool = Field(default=False, description="Whether game is in dome")


# =============================================================================
# PROJECTION MODELS
# =============================================================================

class GameProjection(BaseSportModel):
    """Game projection with betting recommendations."""
    
    game_id: str = Field(description="Game ID")
    projection_time: datetime = Field(description="When projection was made")
    
    # Score projections
    home_score_proj: float = Field(description="Projected home team score")
    away_score_proj: float = Field(description="Projected away team score")
    total_proj: float = Field(description="Projected total score")
    
    # Confidence
    confidence: float = Field(description="Confidence in projection (0-1)")
    confidence_reason: Optional[str] = Field(default=None, description="Why confidence is high/low")
    
    # Line analysis
    spread_edge: Optional[float] = Field(default=None, description="Edge on spread (positive = value)")
    total_edge: Optional[float] = Field(default=None, description="Edge on total (positive = value)")
    moneyline_edge: Optional[float] = Field(default=None, description="Edge on moneyline")
    
    # Recommendations
    recommended_bets: List[str] = Field(default_factory=list, description="Recommended bet types")
    bet_confidence: Dict[str, float] = Field(default_factory=dict, description="Confidence per bet type")
    
    # Factors
    key_factors: List[str] = Field(default_factory=list, description="Key factors influencing projection")
    
    # Results (if game completed)
    actual_home_score: Optional[int] = Field(default=None, description="Actual home score")
    actual_away_score: Optional[int] = Field(default=None, description="Actual away score")
    projection_accuracy: Optional[float] = Field(default=None, description="How accurate projection was")


# =============================================================================
# API RESPONSE MODELS
# =============================================================================

class APIResponse(BaseModel):
    """Standard API response wrapper."""
    
    success: bool = Field(description="Whether request succeeded")
    data: Optional[Any] = Field(default=None, description="Response data")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    status_code: Optional[int] = Field(default=None, description="HTTP status code")
    response_time_ms: Optional[int] = Field(default=None, description="Response time in milliseconds")
    cached: bool = Field(default=False, description="Whether response was cached")


class PaginatedResponse(BaseModel):
    """Paginated API response."""
    
    items: List[Any] = Field(description="List of items")
    total: int = Field(description="Total number of items")
    page: int = Field(description="Current page")
    per_page: int = Field(description="Items per page")
    has_next: bool = Field(description="Whether there are more pages")
    has_prev: bool = Field(description="Whether there are previous pages")