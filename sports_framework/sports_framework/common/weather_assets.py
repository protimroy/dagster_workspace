"""
Common Weather Assets
====================

Shared weather-related assets used across all sports.
"""

from datetime import date
from typing import List
import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import logging

from sports_framework.core.api_clients import OpenWeatherClient, APIClientFactory
from sports_framework.core.models import WeatherForecast
from sports_framework.utils.persistence import save_dataframe
from sports_framework.utils.data_quality import SportsDataQualitySuites

logger = logging.getLogger(__name__)


@asset(
    description="Weather forecasts for all upcoming outdoor games",
    group_name="weather",
    compute_kind="api",
)
def game_weather_forecasts(
    context: AssetExecutionContext,
    nfl_schedule: pd.DataFrame,
    # nba_schedule: pd.DataFrame,  # Add when implemented
    # nhl_schedule: pd.DataFrame,  # Add when implemented
    # mlb_schedule: pd.DataFrame,  # Add when implemented
) -> Output[pd.DataFrame]:
    """Fetch weather forecasts for all upcoming outdoor games."""
    context.log.info("Fetching weather forecasts...")
    
    client = OpenWeatherClient()
    
    if not client.is_available():
        context.log.error("OpenWeather API not available - check API key")
        return Output(pd.DataFrame(), metadata={"forecasts_found": 0, "error": "API not available"})
    
    # Combine all schedules
    all_schedules = [
        ("nfl", nfl_schedule),
        # ("nba", nba_schedule),
        # ("nhl", nhl_schedule),
        # ("mlb", mlb_schedule),
    ]
    
    all_forecasts = []
    
    for sport_name, schedule_df in all_schedules:
        if schedule_df.empty:
            continue
        
        # Filter for upcoming outdoor games
        upcoming_games = schedule_df[
            (pd.to_datetime(schedule_df['game_date']) >= pd.Timestamp(date.today())) &
            (schedule_df.get('is_dome', False) == False)  # Only outdoor games
        ]
        
        context.log.info(f"Checking weather for {len(upcoming_games)} {sport_name} games")
        
        for _, game in upcoming_games.iterrows():
            # Check if we have venue coordinates
            if pd.isna(game.get('venue_lat')) or pd.isna(game.get('venue_lon')):
                continue
            
            # Fetch forecast
            forecast_data = client.fetch_forecast(
                lat=game['venue_lat'],
                lon=game['venue_lon'],
                hours_ahead=24
            )
            
            if forecast_data and not forecast_data.get('mock'):
                forecasts = _parse_weather_forecast(
                    game['game_id'],
                    game.get('venue_id'),
                    forecast_data
                )
                all_forecasts.extend(forecasts)
    
    context.log.info(f"Fetched {len(all_forecasts)} weather forecasts")
    
    if not all_forecasts:
        return Output(pd.DataFrame(), metadata={"forecasts_found": 0})
    
    # Convert to DataFrame
    df = pd.DataFrame([forecast.model_dump() for forecast in all_forecasts])
    
    # Data quality checks
    if context.resources.data_quality_enabled and not df.empty:
        checker = SportsDataQualitySuites.check_weather_forecasts(df, "game_weather_forecasts")
        checker.raise_on_errors()
    
    # Save to database
    save_dataframe(
        df,
        table_name="weather_forecasts",
        unique_keys=["game_id", "forecast_hour"],
        if_exists="append",
        source="openweather"
    )
    
    return Output(
        df,
        metadata={
            "forecasts_found": len(df),
            "games_covered": df['game_id'].nunique(),
            "extreme_weather": len(df[df['overall_impact'] == 'extreme']),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


def _parse_weather_forecast(
    game_id: str,
    venue_id: str,
    forecast_data: Dict[str, Any]
) -> List[WeatherForecast]:
    """Parse OpenWeather forecast into WeatherForecast objects."""
    forecasts = []
    
    # Parse hourly forecasts
    for hour_data in forecast_data.get('hourly', [])[:24]:  # Next 24 hours
        forecast = WeatherForecast(
            game_id=game_id,
            venue_id=venue_id,
            sport=SportCode.NFL,  # Will be dynamic when multiple sports added
            source="openweather",
            temperature_f=hour_data.get('temp'),
            feels_like_f=hour_data.get('feels_like'),
            humidity_pct=hour_data.get('humidity'),
            wind_speed_mph=hour_data.get('wind_speed'),
            wind_gusts_mph=hour_data.get('wind_gust'),
            wind_direction=hour_data.get('wind_deg'),
            precipitation_pct=hour_data.get('pop', 0) * 100,  # Probability of precipitation
            conditions=hour_data.get('weather', [{}])[0].get('description'),
            forecast_hour=datetime.fromtimestamp(hour_data.get('dt')),
        )
        
        # Calculate impact scores
        forecast = _calculate_weather_impact(forecast)
        
        forecasts.append(forecast)
    
    return forecasts


def _calculate_weather_impact(forecast: WeatherForecast) -> WeatherForecast:
    """Calculate weather impact scores for betting."""
    # Default to no impact for dome games
    if forecast.is_dome:
        forecast.passing_impact_score = 100.0
        forecast.kicking_impact_score = 100.0
        forecast.overall_impact = "none"
        forecast.under_favorable = False
        forecast.rushing_favorable = False
        return forecast
    
    # Extract values with defaults
    temp = forecast.temperature_f or 70
    wind = forecast.wind_speed_mph or 5
    precip = forecast.precipitation_pct or 0
    wind_gusts = forecast.wind_gusts_mph or 10
    
    # Passing impact: cold, wind, and precipitation hurt passing
    # 100 = no impact, lower = worse conditions
    passing_penalty = (
        max(0, 50 - temp) * 0.5 +  # Cold penalty (starts at 50°F)
        wind * 1.5 +               # Wind penalty
        wind_gusts * 0.5 +         # Gusts penalty
        precip * 0.3               # Precipitation penalty
    )
    forecast.passing_impact_score = max(0, 100 - passing_penalty)
    
    # Kicking impact: wind is the biggest factor for FGs
    kicking_penalty = (
        wind * 2.5 +               # Wind penalty (major for kicking)
        wind_gusts * 1.0 +         # Gusts very bad for kicking
        precip * 0.2 +             # Precipitation penalty
        max(0, 32 - temp) * 0.3    # Freezing temps affect ball
    )
    forecast.kicking_impact_score = max(0, 100 - kicking_penalty)
    
    # Overall impact category
    if temp < 20 or wind > 25 or precip > 60 or wind_gusts > 35:
        forecast.overall_impact = "extreme"
    elif temp < 35 or wind > 18 or precip > 40 or wind_gusts > 25:
        forecast.overall_impact = "high"
    elif temp < 50 or wind > 12 or precip > 20:
        forecast.overall_impact = "medium"
    else:
        forecast.overall_impact = "low"
    
    # Betting implications
    forecast.under_favorable = (
        wind > 15 or 
        temp < 32 or 
        precip > 40 or
        forecast.overall_impact in ["high", "extreme"]
    )
    
    forecast.rushing_favorable = (
        wind > 15 or  # Can't pass in wind
        precip > 30   # Rain favors ground game
    )
    
    return forecast