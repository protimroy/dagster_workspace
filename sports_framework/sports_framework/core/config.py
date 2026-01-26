"""
Configuration Management
=======================

Centralized configuration using Pydantic Settings.
All configuration via environment variables.
"""

import os
from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # =============================================================================
    # DATABASE
    # =============================================================================
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/sports_betting",
        env="DATABASE_URL"
    )
    db_host: str = Field(default="localhost", env="DB_HOST")
    db_port: int = Field(default=5432, env="DB_PORT")
    db_name: str = Field(default="sports_betting", env="DB_NAME")
    db_user: str = Field(default="postgres", env="DB_USER")
    db_password: str = Field(default="postgres", env="DB_PASSWORD")
    
    # Connection pooling
    db_pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=20, env="DB_MAX_OVERFLOW")
    db_pool_recycle: int = Field(default=3600, env="DB_POOL_RECYCLE")
    
    # =============================================================================
    # API KEYS
    # =============================================================================
    odds_api_key: Optional[str] = Field(default=None, env="ODDS_API_KEY")
    openweather_api_key: Optional[str] = Field(default=None, env="OPENWEATHER_API_KEY")
    sportradar_api_key: Optional[str] = Field(default=None, env="SPORTSRADAR_API_KEY")
    
    # =============================================================================
    # FEATURE FLAGS
    # =============================================================================
    enable_mock_data: bool = Field(default=False, env="ENABLE_MOCK_DATA")
    enable_rate_limiting: bool = Field(default=True, env="ENABLE_RATE_LIMITING")
    enable_data_quality_checks: bool = Field(default=True, env="ENABLE_DATA_QUALITY_CHECKS")
    enable_caching: bool = Field(default=True, env="ENABLE_CACHING")
    enable_debug_logging: bool = Field(default=False, env="ENABLE_DEBUG_LOGGING")
    enable_ml_predictions: bool = Field(default=False, env="ENABLE_ML_PREDICTIONS")
    
    # =============================================================================
    # SPORTS CONFIGURATION
    # =============================================================================
    enabled_sports: List[str] = Field(default=["nfl"], env="ENABLED_SPORTS")
    
    @field_validator("enabled_sports", mode="before")
    @classmethod
    def parse_enabled_sports(cls, v):
        """Parse comma-separated sports list."""
        if isinstance(v, str):
            return [sport.strip().lower() for sport in v.split(",")]
        return v
    
    # =============================================================================
    # DATA LOADING
    # =============================================================================
    load_all_historical_data: bool = Field(default=False, env="LOAD_ALL_HISTORICAL_DATA")
    recent_seasons_count: int = Field(default=2, env="RECENT_SEASONS_COUNT")
    
    # =============================================================================
    # RATE LIMITING
    # =============================================================================
    espn_rate_limit: int = Field(default=8, env="ESPN_RATE_LIMIT")
    odds_api_rate_limit: int = Field(default=2, env="ODDS_API_RATE_LIMIT")
    openweather_rate_limit: int = Field(default=60, env="OPENWEATHER_RATE_LIMIT")
    sportradar_rate_limit: int = Field(default=1, env="SPORTSRADAR_RATE_LIMIT")
    
    # =============================================================================
    # CACHING
    # =============================================================================
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")
    redis_ttl_seconds: int = Field(default=21600, env="REDIS_TTL_SECONDS")  # 6 hours
    cache_max_size: int = Field(default=1000, env="CACHE_MAX_SIZE")
    
    # =============================================================================
    # LOGGING
    # =============================================================================
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")
    
    # =============================================================================
    # DATA RETENTION
    # =============================================================================
    staging_data_retention_days: int = Field(default=30, env="STAGING_DATA_RETENTION_DAYS")
    predictions_retention_days: int = Field(default=365, env="PREDICTIONS_RETENTION_DAYS")
    api_log_retention_days: int = Field(default=7, env="API_LOG_RETENTION_DAYS")
    
    # =============================================================================
    # SCHEDULE
    # =============================================================================
    weather_forecast_days: int = Field(default=7, env="WEATHER_FORECAST_DAYS")
    line_update_cutoff_hours: int = Field(default=2, env="LINE_UPDATE_CUTOFF_HOURS")
    
    # =============================================================================
    # ML PREDICTIONS
    # =============================================================================
    model_refresh_hours: int = Field(default=24, env="MODEL_REFRESH_HOURS")
    min_games_for_training: int = Field(default=100, env="MIN_GAMES_FOR_TRAINING")
    
    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    def validate(self):
        """Validate critical settings."""
        warnings = []
        
        if self.enable_mock_data:
            warnings.append("⚠️  ENABLE_MOCK_DATA is enabled - production data will be FAKE!")
        
        if self.load_all_historical_data:
            warnings.append("⚠️  LOAD_ALL_HISTORICAL_DATA is enabled - this will load years of data")
        
        if not self.db_password or self.db_password == "postgres":
            warnings.append("⚠️  Database password is not set or is using default value")
        
        if warnings:
            print("\n".join(warnings))
        
        return len(warnings) == 0


# Global settings instance
settings = Settings()

# Validate on import
settings.validate()