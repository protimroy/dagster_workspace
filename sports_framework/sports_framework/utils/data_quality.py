"""
Data Quality Framework
=====================

Comprehensive data quality checks for sports data.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DataQualityCheck:
    """Result of a data quality check."""
    
    def __init__(self, name: str, passed: bool, message: str, severity: str = "warning"):
        self.name = name
        self.passed = passed
        self.message = message
        self.severity = severity  # error, warning, info
        self.timestamp = datetime.now()
    
    def __repr__(self):
        status = "✓" if self.passed else "✗"
        return f"{status} {self.name}: {self.message}"


class DataQualityChecker:
    """Framework for running data quality checks."""
    
    def __init__(self, asset_name: str):
        self.asset_name = asset_name
        self.checks: List[DataQualityCheck] = []
    
    def add_check(self, name: str, condition: bool, message: str, severity: str = "warning"):
        """Add a custom check."""
        check = DataQualityCheck(name, condition, message, severity)
        self.checks.append(check)
        
        if not condition:
            if severity == "error":
                logger.error(f"{self.asset_name}: {check}")
            elif severity == "warning":
                logger.warning(f"{self.asset_name}: {check}")
            else:
                logger.info(f"{self.asset_name}: {check}")
        
        return check.passed
    
    def check_not_empty(self, df: pd.DataFrame, message: str = "DataFrame is empty"):
        """Check that DataFrame is not empty."""
        return self.add_check("not_empty", len(df) > 0, message, "error")
    
    def check_schema(self, df: pd.DataFrame, expected_columns: List[str]):
        """Check that DataFrame has expected columns."""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        has_all_columns = expected_set.issubset(actual_columns)
        
        missing = expected_set - actual_columns
        self.add_check(
            "schema",
            has_all_columns,
            f"Missing columns: {missing}. Expected: {expected_columns}, Got: {list(actual_columns)}",
            "error"
        )
    
    def check_no_nulls(self, df: pd.DataFrame, columns: List[str]):
        """Check that specified columns have no null values."""
        for col in columns:
            if col not in df.columns:
                self.add_check(
                    f"column_exists_{col}",
                    False,
                    f"Column '{col}' does not exist",
                    "error"
                )
                continue
            
            null_count = df[col].isnull().sum()
            self.add_check(
                f"no_nulls_{col}",
                null_count == 0,
                f"Column '{col}' has {null_count} null values",
                "error"
            )
    
    def check_unique(self, df: pd.DataFrame, columns: List[str]):
        """Check that specified columns have unique values."""
        # Handle empty DataFrame
        if len(df) == 0:
            self.add_check(
                f"unique_{'_'.join(columns)}",
                True,
                "DataFrame is empty, uniqueness check skipped",
                "info"
            )
            return
        
        # Check if columns exist
        for col in columns:
            if col not in df.columns:
                self.add_check(
                    f"column_exists_{col}",
                    False,
                    f"Column '{col}' does not exist for uniqueness check",
                    "error"
                )
                return
        
        # Check uniqueness
        try:
            subset = df[columns].drop_duplicates()
            is_unique = len(subset) == len(df)
            
            if not is_unique:
                duplicate_count = len(df) - len(subset)
                message = f"Columns {columns} have {duplicate_count} duplicate rows"
            else:
                message = f"Columns {columns} are unique"
            
            self.add_check(
                f"unique_{'_'.join(columns)}",
                is_unique,
                message,
                "error"
            )
        except Exception as e:
            self.add_check(
                f"unique_{'_'.join(columns)}",
                False,
                f"Error checking uniqueness: {str(e)}",
                "error"
            )
    
    def check_range(self, df: pd.DataFrame, column: str, min_val: float, max_val: float):
        """Check that values are within expected range."""
        if column not in df.columns:
            self.add_check(
                f"range_{column}",
                False,
                f"Column '{column}' does not exist for range check",
                "error"
            )
            return
        
        # Handle empty or all-null columns
        if df[column].isnull().all():
            self.add_check(
                f"range_{column}",
                True,
                f"Column '{column}' is all null, range check skipped",
                "info"
            )
            return
        
        # Get actual min/max (ignoring nulls)
        actual_min = df[column].min()
        actual_max = df[column].max()
        
        # Handle non-numeric data
        try:
            actual_min_float = float(actual_min)
            actual_max_float = float(actual_max)
        except (ValueError, TypeError):
            self.add_check(
                f"range_{column}",
                False,
                f"Column '{column}' contains non-numeric data",
                "error"
            )
            return
        
        in_range = (actual_min_float >= min_val) and (actual_max_float <= max_val)
        
        self.add_check(
            f"range_{column}",
            in_range,
            f"Column '{column}' values [{actual_min_float:.2f}, {actual_max_float:.2f}] "
            f"outside expected range [{min_val}, {max_val}]",
            "warning"
        )
    
    def check_freshness(self, df: pd.DataFrame, date_column: str, max_age_hours: int = 24):
        """Check that data is fresh (not too old)."""
        if date_column not in df.columns:
            self.add_check(
                "freshness",
                False,
                f"Date column '{date_column}' not found",
                "warning"
            )
            return
        
        # Handle empty or all-null date column
        if df[date_column].isnull().all():
            self.add_check(
                "freshness",
                True,
                f"Date column '{date_column}' is all null, freshness check skipped",
                "info"
            )
            return
        
        try:
            # Convert to datetime if needed
            dates = pd.to_datetime(df[date_column], errors='coerce')
            
            # Check if conversion succeeded
            if dates.isnull().all():
                self.add_check(
                    "freshness",
                    False,
                    f"Cannot convert column '{date_column}' to datetime",
                    "error"
                )
                return
            
            # Get max date (most recent)
            max_date = dates.max()
            
            # Handle NaT (Not a Time)
            if pd.isna(max_date):
                self.add_check(
                    "freshness",
                    False,
                    f"No valid dates found in column '{date_column}'",
                    "error"
                )
                return
            
            age_hours = (datetime.now() - max_date).total_seconds() / 3600
            is_fresh = age_hours <= max_age_hours
            
            self.add_check(
                "freshness",
                is_fresh,
                f"Data is {age_hours:.1f} hours old (max: {max_age_hours} hours)",
                "warning"
            )
        except Exception as e:
            self.add_check(
                "freshness",
                False,
                f"Error checking freshness: {str(e)}",
                "error"
            )
    
    def check_custom(self, name: str, check_func: Callable[[], bool], message: str):
        """Run a custom check function."""
        try:
            result = check_func()
            self.add_check(name, result, message)
        except Exception as e:
            self.add_check(name, False, f"Check failed with error: {str(e)}", "error")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all checks."""
        passed = sum(1 for check in self.checks if check.passed)
        total = len(self.checks)
        
        errors = [c for c in self.checks if not c.passed and c.severity == "error"]
        warnings = [c for c in self.checks if not c.passed and c.severity == "warning"]
        
        return {
            "asset": self.asset_name,
            "passed": passed,
            "total": total,
            "success_rate": passed / total if total > 0 else 1.0,
            "errors": len(errors),
            "warnings": len(warnings),
            "checks": [
                {
                    "name": c.name,
                    "passed": c.passed,
                    "message": c.message,
                    "severity": c.severity,
                }
                for c in self.checks
            ],
        }
    
    def raise_on_errors(self):
        """Raise exception if any error-level checks failed."""
        errors = [c for c in self.checks if not c.passed and c.severity == "error"]
        if errors:
            error_msg = f"Data quality errors in {self.asset_name}:\n"
            error_msg += "\n".join([f"  - {c.message}" for c in errors])
            raise DataQualityError(error_msg)
    
    def has_errors(self) -> bool:
        """Check if there are any error-level failures."""
        return any(not c.passed and c.severity == "error" for c in self.checks)
    
    def has_warnings(self) -> bool:
        """Check if there are any warning-level failures."""
        return any(not c.passed and c.severity == "warning" for c in self.checks)


class DataQualityError(Exception):
    """Exception raised when data quality checks fail."""
    pass


# Pre-built check suites for common assets
class SportsDataQualitySuites:
    """Pre-configured data quality checks for sports data."""
    
    @staticmethod
    def check_schedule(df: pd.DataFrame, asset_name: str = "schedule") -> DataQualityChecker:
        """Quality checks for schedule data."""
        checker = DataQualityChecker(asset_name)
        checker.check_not_empty(df, "Schedule data is empty")
        checker.check_schema(df, ["game_id", "home_team_id", "away_team_id", "game_date"])
        checker.check_no_nulls(df, ["game_id", "home_team_id", "away_team_id"])
        checker.check_unique(df, ["game_id"])
        checker.check_freshness(df, "game_date", max_age_hours=168)  # 1 week
        return checker
    
    @staticmethod
    def check_team_stats(df: pd.DataFrame, asset_name: str = "team_stats") -> DataQualityChecker:
        """Quality checks for team statistics."""
        checker = DataQualityChecker(asset_name)
        checker.check_not_empty(df, "Team stats data is empty")
        checker.check_schema(df, ["team_id", "points_per_game", "total_yards_per_game"])
        checker.check_no_nulls(df, ["team_id"])
        checker.check_range(df, "points_per_game", 0, 60)
        checker.check_range(df, "total_yards_per_game", 100, 600)
        return checker
    
    @staticmethod
    def check_betting_lines(df: pd.DataFrame, asset_name: str = "betting_lines") -> DataQualityChecker:
        """Quality checks for betting lines."""
        checker = DataQualityChecker(asset_name)
        checker.check_not_empty(df, "Betting lines data is empty")
        checker.check_schema(df, ["game_id", "sportsbook"])
        checker.check_no_nulls(df, ["game_id", "sportsbook"])
        
        # Check spread range if present
        if "home_spread" in df.columns:
            checker.check_range(df, "home_spread", -30, 30)
        
        # Check total range if present
        if "total" in df.columns:
            checker.check_range(df, "total", 30, 80)
        
        checker.check_freshness(df, "last_updated", max_age_hours=2)  # Lines update frequently
        return checker
    
    @staticmethod
    def check_injury_reports(df: pd.DataFrame, asset_name: str = "injury_reports") -> DataQualityChecker:
        """Quality checks for injury reports."""
        checker = DataQualityChecker(asset_name)
        checker.check_not_empty(df, "Injury report data is empty")
        checker.check_schema(df, ["player_id", "team_id", "injury_type", "status"])
        checker.check_no_nulls(df, ["player_id", "team_id", "injury_type", "status"])
        checker.check_freshness(df, "report_date", max_age_hours=48)  # Should be recent
        return checker
    
    @staticmethod
    def check_weather_forecasts(df: pd.DataFrame, asset_name: str = "weather_forecasts") -> DataQualityChecker:
        """Quality checks for weather forecasts."""
        checker = DataQualityChecker(asset_name)
        checker.check_not_empty(df, "Weather forecast data is empty")
        checker.check_schema(df, ["game_id", "forecast_hour"])
        checker.check_no_nulls(df, ["game_id", "forecast_hour"])
        
        # Check temperature range
        if "temperature_f" in df.columns:
            checker.check_range(df, "temperature_f", -20, 120)
        
        # Check wind speed range
        if "wind_speed_mph" in df.columns:
            checker.check_range(df, "wind_speed_mph", 0, 100)
        
        return checker


# Example usage:
if __name__ == "__main__":
    # Create sample data
    df = pd.DataFrame({
        "game_id": ["123", "456"],
        "home_team_id": ["KC", "BUF"],
        "away_team_id": ["BAL", "MIA"],
        "game_date": ["2025-01-20", "2025-01-21"]
    })
    
    # Run quality checks
    checker = SportsDataQualitySuites.check_schedule(df)
    summary = checker.get_summary()
    
    print(f"Success rate: {summary['success_rate']:.1%}")
    print(f"Errors: {summary['errors']}")
    print(f"Warnings: {summary['warnings']}")
    
    # Print all checks
    for check in summary["checks"]:
        status = "✓" if check["passed"] else "✗"
        print(f"{status} {check['name']}: {check['message']}")