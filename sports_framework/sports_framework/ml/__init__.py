"""
Machine Learning Module
======================

ML models and analytics:
- Power ratings
- Game embeddings
- Predictions
"""

from .power_ratings import PowerRatingCalculator, get_power_rating_calculator

__all__ = [
    "PowerRatingCalculator",
    "get_power_rating_calculator",
]