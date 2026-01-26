"""
Rate Limiter
============

Token bucket rate limiter for API calls with exponential backoff.
"""

import time
import threading
from functools import wraps
from typing import Callable, Optional
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token bucket rate limiter."""
    
    def __init__(self, calls_per_second: float = 10.0):
        """
        Initialize rate limiter.
        
        Args:
            calls_per_second: Maximum calls allowed per second
        """
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0
        self.call_count = 0
        self.window_start = time.time()
        self._lock = threading.Lock()
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator to rate limit a function."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.wait_if_needed()
            result = func(*args, **kwargs)
            self.record_call()
            return result
        return wrapper
    
    def wait_if_needed(self):
        """Wait if we're calling too fast."""
        with self._lock:
            now = time.time()
            time_since_last = now - self.last_call_time
            
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.3f}s")
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()
    
    def record_call(self):
        """Record that a call was made."""
        with self._lock:
            now = time.time()
            
            # Reset window if needed
            if now - self.window_start >= 1.0:
                self.window_start = now
                self.call_count = 0
            
            self.call_count += 1
    
    def get_stats(self) -> dict:
        """Get current rate limiting stats."""
        with self._lock:
            return {
                "calls_per_second": self.calls_per_second,
                "call_count": self.call_count,
                "window_start": self.window_start,
                "last_call_time": self.last_call_time,
                "min_interval": self.min_interval,
            }


def rate_limited(calls_per_second: float = 10.0):
    """
    Decorator to rate limit any function.
    
    Usage:
        @rate_limited(calls_per_second=5.0)
        def my_api_call():
            ...
    """
    limiter = RateLimiter(calls_per_second=calls_per_second)
    return limiter


class RateLimitManager:
    """Manages multiple rate limiters for different APIs."""
    
    def __init__(self):
        self.limiters = {}
    
    def get_limiter(self, name: str, calls_per_second: float) -> RateLimiter:
        """Get or create a rate limiter."""
        if name not in self.limiters:
            self.limiters[name] = RateLimiter(calls_per_second)
        return self.limiters[name]
    
    def get_all_stats(self) -> dict:
        """Get stats for all limiters."""
        return {
            name: limiter.get_stats()
            for name, limiter in self.limiters.items()
        }


# Global rate limit manager
rate_limit_manager = RateLimitManager()

# Pre-configured limiters
ESPN_RATE_LIMITER = rate_limit_manager.get_limiter("espn", 8.0)
ODDS_API_RATE_LIMITER = rate_limit_manager.get_limiter("odds_api", 2.0)
OPENWEATHER_RATE_LIMITER = rate_limit_manager.get_limiter("openweather", 1.0)
SPORTSRADAR_RATE_LIMITER = rate_limit_manager.get_limiter("sportradar", 1.0)


# Example usage:
if __name__ == "__main__":
    import requests
    
    @rate_limited(calls_per_second=5.0)
    def fetch_data(url: str):
        return requests.get(url, timeout=10)
    
    # This will respect the rate limit
    for i in range(10):
        fetch_data("https://httpbin.org/get")
        print(f"Call {i+1} completed")