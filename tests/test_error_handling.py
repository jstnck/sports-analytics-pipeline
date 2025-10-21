"""Tests for error handling and rate limiting functionality."""

from __future__ import annotations

import time

from sports_analytics_pipeline.ingest import (
    IngestionError,
    RateLimiter,
    DEFAULT_API_DELAY,
)


class TestErrorHandling:
    """Test core error handling functionality."""

    def test_ingestion_error_basic(self) -> None:
        """Test basic IngestionError functionality."""
        error = IngestionError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"


class TestRateLimiter:
    """Test rate limiting functionality."""

    def test_rate_limiter_initialization(self) -> None:
        """Test RateLimiter initializes correctly."""
        limiter = RateLimiter()
        assert limiter.base_delay == DEFAULT_API_DELAY

    def test_rate_limiter_basic_delay(self) -> None:
        """Test rate limiter enforces basic delays."""
        limiter = RateLimiter(base_delay=0.1)

        # First call should not wait
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        assert elapsed < 0.05

        # Second call should wait
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        assert elapsed >= 0.09
