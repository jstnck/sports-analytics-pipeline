"""Tests for error handling and rate limiting functionality."""

from __future__ import annotations

import time
from urllib.error import HTTPError
from urllib.request import Request

from sports_analytics_pipeline.ingest import (
    IngestionError,
    RateLimiter,
    retry_on_failure,
    DEFAULT_API_DELAY,
    BURST_LIMIT,
)


class TestErrorHandling:
    """Test core error handling functionality."""

    def test_ingestion_error_basic(self) -> None:
        """Test basic IngestionError functionality."""
        error = IngestionError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_retry_decorator_success(self) -> None:
        """Test retry decorator with successful function."""
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def successful_function() -> str:
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_function()
        assert result == "success"
        assert call_count == 1

    def test_retry_decorator_with_http_error(self) -> None:
        """Test retry decorator handles HTTP errors."""
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def failing_function() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                # Create a minimal HTTPError for testing
                req = Request("http://test.com")
                raise HTTPError(req.full_url, 500, "Server Error", {}, None)  # type: ignore[arg-type]
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count == 2


class TestRateLimiter:
    """Test rate limiting functionality."""

    def test_rate_limiter_initialization(self) -> None:
        """Test RateLimiter initializes correctly."""
        limiter = RateLimiter()
        assert limiter.base_delay == DEFAULT_API_DELAY
        assert limiter.burst_limit == BURST_LIMIT
        assert limiter.request_count == 0

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