"""Tests for error handling and rate limiting functionality."""

from __future__ import annotations

import time
import pytest
from urllib.error import HTTPError, URLError

from sports_analytics_pipeline.ingest import (
    IngestionError,
    APIError,
    DataValidationError,
    RateLimiter,
    retry_on_failure,
    DEFAULT_API_DELAY,
    MIN_API_DELAY,
    MAX_API_DELAY,
    BURST_LIMIT,
    BURST_COOLDOWN,
)


class TestCustomExceptions:
    """Test custom exception classes."""

    def test_ingestion_error_inheritance(self) -> None:
        """Test that custom exceptions inherit correctly."""
        error = IngestionError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_api_error_inheritance(self) -> None:
        """Test APIError inherits from IngestionError."""
        error = APIError("API failed")
        assert isinstance(error, IngestionError)
        assert isinstance(error, Exception)
        assert str(error) == "API failed"

    def test_data_validation_error_inheritance(self) -> None:
        """Test DataValidationError inherits from IngestionError."""
        error = DataValidationError("Invalid data")
        assert isinstance(error, IngestionError)
        assert isinstance(error, Exception)
        assert str(error) == "Invalid data"


class TestRateLimiter:
    """Test the RateLimiter class."""

    def test_rate_limiter_initialization(self) -> None:
        """Test RateLimiter initializes with correct defaults."""
        limiter = RateLimiter()
        assert limiter.base_delay == DEFAULT_API_DELAY
        assert limiter.burst_limit == BURST_LIMIT
        assert limiter.request_count == 0
        assert limiter.last_request_time == 0.0

    def test_rate_limiter_custom_params(self) -> None:
        """Test RateLimiter with custom parameters."""
        limiter = RateLimiter(base_delay=1.0, burst_limit=5)
        assert limiter.base_delay == 1.0
        assert limiter.burst_limit == 5

    def test_rate_limiter_first_call_no_wait(self) -> None:
        """Test that first call doesn't wait."""
        limiter = RateLimiter(base_delay=0.1)
        
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        
        # First call should not wait significantly
        assert elapsed < 0.05  # Allow some small processing time
        assert limiter.request_count == 1

    def test_rate_limiter_enforces_delay(self) -> None:
        """Test that rate limiter enforces delays between calls."""
        limiter = RateLimiter(base_delay=0.1)
        
        # First call
        limiter.wait_if_needed()
        
        # Second call should wait
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        
        # Should have waited approximately the base delay
        assert elapsed >= 0.09  # Allow some tolerance
        assert limiter.request_count == 2

    def test_rate_limiter_burst_protection(self) -> None:
        """Test burst protection with cooldown."""
        limiter = RateLimiter(base_delay=0.05, burst_limit=2)
        
        # Make burst_limit number of calls
        for i in range(2):
            limiter.wait_if_needed()
            time.sleep(0.01)  # Small delay to simulate processing
        
        # Next call should trigger burst cooldown
        assert limiter.request_count == 2
        
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        
        # Should have waited for burst cooldown
        assert elapsed >= BURST_COOLDOWN * 0.9  # Allow some tolerance
        assert limiter.request_count == 1  # Should reset after burst


class TestRetryDecorator:
    """Test the retry_on_failure decorator."""

    def test_retry_decorator_success_no_retry(self) -> None:
        """Test that successful functions don't retry."""
        call_count = 0
        
        @retry_on_failure(max_retries=3, delay=0.01)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_function()
        assert result == "success"
        assert call_count == 1

    def test_retry_decorator_http_error_retries(self) -> None:
        """Test that HTTP errors are retried."""
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise HTTPError(None, 500, "Server Error", None, None)
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_decorator_url_error_retries(self) -> None:
        """Test that URL errors are retried."""
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise URLError("Connection failed")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_decorator_max_retries_exceeded(self) -> None:
        """Test that max retries are respected and APIError is raised."""
        call_count = 0
        
        @retry_on_failure(max_retries=2, delay=0.01)
        def always_failing_function():
            nonlocal call_count
            call_count += 1
            raise HTTPError(None, 500, "Server Error", None, None)
        
        with pytest.raises(APIError) as exc_info:
            always_failing_function()
        
        assert call_count == 3  # Initial call + 2 retries
        assert "API request failed after 2 retries" in str(exc_info.value)

    def test_retry_decorator_non_retryable_error(self) -> None:
        """Test that non-retryable errors fail immediately."""
        call_count = 0
        
        @retry_on_failure(max_retries=3, delay=0.01)
        def non_retryable_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("This should not be retried")
        
        with pytest.raises(ValueError):
            non_retryable_error()
        
        assert call_count == 1  # Should not retry

    def test_retry_decorator_exponential_backoff(self) -> None:
        """Test exponential backoff timing."""
        call_times = []
        
        @retry_on_failure(max_retries=3, delay=0.1, backoff=2.0)
        def timing_function():
            call_times.append(time.time())
            raise HTTPError(None, 500, "Server Error", None, None)
        
        with pytest.raises(APIError):
            timing_function()
        
        # Should have made 4 calls (initial + 3 retries)
        assert len(call_times) == 4
        
        # Check that delays increase exponentially (approximately)
        delays = [call_times[i+1] - call_times[i] for i in range(len(call_times)-1)]
        
        # First delay should be around 0.1s
        assert 0.08 <= delays[0] <= 0.15
        # Second delay should be around 0.2s (0.1 * 2)
        assert 0.15 <= delays[1] <= 0.35
        # Third delay should be around 0.4s (0.2 * 2)
        assert 0.3 <= delays[2] <= 0.6


class TestConstants:
    """Test that constants are properly defined."""

    def test_api_delay_constants(self) -> None:
        """Test API delay constants are reasonable."""
        assert MIN_API_DELAY == 0.1
        assert DEFAULT_API_DELAY == 0.5
        assert MAX_API_DELAY == 5.0
        assert MIN_API_DELAY <= DEFAULT_API_DELAY <= MAX_API_DELAY

    def test_burst_constants(self) -> None:
        """Test burst protection constants are reasonable."""
        assert BURST_LIMIT == 100
        assert BURST_COOLDOWN == 1.0
        assert isinstance(BURST_LIMIT, int)
        assert isinstance(BURST_COOLDOWN, float)