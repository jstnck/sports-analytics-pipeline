"""Test DLT error handling and recovery mechanisms."""

from __future__ import annotations

import pytest
from unittest.mock import Mock

from dlt.pipeline.exceptions import PipelineStepFailed

from sports_analytics_pipeline.ingest import _run_pipeline_with_recovery


class TestDLTErrorHandling:
    """Test DLT pipeline error handling and recovery."""

    def test_run_pipeline_with_recovery_success(self):
        """Test that normal pipeline execution works without recovery."""
        # Create a mock pipeline that succeeds on first try
        mock_pipeline = Mock()
        mock_pipeline.run.return_value = Mock(loads_ids=["test_load_id"])
        
        mock_resources = [Mock()]
        
        result = _run_pipeline_with_recovery(mock_pipeline, mock_resources)
        
        # Should succeed on first attempt
        assert mock_pipeline.run.call_count == 1
        assert result.loads_ids == ["test_load_id"]
        
    def test_run_pipeline_with_recovery_retries_on_failure(self):
        """Test that recovery retries on failure."""
        mock_pipeline = Mock()
        
        # First call fails, second succeeds
        failure_error = Exception("Temporary failure")
        mock_pipeline.run.side_effect = [failure_error, Mock(loads_ids=["recovered_load_id"])]
        
        mock_resources = [Mock()]
        
        result = _run_pipeline_with_recovery(mock_pipeline, mock_resources)
        
        # Should retry and succeed
        assert mock_pipeline.run.call_count == 2
        assert result.loads_ids == ["recovered_load_id"]
        
    def test_run_pipeline_with_recovery_gives_up_after_max_attempts(self):
        """Test that recovery gives up after max retry attempts."""
        mock_pipeline = Mock()
        
        # Always fails
        failure_error = Exception("Persistent failure")
        mock_pipeline.run.side_effect = failure_error
        
        mock_resources = [Mock()]
        
        with pytest.raises(Exception) as exc_info:
            _run_pipeline_with_recovery(mock_pipeline, mock_resources, max_attempts=2)
            
        # Should try twice and give up
        assert mock_pipeline.run.call_count == 2
        assert str(exc_info.value) == "Persistent failure"
        
    def test_run_pipeline_with_recovery_handles_pipeline_step_failed(self):
        """Test that PipelineStepFailed exceptions are handled correctly."""
        mock_pipeline = Mock()
        
        # First call fails with PipelineStepFailed, second succeeds
        step_error = PipelineStepFailed(
            mock_pipeline, "load", "test_load_id", 
            Exception("Step failed"), 
            None
        )
        mock_pipeline.run.side_effect = [step_error, Mock(loads_ids=["recovered_load_id"])]
        
        mock_resources = [Mock()]
        
        result = _run_pipeline_with_recovery(mock_pipeline, mock_resources)
        
        # Should retry and succeed
        assert mock_pipeline.run.call_count == 2
        assert result.loads_ids == ["recovered_load_id"]
        
    def test_run_pipeline_with_recovery_custom_max_attempts(self):
        """Test that custom max_attempts parameter works."""
        mock_pipeline = Mock()
        
        # Always fails
        failure_error = Exception("Custom attempts test")
        mock_pipeline.run.side_effect = failure_error
        
        mock_resources = [Mock()]
        
        with pytest.raises(Exception):
            _run_pipeline_with_recovery(mock_pipeline, mock_resources, max_attempts=3)
            
        # Should try 3 times as specified
        assert mock_pipeline.run.call_count == 3