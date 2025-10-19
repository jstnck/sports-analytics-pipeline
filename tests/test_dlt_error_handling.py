"""Test DLT error handling and recovery mechanisms."""

from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from sports_analytics_pipeline.ingest import _run_pipeline_with_recovery, get_dlt_destination


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
        
    def test_run_pipeline_with_recovery_handles_table_not_exist_error(self):
        """Test that recovery handles table truncation errors."""
        mock_pipeline = Mock()
        
        # First call fails with table not exist error, second succeeds
        table_error = PipelineStepFailed(
            mock_pipeline, "load", "test_load_id", 
            DatabaseUndefinedRelation("Table with name rosters__coach does not exist! DELETE FROM"), 
            None
        )
        mock_pipeline.run.side_effect = [table_error, Mock(loads_ids=["recovered_load_id"])]
        
        mock_resources = [Mock()]
        
        result = _run_pipeline_with_recovery(mock_pipeline, mock_resources)
        
        # Should call recovery methods and retry
        assert mock_pipeline.run.call_count == 2
        assert mock_pipeline.drop_pending_packages.call_count == 1
        assert mock_pipeline.sync_destination.call_count == 1
        assert result.loads_ids == ["recovered_load_id"]
        
    def test_run_pipeline_with_recovery_gives_up_after_max_attempts(self):
        """Test that recovery gives up after max retry attempts."""
        mock_pipeline = Mock()
        
        # Always fails with table not exist error
        table_error = PipelineStepFailed(
            mock_pipeline, "load", "test_load_id", 
            DatabaseUndefinedRelation("Table with name rosters__coach does not exist! DELETE FROM"), 
            None
        )
        mock_pipeline.run.side_effect = table_error
        
        mock_resources = [Mock()]
        
        with pytest.raises(PipelineStepFailed):
            _run_pipeline_with_recovery(mock_pipeline, mock_resources, max_attempts=2)
            
        # Should try twice and give up
        assert mock_pipeline.run.call_count == 2
        assert mock_pipeline.drop_pending_packages.call_count == 1
        
    def test_run_pipeline_with_recovery_reraises_non_recoverable_errors(self):
        """Test that non-recoverable errors are re-raised immediately."""
        mock_pipeline = Mock()
        
        # Fail with a different type of error
        other_error = PipelineStepFailed(
            mock_pipeline, "normalize", "test_load_id", 
            ValueError("Some other error"), 
            None
        )
        mock_pipeline.run.side_effect = other_error
        
        mock_resources = [Mock()]
        
        with pytest.raises(PipelineStepFailed):
            _run_pipeline_with_recovery(mock_pipeline, mock_resources)
            
        # Should not retry for non-recoverable errors
        assert mock_pipeline.run.call_count == 1
        assert mock_pipeline.drop_pending_packages.call_count == 0
        
    def test_run_pipeline_with_recovery_handles_recovery_method_failures(self):
        """Test that recovery continues even if recovery methods fail."""
        mock_pipeline = Mock()
        
        # First call fails, recovery methods fail, second call succeeds
        table_error = PipelineStepFailed(
            mock_pipeline, "load", "test_load_id", 
            DatabaseUndefinedRelation("Table with name test does not exist! DELETE FROM"), 
            None
        )
        mock_pipeline.run.side_effect = [table_error, Mock(loads_ids=["recovered_load_id"])]
        mock_pipeline.drop_pending_packages.side_effect = Exception("Recovery failed")
        mock_pipeline.sync_destination.side_effect = Exception("Sync failed")
        
        mock_resources = [Mock()]
        
        result = _run_pipeline_with_recovery(mock_pipeline, mock_resources)
        
        # Should still retry even though recovery methods failed
        assert mock_pipeline.run.call_count == 2
        assert result.loads_ids == ["recovered_load_id"]