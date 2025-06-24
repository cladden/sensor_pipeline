"""Tests for Prefect flow."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from sensor_pipeline_prefect.flow import load_to_df, run_core_pipeline, persist


class TestPrefectFlowTasks:
    """Test individual Prefect flow tasks."""

    @patch("sensor_pipeline_prefect.flow.FileSource")
    def test_load_to_df_file_source(self, mock_file_source: Mock) -> None:
        """Test loading data from file source."""
        # Mock the file source
        mock_source_instance = Mock()
        mock_source_instance.load.return_value = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )
        mock_file_source.return_value = mock_source_instance

        # Test file:// URL
        result = load_to_df("file:data/sensor_data.json")

        # Verify the source was created and called
        mock_file_source.assert_called_once_with("data/sensor_data.json")
        mock_source_instance.load.assert_called_once()

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch("sensor_pipeline_prefect.flow.ApiSource")
    def test_load_to_df_api_source(self, mock_api_source: Mock) -> None:
        """Test loading data from API source."""
        # Mock the API source
        mock_source_instance = Mock()
        mock_source_instance.load.return_value = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )
        mock_api_source.return_value = mock_source_instance

        # Test https:// URL
        result = load_to_df("https://api.example.com/sensors")

        # Verify the API source was created and called
        mock_api_source.assert_called_once_with("https://api.example.com/sensors")
        mock_source_instance.load.assert_called_once()

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_run_core_pipeline(self) -> None:
        """Test running the core pipeline."""
        from sensor_pipeline.models import PipelineConfig

        # Create test data
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )

        config = PipelineConfig()
        result = run_core_pipeline(input_data, config)

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "mesh_id" in result.columns

    def test_persist_task(self) -> None:
        """Test persisting DataFrame to JSON file."""
        # Create test data
        df = pd.DataFrame([{"mesh_id": "mesh-001", "avg_temperature_c": 22.4}])

        # Create temporary output directory
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "nested" / "dir" / "output.json"

            # Run the persist task
            persist(df, str(output_path))

            # Verify the nested directory was created
            assert output_path.parent.exists()
            assert output_path.exists()

            # Verify the content
            import json

            with open(output_path) as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["mesh_id"] == "mesh-001"
