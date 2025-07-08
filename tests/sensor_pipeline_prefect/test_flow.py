"""Tests for Prefect flow."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from sensor_pipeline_prefect.flow import (
    load_to_df,
    run_core_pipeline,
    persist,
    validate_sensor_input,
    convert_timestamp,
    convert_temperature,
    detect_anomalies,
    aggregate_mesh,
)


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

        # Test file:// URL - call the function directly to avoid Prefect runtime
        result = load_to_df.fn("file:data/sensor_data.json")

        # Verify the source was created and called
        mock_file_source.assert_called_once_with("data/sensor_data.json")
        mock_source_instance.load.assert_called_once()

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_load_to_df_unsupported_scheme(self) -> None:
        """Test loading data from unsupported source scheme."""
        with pytest.raises(ValueError, match="Unsupported source scheme: https"):
            load_to_df.fn("https://api.example.com/sensors")

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
        result = run_core_pipeline.fn(input_data, config)

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

            # Run the persist task - call function directly
            persist.fn(df, str(output_path))

            # Verify the nested directory was created
            assert output_path.parent.exists()
            assert output_path.exists()

            # Verify the content
            import json

            with open(output_path) as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["mesh_id"] == "mesh-001"

    def test_validate_sensor_input_task(self) -> None:
        """Test sensor input validation task."""
        # Create valid test data
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

        result = validate_sensor_input.fn(input_data)

        # Should return the same DataFrame if valid
        pd.testing.assert_frame_equal(result, input_data)

    def test_convert_timestamp_task(self) -> None:
        """Test timestamp conversion task."""
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

        result = convert_timestamp.fn(input_data)

        # Should add timestamp_est column
        assert "timestamp_est" in result.columns
        assert len(result) == 1

    def test_convert_temperature_task(self) -> None:
        """Test temperature conversion task."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 0.0,
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )

        result = convert_temperature.fn(input_data)

        # Should add temperature_f column
        assert "temperature_f" in result.columns
        assert result["temperature_f"].iloc[0] == 32.0

    def test_detect_anomalies_task(self) -> None:
        """Test anomaly detection task."""
        from sensor_pipeline.models import PipelineConfig

        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": -15.0,  # Should trigger alert
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )

        config = PipelineConfig()
        result = detect_anomalies.fn(input_data, config)

        # Should add alert columns
        assert "temperature_alert" in result.columns
        assert "humidity_alert" in result.columns
        assert result["temperature_alert"].iloc[0]  # Should trigger alert

    def test_aggregate_mesh_task(self) -> None:
        """Test mesh aggregation task."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.0,
                    "temperature_f": 71.6,
                    "humidity": 41.2,
                    "status": "ok",
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 24.0,
                    "temperature_f": 75.2,
                    "humidity": 42.8,
                    "status": "ok",
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
            ]
        )

        result = aggregate_mesh.fn(input_data)

        # Should aggregate into single mesh summary
        assert len(result) == 1
        assert result["mesh_id"].iloc[0] == "mesh-001"
        assert result["total_readings"].iloc[0] == 2
        assert result["avg_temperature_c"].iloc[0] == 23.0  # (22 + 24) / 2
