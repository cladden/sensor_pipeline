"""Tests for mesh aggregation transform."""

import pandas as pd

from sensor_pipeline.models import PipelineConfig
from sensor_pipeline.transforms import AggregateMesh


class TestAggregateMesh:
    """Test mesh aggregation transform."""

    def test_basic_aggregation(self) -> None:
        """Test basic mesh aggregation functionality."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.0,
                    "temperature_f": 71.6,
                    "humidity": 40.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 24.0,
                    "temperature_f": 75.2,
                    "humidity": 45.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
                {
                    "mesh_id": "mesh-002",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 30.0,
                    "temperature_f": 86.0,
                    "humidity": 60.0,
                    "temperature_alert": True,
                    "humidity_alert": False,
                },
            ]
        )

        config = PipelineConfig()
        transform = AggregateMesh(config)
        result = transform.transform(df)

        # Should have 2 mesh summaries
        assert len(result) == 2
        assert set(result["mesh_id"]) == {"mesh-001", "mesh-002"}

        # Check mesh-001 aggregation
        mesh001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh001["total_readings"] == 2
        assert mesh001["avg_temperature_c"] == 23.0  # (22 + 24) / 2
        assert mesh001["avg_temperature_f"] == 73.4  # (71.6 + 75.2) / 2
        assert mesh001["avg_humidity"] == 42.5  # (40 + 45) / 2
        assert not mesh001["temperature_alert"]  # No alerts
        assert not mesh001["humidity_alert"]

        # Check mesh-002 aggregation
        mesh002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert mesh002["total_readings"] == 1
        assert mesh002["avg_temperature_c"] == 30.0
        assert mesh002["avg_temperature_f"] == 86.0
        assert mesh002["avg_humidity"] == 60.0
        assert mesh002["temperature_alert"]  # Has alert
        assert not mesh002["humidity_alert"]

    def test_alert_aggregation(self) -> None:
        """Test that alerts are properly aggregated (any True = True)."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-hot",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 70.0,
                    "temperature_f": 158.0,
                    "humidity": 50.0,
                    "temperature_alert": True,
                    "humidity_alert": False,
                },
                {
                    "mesh_id": "mesh-dry",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 25.0,
                    "temperature_f": 77.0,
                    "humidity": 5.0,
                    "temperature_alert": False,
                    "humidity_alert": True,
                },
                {
                    "mesh_id": "mesh-ok",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 22.0,
                    "temperature_f": 71.6,
                    "humidity": 45.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
            ]
        )

        config = PipelineConfig()
        transform = AggregateMesh(config)
        result = transform.transform(df)

        hot_mesh = result[result["mesh_id"] == "mesh-hot"].iloc[0]
        dry_mesh = result[result["mesh_id"] == "mesh-dry"].iloc[0]
        ok_mesh = result[result["mesh_id"] == "mesh-ok"].iloc[0]

        assert hot_mesh["temperature_alert"]
        assert dry_mesh["humidity_alert"]
        assert not ok_mesh["temperature_alert"]
        assert not ok_mesh["humidity_alert"]

    def test_empty_dataframe(self) -> None:
        """Test handling of empty input."""
        df = pd.DataFrame(
            columns=[
                "mesh_id",
                "device_id",
                "timestamp",
                "temperature_c",
                "temperature_f",
                "humidity",
                "temperature_alert",
                "humidity_alert",
            ]
        )

        config = PipelineConfig()
        transform = AggregateMesh(config)
        result = transform.transform(df)

        assert len(result) == 0
        assert list(result.columns) == [
            "mesh_id",
            "avg_temperature_c",
            "avg_temperature_f",
            "avg_humidity",
            "total_readings",
            "temperature_alert",
            "humidity_alert",
        ]

    def test_single_reading_per_mesh(self) -> None:
        """Test aggregation with single reading per mesh."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 25.0,
                    "temperature_f": 77.0,
                    "humidity": 50.0,
                    "temperature_alert": False,
                    "humidity_alert": True,
                }
            ]
        )

        config = PipelineConfig()
        transform = AggregateMesh(config)
        result = transform.transform(df)

        assert len(result) == 1
        mesh = result.iloc[0]
        assert mesh["mesh_id"] == "mesh-001"
        assert mesh["total_readings"] == 1
        assert mesh["avg_temperature_c"] == 25.0
        assert mesh["avg_temperature_f"] == 77.0
        assert mesh["avg_humidity"] == 50.0
        assert not mesh["temperature_alert"]
        assert mesh["humidity_alert"]
