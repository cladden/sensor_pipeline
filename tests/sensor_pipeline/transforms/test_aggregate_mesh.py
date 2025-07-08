"""Tests for aggregate mesh transform."""

import pandas as pd

from sensor_pipeline.transforms.aggregate_mesh import AggregateMesh


class TestAggregateMesh:
    """Test mesh aggregation functionality."""

    def test_basic_aggregation(self) -> None:
        """Test basic mesh aggregation without alerts."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.0,
                    "temperature_f": 71.6,
                    "humidity": 40.0,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 24.0,
                    "temperature_f": 75.2,
                    "humidity": 45.0,
                },
                {
                    "mesh_id": "mesh-002",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 20.0,
                    "temperature_f": 68.0,
                    "humidity": 35.0,
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Should have 2 mesh summaries
        assert len(result) == 2
        assert set(result["mesh_id"]) == {"mesh-001", "mesh-002"}

        # Check mesh-001 aggregation
        mesh001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh001["avg_temperature_c"] == 23.0  # (22 + 24) / 2
        assert mesh001["avg_temperature_f"] == 73.4  # (71.6 + 75.2) / 2
        assert mesh001["avg_humidity"] == 42.5  # (40 + 45) / 2
        assert mesh001["total_readings"] == 2

        # Check mesh-002 aggregation
        mesh002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert mesh002["avg_temperature_c"] == 20.0
        assert mesh002["avg_temperature_f"] == 68.0
        assert mesh002["avg_humidity"] == 35.0
        assert mesh002["total_readings"] == 1

    def test_alert_aggregation(self) -> None:
        """Test aggregation with alert columns."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.0,
                    "temperature_f": 71.6,
                    "humidity": 40.0,
                    "temperature_alert": True,  # One alert
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
                    "humidity_alert": True,  # Different alert
                },
                {
                    "mesh_id": "mesh-002",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 20.0,
                    "temperature_f": 68.0,
                    "humidity": 35.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Check alert aggregation (any True = True)
        mesh001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh001["temperature_alert"]  # One reading had alert
        assert mesh001["humidity_alert"]  # One reading had alert

        mesh002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert not mesh002["temperature_alert"]  # No alerts
        assert not mesh002["humidity_alert"]  # No alerts

    def test_empty_dataframe(self) -> None:
        """Test aggregation with empty DataFrame."""
        input_data = pd.DataFrame(
            columns=[
                "mesh_id",
                "device_id",
                "timestamp",
                "temperature_c",
                "temperature_f",
                "humidity",
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        assert len(result) == 0
        # Should still have expected columns
        expected_columns = [
            "mesh_id",
            "avg_temperature_c",
            "avg_temperature_f",
            "avg_humidity",
            "total_readings",
            "temperature_alert",
            "humidity_alert",
        ]
        for col in expected_columns:
            assert col in result.columns

    def test_single_reading_per_mesh(self) -> None:
        """Test aggregation with single reading per mesh."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.5,
                    "temperature_f": 72.5,
                    "humidity": 42.0,
                    "temperature_alert": True,
                    "humidity_alert": False,
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        assert len(result) == 1
        mesh001 = result.iloc[0]
        assert mesh001["mesh_id"] == "mesh-001"
        assert mesh001["avg_temperature_c"] == 22.5
        assert mesh001["avg_temperature_f"] == 72.5
        assert mesh001["avg_humidity"] == 42.0
        assert mesh001["total_readings"] == 1
        assert mesh001["temperature_alert"]
        assert not mesh001["humidity_alert"]
