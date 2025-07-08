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
                    "temperature_alert": False,
                    "humidity_alert": False,
                    "status_alert": False,
                    "is_healthy": True,
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
                    "status_alert": False,
                    "is_healthy": True,
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
                    "status_alert": False,
                    "is_healthy": True,
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Check basic structure
        assert len(result) == 2
        assert set(result["mesh_id"]) == {"mesh-001", "mesh-002"}

        # Check mesh-001 aggregation
        mesh_001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh_001["avg_temperature_c"] == 23.0
        assert mesh_001["avg_temperature_f"] == 73.4
        assert mesh_001["avg_humidity"] == 42.5
        assert mesh_001["total_readings"] == 2
        assert mesh_001["temperature_anomaly_count"] == 0
        assert mesh_001["humidity_anomaly_count"] == 0
        assert mesh_001["status_anomaly_count"] == 0
        assert mesh_001["healthy_reading_percentage"] == 100.0

        # Check mesh-002 aggregation
        mesh_002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert mesh_002["avg_temperature_c"] == 20.0
        assert mesh_002["avg_temperature_f"] == 68.0
        assert mesh_002["avg_humidity"] == 35.0
        assert mesh_002["total_readings"] == 1
        assert mesh_002["temperature_anomaly_count"] == 0
        assert mesh_002["humidity_anomaly_count"] == 0
        assert mesh_002["status_anomaly_count"] == 0
        assert mesh_002["healthy_reading_percentage"] == 100.0

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
                    "status_alert": False,
                    "is_healthy": False,  # Not healthy due to temp alert
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
                    "status_alert": True,  # Status alert
                    "is_healthy": False,  # Not healthy due to multiple alerts
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
                    "status_alert": False,
                    "is_healthy": True,  # Healthy reading
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Check mesh-001 aggregation (has alerts)
        mesh_001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh_001["total_readings"] == 2
        assert mesh_001["temperature_anomaly_count"] == 1
        assert mesh_001["humidity_anomaly_count"] == 1
        assert mesh_001["status_anomaly_count"] == 1
        assert mesh_001["healthy_reading_percentage"] == 0.0  # 0/2 healthy

        # Check mesh-002 aggregation (no alerts)
        mesh_002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert mesh_002["total_readings"] == 1
        assert mesh_002["temperature_anomaly_count"] == 0
        assert mesh_002["humidity_anomaly_count"] == 0
        assert mesh_002["status_anomaly_count"] == 0
        assert mesh_002["healthy_reading_percentage"] == 100.0  # 1/1 healthy

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
                "temperature_alert",
                "humidity_alert",
                "status_alert",
                "is_healthy",
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Should return empty DataFrame with proper columns
        assert len(result) == 0
        expected_columns = [
            "mesh_id",
            "avg_temperature_c",
            "avg_temperature_f",
            "avg_humidity",
            "total_readings",
            "temperature_anomaly_count",
            "humidity_anomaly_count",
            "status_anomaly_count",
            "healthy_reading_percentage",
        ]
        assert list(result.columns) == expected_columns

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
                    "status_alert": True,
                    "is_healthy": False,  # Not healthy due to alerts
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Check single mesh result
        assert len(result) == 1
        mesh_001 = result.iloc[0]
        assert mesh_001["mesh_id"] == "mesh-001"
        assert mesh_001["avg_temperature_c"] == 22.5
        assert mesh_001["avg_temperature_f"] == 72.5
        assert mesh_001["avg_humidity"] == 42.0
        assert mesh_001["total_readings"] == 1
        assert mesh_001["temperature_anomaly_count"] == 1
        assert mesh_001["humidity_anomaly_count"] == 0
        assert mesh_001["status_anomaly_count"] == 1
        assert mesh_001["healthy_reading_percentage"] == 0.0

    def test_healthy_percentage_calculation(self) -> None:
        """Test healthy reading percentage calculation."""
        input_data = pd.DataFrame(
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
                    "status_alert": False,  # Healthy reading
                    "is_healthy": True,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 24.0,
                    "temperature_f": 75.2,
                    "humidity": 45.0,
                    "temperature_alert": True,  # Has alert
                    "humidity_alert": False,
                    "status_alert": False,
                    "is_healthy": False,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 20.0,
                    "temperature_f": 68.0,
                    "humidity": 35.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                    "status_alert": False,  # Healthy reading
                    "is_healthy": True,
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-D",
                    "timestamp": "2025-03-26T13:48:00Z",
                    "temperature_c": 25.0,
                    "temperature_f": 77.0,
                    "humidity": 50.0,
                    "temperature_alert": False,
                    "humidity_alert": False,
                    "status_alert": False,  # Healthy reading
                    "is_healthy": True,
                },
            ]
        )

        transform = AggregateMesh()
        result = transform.transform(input_data)

        # Check healthy percentage calculation
        mesh_001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh_001["total_readings"] == 4
        assert mesh_001["healthy_reading_percentage"] == 75.0  # 3/4 healthy
