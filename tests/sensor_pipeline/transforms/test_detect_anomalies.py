"""Tests for anomaly detection transform."""

import pandas as pd

from sensor_pipeline.models import PipelineConfig
from sensor_pipeline.transforms import DetectAnomalies


class TestDetectAnomalies:
    """Test anomaly detection transform."""

    def setup_method(self) -> None:
        """Set up test configuration."""
        self.config = PipelineConfig(
            temp_low=-10.0, temp_high=60.0, hum_low=10.0, hum_high=90.0
        )

    def test_temperature_alerts(self) -> None:
        """Test temperature-based anomaly detection."""
        df = pd.DataFrame(
            [
                {
                    "temperature_c": -15.0,  # Too cold
                    "humidity": 50.0,
                    "status": "ok",
                },
                {
                    "temperature_c": 70.0,  # Too hot
                    "humidity": 50.0,
                    "status": "ok",
                },
                {
                    "temperature_c": 25.0,  # Normal
                    "humidity": 50.0,
                    "status": "ok",
                },
            ]
        )

        transform = DetectAnomalies(self.config)
        result = transform.transform(df)

        # Check temperature alerts
        assert result["temperature_alert"].tolist() == [True, True, False]

        # Check other alerts are False
        assert result["humidity_alert"].tolist() == [False, False, False]
        assert result["status_alert"].tolist() == [False, False, False]

        # Check is_healthy (should be False for temperature alerts)
        assert result["is_healthy"].tolist() == [False, False, True]

    def test_humidity_alerts(self) -> None:
        """Test humidity-based anomaly detection."""
        df = pd.DataFrame(
            [
                {
                    "temperature_c": 25.0,
                    "humidity": 5.0,  # Too dry
                    "status": "ok",
                },
                {
                    "temperature_c": 25.0,
                    "humidity": 95.0,  # Too humid
                    "status": "ok",
                },
                {
                    "temperature_c": 25.0,
                    "humidity": 50.0,  # Normal
                    "status": "ok",
                },
            ]
        )

        transform = DetectAnomalies(self.config)
        result = transform.transform(df)

        # Check humidity alerts
        assert result["humidity_alert"].tolist() == [True, True, False]

        # Check other alerts are False
        assert result["temperature_alert"].tolist() == [False, False, False]
        assert result["status_alert"].tolist() == [False, False, False]

        # Check is_healthy (should be False for humidity alerts)
        assert result["is_healthy"].tolist() == [False, False, True]

    def test_status_based_alerts(self) -> None:
        """Test status-based anomaly detection."""
        df = pd.DataFrame(
            [
                {
                    "temperature_c": 25.0,
                    "humidity": 50.0,
                    "status": "error",  # Bad status
                },
                {
                    "temperature_c": 25.0,
                    "humidity": 50.0,
                    "status": "warning",  # Bad status
                },
                {
                    "temperature_c": 25.0,
                    "humidity": 50.0,
                    "status": "ok",  # Good status
                },
            ]
        )

        transform = DetectAnomalies(self.config)
        result = transform.transform(df)

        # Check status alerts
        assert result["status_alert"].tolist() == [True, True, False]

        # Check other alerts are False
        assert result["temperature_alert"].tolist() == [False, False, False]
        assert result["humidity_alert"].tolist() == [False, False, False]

        # Check is_healthy (should be False for status alerts)
        assert result["is_healthy"].tolist() == [False, False, True]

    def test_multiple_alerts_composite_health(self) -> None:
        """Test that is_healthy correctly reflects composite health."""
        df = pd.DataFrame(
            [
                {
                    "temperature_c": -15.0,  # Temperature alert
                    "humidity": 5.0,  # Humidity alert
                    "status": "error",  # Status alert
                },
                {
                    "temperature_c": 70.0,  # Temperature alert only
                    "humidity": 50.0,  # Normal
                    "status": "ok",  # Normal
                },
                {
                    "temperature_c": 25.0,  # All normal
                    "humidity": 50.0,  # All normal
                    "status": "ok",  # All normal
                },
            ]
        )

        transform = DetectAnomalies(self.config)
        result = transform.transform(df)

        # Check individual alerts
        assert result["temperature_alert"].tolist() == [True, True, False]
        assert result["humidity_alert"].tolist() == [True, False, False]
        assert result["status_alert"].tolist() == [True, False, False]

        # Check is_healthy: only healthy if NO alerts
        assert result["is_healthy"].tolist() == [False, False, True]

    def test_default_thresholds(self) -> None:
        """Test default threshold values."""
        df = pd.DataFrame(
            {
                "temperature_c": [25.0],
                "humidity": [50.0],
                "status": ["ok"],
            }
        )

        config = PipelineConfig()  # Use defaults
        transform = DetectAnomalies(config)
        result = transform.transform(df)

        # Should not trigger alerts with normal values
        assert not result["temperature_alert"].iloc[0]
        assert not result["humidity_alert"].iloc[0]
        assert not result["status_alert"].iloc[0]
