"""Tests for anomaly detection transform."""

import pandas as pd

from sensor_pipeline.models import PipelineConfig
from sensor_pipeline.transforms import DetectAnomalies


class TestDetectAnomalies:
    """Test anomaly detection transform."""

    def test_temperature_alerts(self) -> None:
        """Test temperature anomaly detection."""
        df = pd.DataFrame(
            {
                "temperature_c": [-15.0, 75.0, 25.0, None],
                "humidity": [50.0, 50.0, 50.0, 50.0],
                "status": ["ok", "ok", "ok", "error"],
            }
        )

        config = PipelineConfig(
            temp_low=-10.0, temp_high=60.0, hum_low=20.0, hum_high=80.0
        )
        transform = DetectAnomalies(config)
        result = transform.transform(df)

        assert result["temperature_alert"].iloc[0]  # Too cold
        assert result["temperature_alert"].iloc[1]  # Too hot
        assert not result["temperature_alert"].iloc[2]  # Normal
        assert result["temperature_alert"].iloc[3]  # Bad status

    def test_humidity_alerts(self) -> None:
        """Test humidity anomaly detection."""
        df = pd.DataFrame(
            {
                "temperature_c": [25.0, 25.0, 25.0],
                "humidity": [10.0, 90.0, 50.0],
                "status": ["ok", "ok", "ok"],
            }
        )

        config = PipelineConfig(
            temp_low=-10.0, temp_high=60.0, hum_low=20.0, hum_high=80.0
        )
        transform = DetectAnomalies(config)
        result = transform.transform(df)

        assert result["humidity_alert"].iloc[0]  # Too dry
        assert result["humidity_alert"].iloc[1]  # Too humid
        assert not result["humidity_alert"].iloc[2]  # Normal

    def test_status_based_alerts(self) -> None:
        """Test alerts based on device status."""
        df = pd.DataFrame(
            {
                "temperature_c": [25.0, 25.0, 25.0],
                "humidity": [50.0, 50.0, 50.0],
                "status": ["ok", "error", "warning"],
            }
        )

        config = PipelineConfig(
            temp_low=-10.0, temp_high=60.0, hum_low=20.0, hum_high=80.0
        )
        transform = DetectAnomalies(config)
        result = transform.transform(df)

        # Status != "ok" should trigger both alerts
        assert not result["temperature_alert"].iloc[0]
        assert result["temperature_alert"].iloc[1]
        assert result["temperature_alert"].iloc[2]

        assert not result["humidity_alert"].iloc[0]
        assert result["humidity_alert"].iloc[1]
        assert result["humidity_alert"].iloc[2]

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
