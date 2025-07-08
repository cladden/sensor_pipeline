"""Detect anomalies in sensor readings."""

import pandas as pd

from ..models import PipelineConfig


class DetectAnomalies:
    """Detect temperature and humidity anomalies."""

    def __init__(self, config: PipelineConfig):
        """Initialize with threshold configuration.

        Args:
            config: Pipeline configuration with thresholds
        """
        self.config = config

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies in sensor readings.

        Args:
            df: DataFrame with sensor readings

        Returns:
            DataFrame with anomaly alert columns added
        """
        result = df.copy()

        # Temperature anomalies
        result["temperature_alert"] = (
            result["temperature_c"] < self.config.temp_low
        ) | (result["temperature_c"] > self.config.temp_high)

        # Humidity anomalies
        result["humidity_alert"] = (result["humidity"] < self.config.hum_low) | (
            result["humidity"] > self.config.hum_high
        )

        # Status anomalies
        result["status_alert"] = result["status"] != "ok"

        # Composite health indicator: healthy if NO alerts
        result["is_healthy"] = (
            (~result["temperature_alert"])
            & (~result["humidity_alert"])
            & (~result["status_alert"])
        )

        return result
