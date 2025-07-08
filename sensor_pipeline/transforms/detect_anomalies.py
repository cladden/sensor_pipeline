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

        # Temperature anomalies
        df["temperature_alert"] = (df["temperature_c"] < self.config.temp_low) | (
            df["temperature_c"] > self.config.temp_high
        )

        # Humidity anomalies
        df["humidity_alert"] = (df["humidity"] < self.config.hum_low) | (
            df["humidity"] > self.config.hum_high
        )

        # Status anomalies
        df["status_alert"] = df["status"] != "ok"

        # Composite health indicator: healthy if NO alerts
        df["is_healthy"] = (
            (~df["temperature_alert"]) & (~df["humidity_alert"]) & (~df["status_alert"])
        )

        return df
