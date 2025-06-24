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
        """Add anomaly alert columns.

        Args:
            df: DataFrame with temperature_c and humidity columns

        Returns:
            DataFrame with temperature_alert and humidity_alert columns
        """
        df = df.copy()

        # Temperature alerts
        df["temperature_alert"] = (
            (df["temperature_c"] < self.config.temp_low)
            | (df["temperature_c"] > self.config.temp_high)
            | (df["status"] != "ok")
        )

        # Humidity alerts
        df["humidity_alert"] = (
            (df["humidity"] < self.config.hum_low)
            | (df["humidity"] > self.config.hum_high)
            | (df["status"] != "ok")
        )

        return df
