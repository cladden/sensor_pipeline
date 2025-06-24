"""Aggregate sensor readings by mesh network."""

import pandas as pd

from ..models import PipelineConfig


class AggregateMesh:
    """Aggregate readings by mesh network."""

    def __init__(self, config: PipelineConfig):
        """Initialize with threshold configuration.

        Args:
            config: Pipeline configuration with thresholds
        """
        self.config = config

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group by mesh_id and compute aggregates.

        Args:
            df: DataFrame with processed sensor readings

        Returns:
            DataFrame with one row per mesh_id containing aggregated metrics
        """
        # Group by mesh_id and aggregate
        agg_dict = {
            "temperature_c": "mean",
            "temperature_f": "mean",
            "humidity": "mean",
            "mesh_id": "count",  # Count for total_readings
        }

        # Include alert columns if they exist
        if "temperature_alert" in df.columns:
            agg_dict["temperature_alert"] = "any"
        if "humidity_alert" in df.columns:
            agg_dict["humidity_alert"] = "any"

        agg_df = df.groupby("mesh_id").agg(agg_dict).round(2)

        # Rename count column
        agg_df = agg_df.rename(columns={"mesh_id": "total_readings"})

        # Reset index to make mesh_id a column
        agg_df = agg_df.reset_index()

        # Rename columns to match expected output
        agg_df = agg_df.rename(
            columns={
                "temperature_c": "avg_temperature_c",
                "temperature_f": "avg_temperature_f",
                "humidity": "avg_humidity",
            }
        )

        # Add alert columns based on averages OR individual alerts
        if "temperature_alert" in agg_df.columns:
            # Combine individual alerts with average-based alerts
            agg_df["temperature_alert"] = (
                agg_df["temperature_alert"]  # Any individual alert
                | (agg_df["avg_temperature_c"] < self.config.temp_low)
                | (agg_df["avg_temperature_c"] > self.config.temp_high)
            )
        else:
            # Only average-based alerts
            agg_df["temperature_alert"] = (
                agg_df["avg_temperature_c"] < self.config.temp_low
            ) | (agg_df["avg_temperature_c"] > self.config.temp_high)

        if "humidity_alert" in agg_df.columns:
            # Combine individual alerts with average-based alerts
            agg_df["humidity_alert"] = (
                agg_df["humidity_alert"]  # Any individual alert
                | (agg_df["avg_humidity"] < self.config.hum_low)
                | (agg_df["avg_humidity"] > self.config.hum_high)
            )
        else:
            # Only average-based alerts
            agg_df["humidity_alert"] = (
                agg_df["avg_humidity"] < self.config.hum_low
            ) | (agg_df["avg_humidity"] > self.config.hum_high)

        return agg_df
