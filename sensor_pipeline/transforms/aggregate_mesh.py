"""Aggregate sensor readings by mesh network."""

import pandas as pd


class AggregateMesh:
    """Aggregate readings by mesh network."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate sensor readings by mesh_id.

        Args:
            df: DataFrame with processed sensor readings including is_healthy

        Returns:
            DataFrame with mesh-level aggregations
        """
        # Handle empty dataframe case
        if len(df) == 0:
            return pd.DataFrame(
                columns=[
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
            )

        # Group by mesh_id and aggregate
        grouped = (
            df.groupby("mesh_id")
            .agg(
                avg_temperature_c=("temperature_c", "mean"),
                avg_temperature_f=("temperature_f", "mean"),
                avg_humidity=("humidity", "mean"),
                total_readings=("mesh_id", "count"),
                temperature_anomaly_count=("temperature_alert", "sum"),
                humidity_anomaly_count=("humidity_alert", "sum"),
                status_anomaly_count=("status_alert", "sum"),
                healthy_reading_count=("is_healthy", "sum"),
            )
            .reset_index()
        )

        # Calculate healthy reading percentage
        grouped["healthy_reading_percentage"] = (
            grouped["healthy_reading_count"] / grouped["total_readings"] * 100
        ).round(1)

        # Drop the intermediate count column
        grouped = grouped.drop(columns=["healthy_reading_count"])

        return grouped
