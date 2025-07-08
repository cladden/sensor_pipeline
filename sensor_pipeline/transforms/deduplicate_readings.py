"""Remove duplicate sensor readings."""

import pandas as pd


class DeduplicateReadings:
    """Remove duplicate sensor readings based on mesh_id, device_id,
    and timestamp."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove exact duplicates from sensor readings.

        Args:
            df: DataFrame with sensor readings

        Returns:
            DataFrame with duplicates removed, keeping first occurrence
        """
        # Remove exact duplicates and ensure we have our own copy
        return df.drop_duplicates(
            subset=["mesh_id", "device_id", "timestamp"], keep="first"
        ).copy()
