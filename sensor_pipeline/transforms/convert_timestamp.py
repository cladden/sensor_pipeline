"""Convert timestamps from UTC to EST."""

import pandas as pd
from zoneinfo import ZoneInfo


class ConvertTimestamp:
    """Convert UTC timestamps to Eastern Standard Time."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert timestamp column from UTC to EST.

        Args:
            df: DataFrame with 'timestamp' column in UTC

        Returns:
            DataFrame with additional 'timestamp_est' column
        """
        # Ensure timestamp is datetime and UTC-aware
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            # Clean up malformed timestamps that have both +00:00 and Z
            df["timestamp"] = df["timestamp"].str.replace(r"\+00:00Z$", "Z", regex=True)
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")

        # Make UTC-aware if not already
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

        # Convert to EST
        est_tz = ZoneInfo("Etc/GMT+5")  # constant five-hour west offset
        df["timestamp_est"] = df["timestamp"].dt.tz_convert(est_tz)

        return df
