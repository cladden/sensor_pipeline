"""Convert temperature from Celsius to Fahrenheit."""

import pandas as pd


class ConvertTemperature:
    """Convert temperature from Celsius to Fahrenheit."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add temperature_f column with Fahrenheit conversion.

        Args:
            df: DataFrame with 'temperature_c' column

        Returns:
            DataFrame with additional 'temperature_f' column
        """
        df["temperature_f"] = (df["temperature_c"] * 9 / 5) + 32
        return df
