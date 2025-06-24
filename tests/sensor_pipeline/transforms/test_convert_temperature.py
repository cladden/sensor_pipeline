"""Tests for temperature conversion transform."""

import pandas as pd
import pytest

from sensor_pipeline.transforms import ConvertTemperature


class TestConvertTemperature:
    """Test temperature conversion transform."""

    def test_celsius_to_fahrenheit(self) -> None:
        """Test conversion from Celsius to Fahrenheit."""
        df = pd.DataFrame(
            {
                "temperature_c": [0.0, 100.0, -40.0, 37.0],
                "other_column": ["a", "b", "c", "d"],
            }
        )

        transform = ConvertTemperature()
        result = transform.transform(df)

        # Check that original column is preserved
        assert "temperature_c" in result.columns
        pd.testing.assert_series_equal(result["temperature_c"], df["temperature_c"])

        # Check new Fahrenheit column
        assert "temperature_f" in result.columns
        expected_f = [32.0, 212.0, -40.0, 98.6]
        pd.testing.assert_series_equal(
            result["temperature_f"],
            pd.Series(expected_f, name="temperature_f"),
            check_exact=False,
            atol=0.1,
        )

        # Check other columns are preserved
        assert "other_column" in result.columns
        pd.testing.assert_series_equal(result["other_column"], df["other_column"])

    def test_missing_temperature_column(self) -> None:
        """Test error when temperature_c column is missing."""
        df = pd.DataFrame({"other_column": [1, 2, 3]})

        transform = ConvertTemperature()
        with pytest.raises(KeyError):
            transform.transform(df)
