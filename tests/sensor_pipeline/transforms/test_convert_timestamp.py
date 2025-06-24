"""Tests for timestamp conversion transform."""

import pandas as pd
import pytest

from sensor_pipeline.transforms import ConvertTimestamp


class TestConvertTimestamp:
    """Test timestamp conversion transform."""

    def test_iso_timestamp_conversion(self) -> None:
        """Test conversion of ISO format timestamps."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-03-21T21:22:44.052986Z",
                    "2025-03-22T18:33:44.053032Z",
                    "2025-03-23T18:11:44.053048Z",
                ],
                "value": [1, 2, 3],
            }
        )

        transform = ConvertTimestamp()
        result = transform.transform(df)

        assert "timestamp" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

        # Check specific conversion
        expected = pd.Timestamp("2025-03-21T21:22:44.052986Z")
        assert result["timestamp"].iloc[0] == expected

    def test_mixed_timestamp_formats(self) -> None:
        """Test handling of mixed timestamp formats found in real data.

        Data contains exactly two formats:
        - Normal ISO: YYYY-MM-DDTHH:MM:SS.SSSSSSZ (500 occurrences)
        - Malformed: YYYY-MM-DDTHH:MM:SS.SSSSSS+00:00Z (49,500 occurrences)
        """
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-03-21T21:22:44.052986Z",  # Normal ISO format (500 in data)
                    "2025-05-07T16:32:44.057320+00:00Z",  # Malformed format (49,500 in data)
                    "2025-03-23T22:04:44.056442+00:00Z",  # Another malformed example
                    "2025-03-24T02:10:44.053059Z",  # Another normal ISO format
                ],
                "value": [1, 2, 3, 4],
            }
        )

        transform = ConvertTimestamp()
        result = transform.transform(df)

        assert "timestamp" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert "timestamp_est" in result.columns

        # All timestamps should be parsed successfully
        assert len(result) == 4
        assert not result["timestamp"].isna().any()

        # Check that the malformed timestamps are cleaned and parsed correctly
        # The +00:00Z should be converted to just Z
        expected_first = pd.Timestamp("2025-03-21T21:22:44.052986Z")
        expected_second = pd.Timestamp(
            "2025-05-07T16:32:44.057320Z"
        )  # Should be cleaned to Z format

        assert result["timestamp"].iloc[0] == expected_first
        assert result["timestamp"].iloc[1] == expected_second

    def test_invalid_timestamp_format(self) -> None:
        """Test handling of invalid timestamp format."""
        df = pd.DataFrame(
            {
                "timestamp": ["invalid-timestamp", "2025-03-26T13:45:00Z"],
                "value": [1, 2],
            }
        )

        transform = ConvertTimestamp()
        with pytest.raises(ValueError):
            transform.transform(df)

    def test_missing_timestamp_column(self) -> None:
        """Test error when timestamp column is missing."""
        df = pd.DataFrame({"value": [1, 2, 3]})

        transform = ConvertTimestamp()
        with pytest.raises(KeyError):
            transform.transform(df)
