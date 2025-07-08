"""Tests for deduplicate readings transform."""

import pandas as pd

from sensor_pipeline.transforms.deduplicate_readings import DeduplicateReadings


class TestDeduplicateReadings:
    """Test deduplication transform."""

    def test_remove_exact_duplicates(self) -> None:
        """Test removal of exact duplicates."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",  # Exact duplicate
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",  # Different device
                    "temperature_c": 23.1,
                    "humidity": 42.8,
                    "status": "ok",
                },
            ]
        )

        transform = DeduplicateReadings()
        result = transform.transform(df)

        # Should remove one duplicate, keep 2 records
        assert len(result) == 2
        assert result["device_id"].tolist() == ["device-A", "device-B"]

    def test_keep_first_occurrence(self) -> None:
        """Test that first occurrence is kept when duplicates exist."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,  # First occurrence
                    "humidity": 41.2,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 99.9,  # Duplicate with different temp
                    "humidity": 99.9,
                    "status": "error",
                },
            ]
        )

        transform = DeduplicateReadings()
        result = transform.transform(df)

        # Should keep only first occurrence
        assert len(result) == 1
        assert result["temperature_c"].iloc[0] == 22.4
        assert result["humidity"].iloc[0] == 41.2
        assert result["status"].iloc[0] == "ok"

    def test_different_timestamps_not_duplicates(self) -> None:
        """Test that same device at different times are not duplicates."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",  # Time 1
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:46:00Z",  # Time 2
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                },
            ]
        )

        transform = DeduplicateReadings()
        result = transform.transform(df)

        # Should keep both records (different timestamps)
        assert len(result) == 2

    def test_no_duplicates(self) -> None:
        """Test behavior when no duplicates exist."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-002",
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 23.1,
                    "humidity": 42.8,
                    "status": "ok",
                },
            ]
        )

        transform = DeduplicateReadings()
        result = transform.transform(df)

        # Should return all records unchanged
        assert len(result) == 2
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), df.reset_index(drop=True)
        )

    def test_empty_dataframe(self) -> None:
        """Test behavior with empty DataFrame."""
        df = pd.DataFrame(
            columns=[
                "mesh_id",
                "device_id",
                "timestamp",
                "temperature_c",
                "humidity",
                "status",
            ]
        )

        transform = DeduplicateReadings()
        result = transform.transform(df)

        # Should return empty DataFrame with same columns
        assert len(result) == 0
        assert list(result.columns) == list(df.columns)
