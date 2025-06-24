"""Tests for file-based data source."""

import json
import tempfile
from pathlib import Path

import pytest

from sensor_pipeline.sources import FileSource


class TestFileSource:
    """Test file-based data source."""

    def test_load_json_file(self) -> None:
        """Test loading standard JSON file."""
        data = [
            {"mesh_id": "mesh-001", "device_id": "device-A", "temperature_c": 22.4},
            {"mesh_id": "mesh-002", "device_id": "device-B", "temperature_c": 23.1},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            temp_path = f.name

        try:
            source = FileSource(temp_path)
            df = source.load()

            assert len(df) == 2
            assert list(df.columns) == ["mesh_id", "device_id", "temperature_c"]
            assert df["mesh_id"].tolist() == ["mesh-001", "mesh-002"]
            assert df["temperature_c"].tolist() == [22.4, 23.1]
        finally:
            Path(temp_path).unlink()

    def test_load_jsonl_file(self) -> None:
        """Test loading JSON Lines file."""
        data = [
            {"mesh_id": "mesh-001", "device_id": "device-A", "temperature_c": 22.4},
            {"mesh_id": "mesh-002", "device_id": "device-B", "temperature_c": 23.1},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for item in data:
                json.dump(item, f)
                f.write("\n")
            temp_path = f.name

        try:
            source = FileSource(temp_path)
            df = source.load()

            assert len(df) == 2
            assert list(df.columns) == ["mesh_id", "device_id", "temperature_c"]
            assert df["mesh_id"].tolist() == ["mesh-001", "mesh-002"]
            assert df["temperature_c"].tolist() == [22.4, 23.1]
        finally:
            Path(temp_path).unlink()

    def test_file_not_found(self) -> None:
        """Test error handling for missing file."""
        source = FileSource("nonexistent.json")
        with pytest.raises(FileNotFoundError):
            source.load()

    def test_unsupported_format(self) -> None:
        """Test error handling for unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"not json")
            temp_path = f.name

        try:
            source = FileSource(temp_path)
            with pytest.raises(ValueError, match="Unsupported file format"):
                source.load()
        finally:
            Path(temp_path).unlink()
