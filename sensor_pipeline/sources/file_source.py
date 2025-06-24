"""File-based sensor data source."""

import json
from pathlib import Path
import pandas as pd

from .source_base import SensorSource


class FileSource(SensorSource):
    """Load sensor data from JSON/JSONL files."""

    def __init__(self, file_path: str | Path):
        """Initialize with file path.

        Args:
            file_path: Path to JSON or JSONL file
        """
        self.file_path = Path(file_path)

    def load(self) -> pd.DataFrame:
        """Load data from file.

        Returns:
            DataFrame with sensor readings

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is unsupported
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        suffix = self.file_path.suffix.lower()

        if suffix == ".json":
            # Standard JSON file
            with open(self.file_path, "r") as f:
                data = json.load(f)
            return pd.DataFrame(data)

        elif suffix == ".jsonl":
            # JSON Lines format
            records = []
            with open(self.file_path, "r") as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))
            return pd.DataFrame(records)

        else:
            raise ValueError(f"Unsupported file format: {suffix}")
