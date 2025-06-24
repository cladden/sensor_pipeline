"""Abstract base class for sensor data sources."""

from abc import ABC, abstractmethod
import pandas as pd


class SensorSource(ABC):
    """Abstract base class for sensor data sources."""

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """Load sensor data and return as DataFrame.

        Returns:
            DataFrame with sensor readings
        """
        pass
