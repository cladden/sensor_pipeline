"""Generic pipeline for composing transformation steps."""

from typing import Any
import pandas as pd

from .models import PipelineConfig


class Pipeline:
    """Generic pipeline for composing transformation steps."""

    def __init__(self, steps: list[Any]):
        """Initialize pipeline with transformation steps.

        Args:
            steps: List of transform objects with transform() method
        """
        self.steps = steps

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all pipeline steps in sequence.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame after all steps
        """
        result = df.copy()

        for step in self.steps:
            result = step.transform(result)

        return result


def create_sensor_pipeline(config: PipelineConfig) -> Pipeline:
    """Create a sensor data processing pipeline.

    Args:
        config: Pipeline configuration

    Returns:
        Configured Pipeline instance
    """
    from .transforms import (
        ConvertTimestamp,
        ConvertTemperature,
        DetectAnomalies,
        AggregateMesh,
    )

    steps = [
        ConvertTimestamp(),
        ConvertTemperature(),
        DetectAnomalies(config),
        AggregateMesh(config),
    ]

    return Pipeline(steps)
