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
        ValidateSchema,
        ConvertTimestamp,
        ConvertTemperature,
        DetectAnomalies,
        DeduplicateReadings,
        AggregateMesh,
    )
    from .models import (
        sensor_input_schema,
        processed_reading_schema,
        mesh_summary_schema,
    )

    steps = [
        ValidateSchema(sensor_input_schema),
        ConvertTimestamp(),
        ConvertTemperature(),
        DetectAnomalies(config),
        ValidateSchema(processed_reading_schema),
        DeduplicateReadings(),
        AggregateMesh(),
        ValidateSchema(mesh_summary_schema),
    ]

    return Pipeline(steps)
