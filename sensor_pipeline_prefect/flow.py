"""Prefect 3 flow for sensor data pipeline."""

import json
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from sensor_pipeline.models import PipelineConfig
from sensor_pipeline.pipeline import create_sensor_pipeline
from sensor_pipeline.sources import FileSource, SensorSource
from sensor_pipeline.transforms import (
    ValidateSchema,
    ConvertTimestamp,
    ConvertTemperature,
    DetectAnomalies,
    DeduplicateReadings,
    AggregateMesh,
)
from sensor_pipeline.models import (
    sensor_input_schema,
    processed_reading_schema,
    mesh_summary_schema,
)


@task(
    retries=3,
    retry_delay_seconds=10,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def load_to_df(source_url: str) -> pd.DataFrame:
    """Load sensor data from source URL.

    Args:
        source_url: Proto-URL (file: or api:) for data source

    Returns:
        DataFrame with sensor readings
    """
    parsed = urlparse(source_url)

    source: SensorSource
    if parsed.scheme == "file":
        source = FileSource(parsed.path)
    else:
        raise ValueError(
            f"Unsupported source scheme: {parsed.scheme}. Only 'file:' is supported."
        )

    df = source.load()
    print(f"Loaded {len(df)} sensor readings from {source_url}")
    return df


@task
def validate_sensor_input(df: pd.DataFrame) -> pd.DataFrame:
    """Validate sensor input data against schema.

    Args:
        df: Input DataFrame with sensor readings

    Returns:
        Validated DataFrame

    Raises:
        SchemaError: If validation fails
    """
    transform = ValidateSchema(sensor_input_schema)
    result = transform.transform(df)
    print(f"Validated {len(df)} sensor readings")
    return result


@task
def convert_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Convert UTC timestamps to Eastern Time.

    Args:
        df: DataFrame with timestamp column

    Returns:
        DataFrame with timestamp_est column added
    """
    transform = ConvertTimestamp()
    result = transform.transform(df)
    print(f"Converted timestamps for {len(df)} readings")
    return result


@task
def convert_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """Convert temperature from Celsius to Fahrenheit.

    Args:
        df: DataFrame with temperature_c column

    Returns:
        DataFrame with temperature_f column added
    """
    transform = ConvertTemperature()
    result = transform.transform(df)
    print(f"Converted temperatures for {len(df)} readings")
    return result


@task
def validate_processed_reading(df: pd.DataFrame) -> pd.DataFrame:
    """Validate processed reading data against schema.

    Args:
        df: DataFrame with processed readings

    Returns:
        Validated DataFrame

    Raises:
        SchemaError: If validation fails
    """
    transform = ValidateSchema(processed_reading_schema)
    result = transform.transform(df)
    print(f"Validated {len(df)} processed readings")
    return result


@task
def detect_anomalies(df: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    """Detect temperature and humidity anomalies.

    Args:
        df: DataFrame with sensor readings
        config: Pipeline configuration with thresholds

    Returns:
        DataFrame with alert columns added
    """
    transform = DetectAnomalies(config)
    result = transform.transform(df)
    print(f"Detected anomalies for {len(df)} readings")
    return result


@task
def aggregate_mesh(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate readings by mesh network.

    Args:
        df: DataFrame with sensor readings

    Returns:
        DataFrame with mesh summaries
    """
    transform = AggregateMesh()
    result = transform.transform(df)
    print(f"Aggregated {len(df)} readings into {len(result)} mesh summaries")
    return result


@task
def validate_mesh_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Validate mesh summary data against schema.

    Args:
        df: DataFrame with mesh summaries

    Returns:
        Validated DataFrame

    Raises:
        SchemaError: If validation fails
    """
    transform = ValidateSchema(mesh_summary_schema)
    result = transform.transform(df)
    print(f"Validated {len(df)} mesh summaries")
    return result


@task
def deduplicate_readings(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate sensor readings.

    Args:
        df: DataFrame with processed readings

    Returns:
        DataFrame with duplicates removed
    """
    transform = DeduplicateReadings()
    result = transform.transform(df)
    print(f"Deduplicated from {len(df)} to {len(result)} readings")
    return result


@task
def run_core_pipeline(df: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    """Run the core sensor pipeline processing.

    Args:
        df: Input DataFrame with sensor readings
        config: Pipeline configuration

    Returns:
        Processed DataFrame with mesh summaries
    """
    pipeline = create_sensor_pipeline(config)
    result = pipeline.run(df)
    print(f"Processed {len(df)} readings into {len(result)} mesh summaries")
    return result


@task
def persist(df: pd.DataFrame, output_path: str) -> None:
    """Persist DataFrame to JSON file.

    Args:
        df: DataFrame to save
        output_path: Path to output JSON file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Convert to pretty JSON
    data = df.to_dict("records")
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Results saved to {output_path}")


@flow(name="Sensor-Mesh-Summary", log_prints=True)
def sensor_mesh_summary(
    input: str = "file:data/sensor_data.json",
    output_path: str = "out/mesh_summary.json",
    temp_low: float = -10.0,
    temp_high: float = 60.0,
    hum_low: float = 10.0,
    hum_high: float = 90.0,
) -> None:
    """Sensor mesh summary flow.

    Args:
        input: Proto-URL for input data source
        output_path: Path for output JSON file
        temp_low: Low temperature threshold (C)
        temp_high: High temperature threshold (C)
        hum_low: Low humidity threshold (%)
        hum_high: High humidity threshold (%)
    """
    # Create configuration
    config = PipelineConfig(
        temp_low=temp_low,
        temp_high=temp_high,
        hum_low=hum_low,
        hum_high=hum_high,
    )

    # Execute pipeline tasks
    df = load_to_df(input)
    validated_df = validate_sensor_input(df)
    timestamp_df = convert_timestamp(validated_df)
    temperature_df = convert_temperature(timestamp_df)
    anomaly_df = detect_anomalies(temperature_df, config)
    processed_df = validate_processed_reading(anomaly_df)
    deduplicated_df = deduplicate_readings(processed_df)
    summary_df = aggregate_mesh(deduplicated_df)
    validated_summary_df = validate_mesh_summary(summary_df)
    persist(validated_summary_df, output_path)


if __name__ == "__main__":
    # Run with defaults
    sensor_mesh_summary()
