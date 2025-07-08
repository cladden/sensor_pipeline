"""Data models for sensor pipeline."""

from pydantic import BaseModel, Field
import pandera.pandas as pa


# Pandera schema for sensor input validation
sensor_input_schema = pa.DataFrameSchema(
    {
        "mesh_id": pa.Column(pa.String, nullable=False),
        "device_id": pa.Column(pa.String, nullable=False),
        "timestamp": pa.Column(
            pa.String, nullable=False
        ),  # string type to handle malformed timestamps
        "temperature_c": pa.Column(pa.Float, nullable=False),
        "humidity": pa.Column(pa.Float, nullable=False),
        "status": pa.Column(
            pa.String, checks=pa.Check.isin(["ok", "warning", "error"]), nullable=False
        ),
    },
    strict=True,  # no extra cols
    coerce=False,  # no auto-cast dtypes
)


# Pandera schema for processed reading validation
processed_reading_schema = pa.DataFrameSchema(
    {
        "mesh_id": pa.Column(pa.String, nullable=False),
        "device_id": pa.Column(pa.String, nullable=False),
        "timestamp": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "timestamp_est": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "temperature_c": pa.Column(pa.Float, nullable=False),
        "temperature_f": pa.Column(pa.Float, nullable=False),
        "humidity": pa.Column(pa.Float, nullable=False),
        "status": pa.Column(
            pa.String, checks=pa.Check.isin(["ok", "warning", "error"]), nullable=False
        ),
        "temperature_alert": pa.Column(pa.Bool, nullable=False),
        "humidity_alert": pa.Column(pa.Bool, nullable=False),
        "status_alert": pa.Column(pa.Bool, nullable=False),
        "is_healthy": pa.Column(pa.Bool, nullable=False),
    },
    strict=True,  # no extra cols
    coerce=False,  # no auto-cast dtypes
)


# Pandera schema for mesh summary validation
mesh_summary_schema = pa.DataFrameSchema(
    {
        "mesh_id": pa.Column(pa.String, nullable=False),
        "avg_temperature_c": pa.Column(pa.Float, nullable=False),
        "avg_temperature_f": pa.Column(pa.Float, nullable=False),
        "avg_humidity": pa.Column(pa.Float, nullable=False),
        "total_readings": pa.Column(pa.Int, nullable=False),
        "temperature_anomaly_count": pa.Column(pa.Int, nullable=False),
        "humidity_anomaly_count": pa.Column(pa.Int, nullable=False),
        "status_anomaly_count": pa.Column(pa.Int, nullable=False),
        "healthy_reading_percentage": pa.Column(pa.Float, nullable=False),
    },
    strict=True,  # no extra cols
    coerce=False,  # no auto-cast dtypes
)


class PipelineConfig(BaseModel):
    """Configuration for pipeline execution."""

    temp_low: float = Field(default=-10.0, description="Low temperature threshold (C)")
    temp_high: float = Field(default=60.0, description="High temperature threshold (C)")
    hum_low: float = Field(default=10.0, description="Low humidity threshold (%)")
    hum_high: float = Field(default=90.0, description="High humidity threshold (%)")
