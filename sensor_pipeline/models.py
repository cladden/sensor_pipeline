"""Data models for sensor pipeline."""

from datetime import datetime

from pydantic import BaseModel, Field


class SensorReading(BaseModel):
    """Individual sensor reading."""

    mesh_id: str
    device_id: str
    timestamp: datetime
    temperature_c: float
    humidity: float
    status: str


class ProcessedReading(BaseModel):
    """Sensor reading after initial processing."""

    mesh_id: str
    device_id: str
    timestamp: datetime
    timestamp_est: datetime
    temperature_c: float
    temperature_f: float
    humidity: float
    status: str
    temperature_alert: bool = False
    humidity_alert: bool = False


class MeshSummary(BaseModel):
    """Aggregated summary per mesh network."""

    mesh_id: str
    avg_temperature_c: float
    avg_temperature_f: float
    avg_humidity: float
    total_readings: int
    temperature_alert: bool = False
    humidity_alert: bool = False


class PipelineConfig(BaseModel):
    """Configuration for pipeline execution."""

    temp_low: float = Field(default=-10.0, description="Low temperature threshold (C)")
    temp_high: float = Field(default=60.0, description="High temperature threshold (C)")
    hum_low: float = Field(default=10.0, description="Low humidity threshold (%)")
    hum_high: float = Field(default=90.0, description="High humidity threshold (%)")
