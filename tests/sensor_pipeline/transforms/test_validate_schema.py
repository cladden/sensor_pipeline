"""Tests for generic schema validation transform."""

import pandas as pd
import pytest
from pandera.errors import SchemaError, SchemaErrors

from sensor_pipeline.transforms import ValidateSchema
from sensor_pipeline.models import (
    sensor_input_schema,
    processed_reading_schema,
    mesh_summary_schema,
)


class TestValidateSchemaWithSensorInput:
    """Test ValidateSchema with sensor input schema."""

    def test_valid_sensor_data(self) -> None:
        """Test validation with valid sensor data."""
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

        transform = ValidateSchema(sensor_input_schema)
        result = transform.transform(df)

        # Should return the same DataFrame if valid
        pd.testing.assert_frame_equal(result, df)

    def test_missing_required_column(self) -> None:
        """Test validation fails with missing required column."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    # Missing 'status' column
                }
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)

    def test_extra_column_strict_mode(self) -> None:
        """Test validation fails with extra columns in strict mode."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "ok",
                    "extra_column": "not_allowed",  # Extra column
                }
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)

    def test_wrong_data_type(self) -> None:
        """Test validation fails with wrong data types."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": "not_a_number",  # Wrong type
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)

    def test_null_values(self) -> None:
        """Test validation fails with null values."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": None,  # Null value
                    "humidity": 41.2,
                    "status": "ok",
                }
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)

    def test_empty_dataframe(self) -> None:
        """Test validation with empty DataFrame."""
        # Create empty DataFrame with proper dtypes for pandera validation
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
        # Set proper dtypes for empty DataFrame
        df = df.astype(
            {
                "mesh_id": "string",
                "device_id": "string",
                "timestamp": "string",
                "temperature_c": "float64",
                "humidity": "float64",
                "status": "string",
            }
        )

        transform = ValidateSchema(sensor_input_schema)
        result = transform.transform(df)

        # Should return empty DataFrame with proper structure
        assert len(result) == 0
        assert list(result.columns) == [
            "mesh_id",
            "device_id",
            "timestamp",
            "temperature_c",
            "humidity",
            "status",
        ]

    def test_valid_status_values(self) -> None:
        """Test validation with all valid status values."""
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
                    "status": "warning",
                },
                {
                    "mesh_id": "mesh-003",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": 24.0,
                    "humidity": 43.5,
                    "status": "error",
                },
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        result = transform.transform(df)

        # Should validate all three valid status values
        pd.testing.assert_frame_equal(result, df)

    def test_invalid_status_value(self) -> None:
        """Test validation fails with invalid status value."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 22.4,
                    "humidity": 41.2,
                    "status": "invalid_status",  # Invalid status value
                }
            ]
        )

        transform = ValidateSchema(sensor_input_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)


class TestValidateSchemaWithProcessedReading:
    """Test ValidateSchema with processed reading schema."""

    def test_valid_processed_data(self) -> None:
        """Test validation with valid processed reading data."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": pd.to_datetime("2025-03-26T13:45:00Z"),
                    "timestamp_est": pd.to_datetime("2025-03-26T08:45:00-05:00"),
                    "temperature_c": 22.4,
                    "temperature_f": 72.32,
                    "humidity": 41.2,
                    "status": "ok",
                    "temperature_alert": False,
                    "humidity_alert": False,
                }
            ]
        )

        transform = ValidateSchema(processed_reading_schema)
        result = transform.transform(df)

        # Should return the same DataFrame if valid
        assert len(result) == 1
        assert list(result.columns) == list(df.columns)

    def test_missing_alert_columns(self) -> None:
        """Test validation fails with missing alert columns."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": pd.to_datetime("2025-03-26T13:45:00Z"),
                    "timestamp_est": pd.to_datetime("2025-03-26T08:45:00-05:00"),
                    "temperature_c": 22.4,
                    "temperature_f": 72.32,
                    "humidity": 41.2,
                    "status": "ok",
                    # Missing temperature_alert and humidity_alert
                }
            ]
        )

        transform = ValidateSchema(processed_reading_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)


class TestValidateSchemaWithMeshSummary:
    """Test ValidateSchema with mesh summary schema."""

    def test_valid_mesh_summary(self) -> None:
        """Test validation with valid mesh summary data."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "avg_temperature_c": 22.4,
                    "avg_temperature_f": 72.32,
                    "avg_humidity": 41.2,
                    "total_readings": 100,
                    "temperature_alert": False,
                    "humidity_alert": True,
                }
            ]
        )

        transform = ValidateSchema(mesh_summary_schema)
        result = transform.transform(df)

        # Should return the same DataFrame if valid
        pd.testing.assert_frame_equal(result, df)

    def test_invalid_total_readings_type(self) -> None:
        """Test validation fails with wrong total_readings type."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "avg_temperature_c": 22.4,
                    "avg_temperature_f": 72.32,
                    "avg_humidity": 41.2,
                    "total_readings": "not_a_number",  # Should be int
                    "temperature_alert": False,
                    "humidity_alert": True,
                }
            ]
        )

        transform = ValidateSchema(mesh_summary_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)

    def test_invalid_alert_type(self) -> None:
        """Test validation fails with wrong alert column types."""
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "avg_temperature_c": 22.4,
                    "avg_temperature_f": 72.32,
                    "avg_humidity": 41.2,
                    "total_readings": 100,
                    "temperature_alert": "not_a_bool",  # Should be bool
                    "humidity_alert": True,
                }
            ]
        )

        transform = ValidateSchema(mesh_summary_schema)
        with pytest.raises((SchemaError, SchemaErrors)):
            transform.transform(df)


class TestValidateSchemaGeneric:
    """Test generic ValidateSchema functionality."""

    def test_lazy_validation_provides_detailed_errors(self) -> None:
        """Test that lazy validation provides detailed error information."""
        # Create data with multiple validation errors
        df = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "avg_temperature_c": 22.4,
                    "avg_temperature_f": 72.32,
                    "avg_humidity": 41.2,
                    "total_readings": "invalid",  # Wrong type
                    "temperature_alert": "also_invalid",  # Wrong type
                    "humidity_alert": True,
                }
            ]
        )

        transform = ValidateSchema(mesh_summary_schema)
        with pytest.raises((SchemaError, SchemaErrors)) as exc_info:
            transform.transform(df)

        # Should provide detailed error information
        error_str = str(exc_info.value)
        assert "total_readings" in error_str
        assert "temperature_alert" in error_str
