"""Integration tests for complete pipeline."""

import pandas as pd

from sensor_pipeline.models import PipelineConfig
from sensor_pipeline.pipeline import Pipeline, create_sensor_pipeline


class TestPipeline:
    """Test complete pipeline integration."""

    def test_empty_pipeline(self) -> None:
        """Test pipeline with no steps."""
        pipeline = Pipeline([])
        df = pd.DataFrame([{"test": "data"}])

        result = pipeline.run(df)
        pd.testing.assert_frame_equal(result, df)

    def test_pipeline_step_order(self) -> None:
        """Test that pipeline steps execute in order."""

        class AddColumn:
            def __init__(self, col_name: str, value: str) -> None:
                self.col_name = col_name
                self.value = value

            def transform(self, df: pd.DataFrame) -> pd.DataFrame:
                df = df.copy()
                df[self.col_name] = self.value
                return df

        steps = [
            AddColumn("step1", "first"),
            AddColumn("step2", "second"),
        ]

        pipeline = Pipeline(steps)
        df = pd.DataFrame([{"original": "data"}])
        result = pipeline.run(df)

        assert "step1" in result.columns
        assert "step2" in result.columns
        assert result["step1"].iloc[0] == "first"
        assert result["step2"].iloc[0] == "second"


class TestSensorPipeline:
    """Test complete sensor pipeline end-to-end."""

    def test_complete_sensor_pipeline(self) -> None:
        """Test full sensor data processing pipeline."""
        # Sample input data
        input_data = pd.DataFrame(
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
                    "device_id": "device-B",
                    "timestamp": "2025-03-26T13:46:00Z",
                    "temperature_c": 23.1,
                    "humidity": 42.8,
                    "status": "ok",
                },
                {
                    "mesh_id": "mesh-002",
                    "device_id": "device-C",
                    "timestamp": "2025-03-26T13:47:00Z",
                    "temperature_c": -15.2,  # Should trigger temp alert
                    "humidity": 35.6,
                    "status": "error",  # Should trigger both alerts
                },
            ]
        )

        config = PipelineConfig(
            temp_low=-10.0, temp_high=60.0, hum_low=10.0, hum_high=90.0
        )

        pipeline = create_sensor_pipeline(config)
        result = pipeline.run(input_data)

        # Should have 2 mesh summaries
        assert len(result) == 2
        assert set(result["mesh_id"]) == {"mesh-001", "mesh-002"}

        # Check required columns exist
        expected_columns = [
            "mesh_id",
            "avg_temperature_c",
            "avg_temperature_f",
            "avg_humidity",
            "total_readings",
            "temperature_anomaly_count",
            "humidity_anomaly_count",
            "status_anomaly_count",
            "healthy_reading_percentage",
        ]
        for col in expected_columns:
            assert col in result.columns

        # Check mesh-001 (normal readings)
        mesh001 = result[result["mesh_id"] == "mesh-001"].iloc[0]
        assert mesh001["total_readings"] == 2
        assert mesh001["avg_temperature_c"] == 22.75  # (22.4 + 23.1) / 2
        assert mesh001["temperature_anomaly_count"] == 0
        assert mesh001["humidity_anomaly_count"] == 0
        assert mesh001["status_anomaly_count"] == 0
        assert mesh001["healthy_reading_percentage"] == 100.0

        # Check mesh-002 (anomalous reading)
        mesh002 = result[result["mesh_id"] == "mesh-002"].iloc[0]
        assert mesh002["total_readings"] == 1
        assert mesh002["avg_temperature_c"] == -15.2
        assert mesh002["temperature_anomaly_count"] == 1  # Below threshold
        assert mesh002["humidity_anomaly_count"] == 0  # Humidity is normal
        assert mesh002["status_anomaly_count"] == 1  # Bad status
        assert mesh002["healthy_reading_percentage"] == 0.0

    def test_temperature_conversion_in_pipeline(self) -> None:
        """Test that temperature conversion works in full pipeline."""
        input_data = pd.DataFrame(
            [
                {
                    "mesh_id": "mesh-001",
                    "device_id": "device-A",
                    "timestamp": "2025-03-26T13:45:00Z",
                    "temperature_c": 0.0,  # Should be 32F
                    "humidity": 50.0,
                    "status": "ok",
                }
            ]
        )

        config = PipelineConfig()
        pipeline = create_sensor_pipeline(config)
        result = pipeline.run(input_data)

        assert result["avg_temperature_f"].iloc[0] == 32.0
