"""Command-line interface for sensor pipeline."""

import argparse
import json
from pathlib import Path
import sys

from .models import PipelineConfig
from .pipeline import create_sensor_pipeline
from .sources import FileSource


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Run sensor data pipeline")
    parser.add_argument("input_file", help="Input JSON/JSONL file path")
    parser.add_argument("output_file", help="Output JSON file path")
    parser.add_argument(
        "--temp-low", type=float, default=-10.0, help="Low temperature threshold (C)"
    )
    parser.add_argument(
        "--temp-high", type=float, default=60.0, help="High temperature threshold (C)"
    )
    parser.add_argument(
        "--hum-low", type=float, default=10.0, help="Low humidity threshold (%%)"
    )
    parser.add_argument(
        "--hum-high", type=float, default=90.0, help="High humidity threshold (%%)"
    )

    args = parser.parse_args()

    try:
        # Create configuration
        config = PipelineConfig(
            temp_low=args.temp_low,
            temp_high=args.temp_high,
            hum_low=args.hum_low,
            hum_high=args.hum_high,
        )

        # Load data
        source = FileSource(args.input_file)
        df = source.load()
        print(f"Loaded {len(df)} sensor readings")

        # Run pipeline
        pipeline = create_sensor_pipeline(config)
        result = pipeline.run(df)
        print(f"Processed into {len(result)} mesh summaries")

        # Save results
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(result.to_dict("records"), f, indent=2, default=str)

        print(f"Results saved to {output_path}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
