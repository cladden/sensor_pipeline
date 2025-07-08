"""Generic schema validation using pandera."""

import pandas as pd
from pandera.pandas import DataFrameSchema


class ValidateSchema:
    """Generic schema validation for any pandera DataFrameSchema."""

    def __init__(self, schema: DataFrameSchema):
        """Initialize with schema to validate against.

        Args:
            schema: Pandera DataFrameSchema to validate against
        """
        self.schema = schema

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate DataFrame against schema.

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame (unchanged if valid)

        Raises:
            SchemaError: If validation fails with full row/col detail
        """
        # Raises SchemaErrors with full row/col detail if anything fails
        return self.schema.validate(df, lazy=True)
