"""Aggregate sensor readings by mesh network."""

import pandas as pd

from ..models import PipelineConfig


class AggregateMesh:
    """Aggregate readings by mesh network."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        agg_dict = {
            "temperature_c": "mean",
            "temperature_f": "mean",
            "humidity": "mean",
            "mesh_id": "count",
        }

        if "temperature_alert" in df.columns:
            agg_dict["temperature_alert"] = "any"
        if "humidity_alert" in df.columns:
            agg_dict["humidity_alert"] = "any"

        agg_df = df.groupby("mesh_id").agg(agg_dict)
        agg_df = (
            agg_df.rename(columns={"mesh_id": "total_readings"})
                  .reset_index()
                  .rename(
                      columns={
                          "temperature_c": "avg_temperature_c",
                          "temperature_f": "avg_temperature_f",
                          "humidity": "avg_humidity",
                      }
                  )
        )

        # mesh-level alerts are simply "any row alert"
        if "temperature_alert" not in agg_df.columns:
            agg_df["temperature_alert"] = False
        if "humidity_alert" not in agg_df.columns:
            agg_df["humidity_alert"] = False

        return agg_df
