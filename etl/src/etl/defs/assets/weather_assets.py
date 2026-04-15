import dagster as dg
import pandas as pd

from etl.defs.resources.open_meteo import OpenMeteo


class WeatherAssetConfig(dg.Config):
    city: str = "chennai"
    date: str = "2024-01-01"
    metric_names: list[str] = ["temperature"]


@dg.asset
def temperature_bronze(
    config: WeatherAssetConfig, open_meteo: dg.ResourceParam[OpenMeteo]
) -> dg.MaterializeResult:
    df = open_meteo.get_metrics(config.city, config.date, config.metric_names)
    return dg.MaterializeResult(
        metadata={
            "city": config.city,
            "date": config.date,
            "metrics": config.metric_names,
            "rows": len(df),
        },
        value=df,
    )


@dg.asset
def temperature_silver(temperature_bronze: pd.DataFrame) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={"transformed": True, "rows": len(temperature_bronze)},
        value=temperature_bronze,
    )
