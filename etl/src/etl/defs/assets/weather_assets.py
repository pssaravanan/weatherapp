import dagster as dg
import pandas as pd

from etl.defs.resources.open_meteo import OpenMeteo

daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")

class WeatherAssetConfig(dg.Config):
    city: str = "chennai"
    date: str = "2024-01-01"
    metric_names: list[str] = ["temperature"]

@dg.asset(
    group_name="bronze",
    description="Raw hourly weather data landed from the Open-Meteo API.",
    partitions_def=daily_partitions,
    metadata={ 'partition_expr': "date" }
)
def temperature_bronze(context: dg.AssetExecutionContext,
    config: WeatherAssetConfig, open_meteo: dg.ResourceParam[OpenMeteo]
) -> pd.DataFrame:
    partition_date = context.partition_key
    df = open_meteo.get_metrics(config.city, partition_date, config.metric_names)
    bronze_df = df.copy()
    bronze_df["city"] = config.city
    bronze_df["date"] = partition_date
    bronze_df["layer"] = "bronze"
    return bronze_df

@dg.asset(
    group_name="silver",
    description="Cleaned hourly weather data modeled from the bronze layer.",
    partitions_def=daily_partitions,
    metadata={ 'partition_expr': "date" }
)
def temperature_silver(temperature_bronze: pd.DataFrame) -> pd.DataFrame:
    silver_df = temperature_bronze.copy()
    silver_df.columns = [column.lower() for column in silver_df.columns]
    silver_df["layer"] = "silver"
    return silver_df
