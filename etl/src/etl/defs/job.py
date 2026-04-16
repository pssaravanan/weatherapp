import dagster as dg

weather_assets_job = dg.define_asset_job(
    name="weather_assets_job",
    selection=dg.AssetSelection.assets("temperature_bronze","temperature_silver"),
    description="Materialize the weather asset pipeline from bronze through silver.",
)


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(jobs=[weather_assets_job])
