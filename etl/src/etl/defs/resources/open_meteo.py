import datetime
from typing import List

import dagster as dg
import requests
import pandas as pd

class OpenMeteo(dg.ConfigurableResource):
    available_metrics: dict[str, str] = {
        "temperature": "temperature_2m",
        "relative_humidity": "relative_humidity_2m",
        "apparent_temperature": "apparent_temperature",
        "precipitation": "precipitation",
        "rain": "rain",
        "snowfall": "snowfall",
        "weather_code": "weather_code",
        "cloud_cover": "cloud_cover",
        "wind_speed": "wind_speed_10m",
        "wind_direction": "wind_direction_10m",
        "wind_gusts": "wind_gusts_10m"
    }

    def _get_lat_lon(self, city: str) -> tuple:
        city_map = {
            "chennai": (13.0827, 80.2707),
            "mumbai": (19.0760, 72.8777),
            "delhi": (28.7041, 77.1025),
            "bangalore": (12.9716, 77.5946),
            "hyderabad": (17.3850, 78.4867),
        }
        return city_map.get(city.lower(), (0, 0))  # default to (0, 0) if city not found

    def _url(self, city: str, date: str, metric_names: List[str]) -> str:
        datetime.datetime.strptime(date, "%Y-%m-%d")
        lat, lon = self._get_lat_lon(city)
        metrics = [self.available_metrics.get(name) for name in metric_names if name in self.available_metrics]
        metrics_query = ",".join(metrics)
        return (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={date}&end_date={date}"
            f"&hourly={metrics_query}"
            "&timezone=auto"
        )

    def get_metrics(self, city: str, date: str, metric_names: List[str] = ['temperature']) -> dict:
        url = self._url(city, date, metric_names)  # Assuming the API can only handle one metric at a time
        response = requests.get(url)
        if response.status_code == 200:
            json_response = response.json()
            return pd.DataFrame(json_response.get("hourly", {}))
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code} - {response.text}")

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "open_meteo": OpenMeteo()
    })
