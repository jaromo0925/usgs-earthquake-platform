from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    app_name: str = "USGS Earthquake Platform"
    app_env: str = "development"
    log_level: str = "INFO"

    mongodb_uri: str = "mongodb://mongo:27017"
    mongodb_database: str = "earthquakes"

    usgs_url: str = (
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    )
    ingestion_interval_seconds: int = Field(default=180, ge=10)
    usgs_timeout_seconds: float = Field(default=20.0, gt=0)
    usgs_max_retries: int = Field(default=3, ge=1, le=10)

    api_default_page_size: int = Field(default=20, ge=1, le=100)
    api_max_page_size: int = Field(default=100, ge=1, le=500)


@lru_cache
def get_settings() -> Settings:
    return Settings()

