"""One settings object for every DockWatch component, read from env vars / `.env` (prefix DOCKWATCH_)."""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from dockwatch import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DOCKWATCH_", env_file=".env", extra="ignore")

    target: Literal["local", "aws"] = "local"

    gbfs_discovery_url: str = "https://gbfs.baywheels.com/gbfs/gbfs.json"
    gbfs_version: str = "2.3"
    gbfs_language: str = "en"
    user_agent: str = f"dockwatch/{__version__} (+https://github.com/braaaeeedyn/dockwatch)"
    http_timeout_s: float = 20.0

    kafka_bootstrap: str = "localhost:19092"
    topic_station_status: str = "gbfs.station_status"
    topic_station_information: str = "gbfs.station_information"
    topic_dlq: str = "gbfs.dlq"
    topic_alerts: str = "gbfs.alerts"
    topic_station_state: str = "dockwatch.station_state"

    # Spark / Iceberg (M2). Inside Docker the hosts are the compose service names.
    spark_kafka_bootstrap: str = "redpanda:9092"
    iceberg_catalog: str = "lake"
    iceberg_rest_uri: str = "http://iceberg-rest:8181"
    iceberg_s3_endpoint: str = "http://s3:8333"
    checkpoint_root: str = "/checkpoints"
    trigger_seconds: int = 60
    watermark: str = "10 minutes"

    # Exporter (M2): JSON files the static site reads
    export_dir: str = "web/data"
    export_interval_s: int = 60

    # Station state rule (DESIGN.md §2, streaming/episodes.py)
    stale_after_min: int = 30
    low_count: int = 2
    low_share: float = 0.10
    alert_after_min: int = 15

    archive_kind: Literal["s3", "fs"] = "s3"
    archive_fs_root: str = "data/raw"
    s3_bucket: str = "dockwatch"
    s3_endpoint: str | None = "http://localhost:8333"
    s3_region: str = "us-west-2"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
