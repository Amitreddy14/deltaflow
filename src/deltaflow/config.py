"""Central runtime configuration, read from DELTAFLOW_* environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All settings have local-development defaults that match docker-compose.yml."""

    model_config = SettingsConfigDict(
        env_prefix="DELTAFLOW_",
        env_file=".env",
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"

    # Reachable from the host. 127.0.0.1 (not localhost) avoids slow IPv6-first lookups on Windows.
    # Inside the compose network the services override these.
    postgres_dsn: str = "postgresql://deltaflow:deltaflow@127.0.0.1:5432/shop"
    kafka_bootstrap_servers: str = "127.0.0.1:29092"
    connect_url: str = "http://127.0.0.1:8083"
    elasticsearch_url: str = "http://127.0.0.1:9200"
    mongo_url: str = "mongodb://127.0.0.1:27017"
    mongo_database: str = "deltaflow"


@lru_cache
def get_settings() -> Settings:
    return Settings()
