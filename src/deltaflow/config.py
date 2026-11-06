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

    # Reachable from the host. Inside the compose network the services override these.
    kafka_bootstrap_servers: str = "localhost:29092"
    connect_url: str = "http://localhost:8083"
    elasticsearch_url: str = "http://localhost:9200"
    mongo_url: str = "mongodb://localhost:27017"
    mongo_database: str = "deltaflow"


@lru_cache
def get_settings() -> Settings:
    return Settings()
