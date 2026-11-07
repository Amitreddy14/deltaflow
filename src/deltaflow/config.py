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
    # Regex (leading ^) so workers pick up topics the connector creates after they start.
    kafka_topic_pattern: str = r"^deltaflow\.public\..+"
    mongo_database: str = "deltaflow"
    connector_name: str = "shop-postgres"
    # A sink worker touches this file every loop, so a container healthcheck can tell a worker
    # that is consuming from one that is hung. Unset disables the heartbeat.
    heartbeat_file: str | None = None
    # Elasticsearch basic auth (production enables xpack security). Unset means no auth.
    elasticsearch_user: str | None = None
    elasticsearch_password: str | None = None
    # Production switch: the API refuses to start without an api_key, and every route except
    # /health then requires it (including reads, /metrics and the connector config).
    auth_required: bool = False
    # When set, every state-changing API call must send it as the X-API-Key header.
    # Unset means open access, which is only appropriate for local development.
    api_key: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
