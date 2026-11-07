import pytest

from deltaflow.config import Settings


def test_defaults_match_local_compose_stack() -> None:
    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "127.0.0.1:29092"
    assert settings.connect_url == "http://127.0.0.1:8083"
    assert settings.mongo_database == "deltaflow"


def test_environment_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DELTAFLOW_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "kafka:9092"
