from __future__ import annotations

import stat
from pathlib import Path

import pytest

from deltaflow import cli, envgen


def _parse(path: Path) -> dict[str, str]:
    lines = [ln for ln in path.read_text().splitlines() if ln and not ln.startswith("#")]
    return dict(ln.split("=", 1) for ln in lines)


def test_generates_distinct_url_safe_secrets(tmp_path: Path) -> None:
    written = envgen.generate(tmp_path, domain="Deltaflow.Example.com", email="a@b.io")

    env = _parse(written.env_file)
    assert env["DOMAIN"] == "deltaflow.example.com"
    secrets_ = [env[k] for k in ("POSTGRES_PASSWORD", "ELASTIC_PASSWORD", "MONGO_ROOT_PASSWORD")]
    secrets_.append(env["DELTAFLOW_API_KEY"])
    assert len(set(secrets_)) == 4
    assert all(len(s) == 48 and all(c in "0123456789abcdef" for c in s) for s in secrets_)
    assert written.api_key_file.read_text() == env["DELTAFLOW_API_KEY"]


def test_env_file_is_owner_only_and_secrets_dir_is_private(tmp_path: Path) -> None:
    written = envgen.generate(tmp_path, domain="x.example.com", email="a@b.io")

    assert stat.S_IMODE(written.env_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(written.api_key_file.parent.stat().st_mode) == 0o700


def test_two_runs_never_share_secrets(tmp_path: Path) -> None:
    first = envgen.generate(tmp_path / "a", domain="x.example.com", email="a@b.io")
    second = envgen.generate(tmp_path / "b", domain="x.example.com", email="a@b.io")

    assert (
        _parse(first.env_file)["POSTGRES_PASSWORD"] != _parse(second.env_file)["POSTGRES_PASSWORD"]
    )


def test_refuses_to_overwrite_existing_secrets(tmp_path: Path) -> None:
    written = envgen.generate(tmp_path, domain="x.example.com", email="a@b.io")
    before = written.env_file.read_text()

    with pytest.raises(envgen.EnvExists):
        envgen.generate(tmp_path, domain="x.example.com", email="a@b.io")

    assert written.env_file.read_text() == before


def test_existing_key_file_blocks_before_anything_is_written(tmp_path: Path) -> None:
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "prometheus_api_key").write_text("old")

    with pytest.raises(envgen.EnvExists):
        envgen.generate(tmp_path, domain="x.example.com", email="a@b.io")

    assert not (tmp_path / ".env.prod").exists()


@pytest.mark.parametrize(
    "domain", ["", "localhost", "has space.com", "-bad.example.com", "a..b.com"]
)
def test_rejects_bad_domains(tmp_path: Path, domain: str) -> None:
    with pytest.raises(ValueError, match="hostname"):
        envgen.generate(tmp_path, domain=domain, email="a@b.io")
    assert not (tmp_path / ".env.prod").exists()


def test_accepts_sslip_hostnames(tmp_path: Path) -> None:
    envgen.generate(tmp_path, domain="152-70-1-2.sslip.io", email="a@b.io")


def test_rejects_bad_email(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="email"):
        envgen.generate(tmp_path, domain="x.example.com", email="nope")


def test_cli_command_writes_files_and_reports_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = ["gen-env", "--domain", "x.example.com", "--email", "a@b.io", "--dir", str(tmp_path)]

    assert cli.main(argv) == 0
    assert (tmp_path / ".env.prod").exists()
    assert cli.main(argv) == 1
    assert "already exists" in capsys.readouterr().err
