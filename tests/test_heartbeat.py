from pathlib import Path

from deltaflow import runners


def test_no_path_means_no_heartbeat() -> None:
    assert runners.heartbeat_writer(None) is None
    assert runners.heartbeat_writer("") is None


def test_beat_creates_and_refreshes_the_file(tmp_path: Path) -> None:
    target = tmp_path / "heartbeat"
    beat = runners.heartbeat_writer(str(target))
    assert beat is not None

    beat()
    first = target.stat().st_mtime_ns
    beat()

    assert target.exists() and target.stat().st_mtime_ns >= first


def test_an_unwritable_path_never_crashes_the_worker(tmp_path: Path) -> None:
    beat = runners.heartbeat_writer(str(tmp_path / "missing-dir" / "heartbeat"))
    assert beat is not None

    beat()  # logs once, does not raise
    beat()
