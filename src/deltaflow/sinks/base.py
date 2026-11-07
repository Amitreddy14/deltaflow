"""The contract between the consumer framework and a destination."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from deltaflow.events import ChangeEvent


class SinkError(RuntimeError):
    """The sink rejected events permanently. Retrying the same events will not help."""


class RetryableSinkError(SinkError):
    """A transient failure (destination down, overloaded). The same batch may succeed later."""


class Sink(Protocol):
    def write(self, events: Sequence[ChangeEvent]) -> None:
        """Apply events, in order. Must be idempotent: the same batch may arrive again.

        Returns normally only when every event is durably applied, or provably already
        superseded by newer data. Raises SinkError (or RetryableSinkError) otherwise.
        """
        ...


class TruncateWatermarks:
    """Per-table "nothing older than this LSN may exist" marks, set by TRUNCATE events.

    A TRUNCATE has no row key, so Kafka may place it in a different partition than the row
    events before it. After a replay or restart an older row event can therefore be read
    *after* the truncate. The mark makes the outcome independent of that arrival order: any
    row event older than the table's truncate is superseded, exactly as if it had been applied
    and then wiped.
    """

    def __init__(self) -> None:
        self._marks: dict[str, int] = {}

    def load(self, marks: Mapping[str, int]) -> None:
        for table, lsn in marks.items():
            self.advance(table, lsn)

    def advance(self, table: str, lsn: int) -> None:
        self._marks[table] = max(self._marks.get(table, 0), lsn)

    def superseded(self, event: ChangeEvent) -> bool:
        return event.lsn < self._marks.get(event.table_name, 0)

    def __len__(self) -> int:
        return len(self._marks)
