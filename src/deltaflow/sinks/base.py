"""The contract between the consumer framework and a destination."""

from __future__ import annotations

from collections.abc import Sequence
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
