"""``aa.local.v2.Logs``: this Connector process's log tail.

Loguru records reach the service from whichever thread logged them; each
subscriber gets the recent ring first and then live records.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from meshcall import service, stream
from pydantic import BaseModel

from connector.mesh.events import EventHub, stream_items
from connector.mesh.hub import LOGS_SERVICE

# Recent records kept for a subscriber that arrives late.
LOG_HISTORY = 2000


class LogRecord(BaseModel):
    time: str
    level: str
    name: str
    message: str
    exception: str | None = None


class LogEntry(LogRecord):
    """One record with its position in this process's log stream."""

    seq: int


class LogHub:
    def __init__(self) -> None:
        self.hub: EventHub[LogRecord] = EventHub(history=LOG_HISTORY, capacity=512)

    def notify(self, _method: str, payload: dict[str, Any]) -> None:
        """Loguru sink: ``("connector/log", record)`` from any thread."""
        self.hub.publish("log", LogRecord.model_validate(payload))


@service(LOGS_SERVICE, description="Follow this Connector process's log.")
class LogsService:
    def __init__(self, logs: LogHub) -> None:
        self._logs = logs

    @stream
    async def tail(self, since: int | None = None) -> AsyncIterator[LogEntry]:
        """Records after since; a fresh stream starts with the recent history."""
        subscription = self._logs.hub.subscribe(since)
        async for event in stream_items(subscription):
            record = event.item
            yield LogEntry(seq=event.seq, **record.model_dump())
