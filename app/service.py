"""Dashboard service: consistent list + watch on top of the store.

- list_resources filters by authorization scope *before* pagination, so
  hidden resources never leak through totals or pages.
- watch() gives each subscriber an independent bounded queue. A slow
  subscriber that overflows its queue receives a resync marker and
  re-aligns from a fresh snapshot; other subscribers are unaffected
  because publishing is non-blocking.
- A bounded event buffer bridges list->watch handoff: a watcher can
  resume from the version returned by an earlier list. If that version
  has expired from the buffer, the watcher gets a SYNC snapshot instead
  of a silently inconsistent stream.
"""
from __future__ import annotations

import asyncio
import threading
from collections import deque
from dataclasses import dataclass
from typing import AsyncGenerator, Union

from .authz import filter_visible, visible
from .models import Resource, ResourceEvent, Scope, VersionExpiredError
from .source import ReplayableSource
from .store import ResourceStore

_SORT_KEYS = {
    "name": lambda r: (r.name, r.id),
    "namespace": lambda r: (r.namespace, r.name, r.id),
    "kind": lambda r: (r.kind, r.name, r.id),
    "status": lambda r: (r.status.value, r.name, r.id),
    "id": lambda r: (r.id,),
}


@dataclass(frozen=True)
class SyncMessage:
    """Full re-alignment point: replace local view with these resources."""

    version: int
    resources: tuple[Resource, ...]


@dataclass(frozen=True)
class EventMessage:
    event: ResourceEvent


WatchMessage = Union[SyncMessage, EventMessage]
_RESYNC = object()  # queue sentinel: subscriber fell behind


class DashboardService:
    def __init__(
        self,
        source: ReplayableSource,
        max_queue_size: int = 100,
        event_buffer_size: int = 1000,
    ) -> None:
        self._source = source
        self._store = ResourceStore()
        self._max_queue_size = max_queue_size
        self._events: deque[ResourceEvent] = deque(maxlen=event_buffer_size)
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = threading.Lock()
        self._source_version = 0
        self._started = False

    # ---- lifecycle ------------------------------------------------

    async def start(self) -> None:
        snap = self._source.snapshot()
        self._store.load_snapshot(snap)
        self._source_version = snap.version
        self._started = True

    async def stop(self) -> None:
        self._started = False

    def pump(self, n: int = 1) -> int:
        """Pull up to n events from the source into the view.

        Stands in for a live upstream feed; call from the event-loop
        thread (tests) or the background pump task (server).
        """
        events = self._source.events_from(self._source_version)[:n]
        for ev in events:
            self._apply_and_broadcast(ev)
        if events:
            self._source_version = events[-1].seq
        return len(events)

    async def run_background_pump(self, interval: float = 0.05) -> None:
        while self._started:
            self.pump(n=10)
            await asyncio.sleep(interval)

    # ---- reads ----------------------------------------------------

    def list_resources(
        self,
        scope: Scope,
        namespace: str | None = None,
        kind: str | None = None,
        sort_by: str = "name",
        offset: int = 0,
        limit: int = 50,
    ) -> tuple[list[Resource], int, int]:
        if sort_by not in _SORT_KEYS:
            raise ValueError(f"unsupported sort_by: {sort_by}")
        items = list(filter_visible(self._store.list_all(), scope))
        if namespace is not None:
            items = [r for r in items if r.namespace == namespace]
        if kind is not None:
            items = [r for r in items if r.kind == kind]
        items.sort(key=_SORT_KEYS[sort_by])
        total = len(items)
        return items[offset : offset + limit], total, self._store.version

    def get_resource(self, resource_id: str, scope: Scope) -> Resource | None:
        res = self._store.get(resource_id)
        if res is None or not visible(res, scope):
            return None  # indistinguishable from non-existent: no leak
        return res

    @property
    def version(self) -> int:
        return self._store.version

    def min_available_version(self) -> int:
        with self._lock:
            if self._events:
                return self._events[0].seq - 1
            return self._store.version

    # ---- watch ----------------------------------------------------

    async def watch(
        self, scope: Scope, since: int | None = None
    ) -> AsyncGenerator[WatchMessage, None]:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue_size)
        with self._lock:
            if since is None or since < self._min_available_version_locked():
                # (Re)align from a snapshot; live events newer than the
                # snapshot version cannot be missed because we register
                # the subscriber under the same lock.
                snapshot = SyncMessage(
                    version=self._store.version,
                    resources=tuple(filter_visible(self._store.list_all(), scope)),
                )
                backlog: list[ResourceEvent] = []
            else:
                snapshot = None
                backlog = [e for e in self._events if e.seq > since]
            self._subscribers.add(queue)
        try:
            if snapshot is not None:
                yield snapshot
            for ev in backlog:
                if visible(ev.resource, scope):
                    yield EventMessage(ev)
            while True:
                item = await queue.get()
                if item is _RESYNC:
                    yield SyncMessage(
                        version=self._store.version,
                        resources=tuple(
                            filter_visible(self._store.list_all(), scope)
                        ),
                    )
                    continue
                if visible(item.resource, scope):
                    yield EventMessage(item)
        finally:
            with self._lock:
                self._subscribers.discard(queue)

    # ---- internals ------------------------------------------------

    def _min_available_version_locked(self) -> int:
        if self._events:
            return self._events[0].seq - 1
        return self._store.version

    def _apply_and_broadcast(self, event: ResourceEvent) -> None:
        changed = self._store.apply(event)
        if not changed:
            return
        with self._lock:
            self._events.append(event)
            for queue in self._subscribers:
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    # Slow subscriber: reset its queue and tell it to
                    # resync. Never block other subscribers.
                    while not queue.empty():
                        queue.get_nowait()
                    queue.put_nowait(_RESYNC)
