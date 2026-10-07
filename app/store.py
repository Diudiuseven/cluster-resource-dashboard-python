"""In-memory resource store.

Consistency rules:
- Idempotent / order-tolerant: per resource, only the event with the
  highest seq wins; duplicates and out-of-order deliveries are dropped.
  Deletes leave a tombstone seq so stale events cannot resurrect a
  resource. Because events for one resource are totally ordered by seq,
  the resulting view equals replaying the log in order.
- Derived parents: a parent with children always reports the worst
  child status (FAILED > DEGRADED > UNKNOWN > HEALTHY); a childless
  parent reports its own status. Derivation is recomputed inside every
  mutation, so any read between mutations is self-consistent.
"""
from __future__ import annotations

import threading

from .models import Resource, ResourceEvent, ResourceStatus, Snapshot

_SEVERITY = {
    ResourceStatus.FAILED: 3,
    ResourceStatus.DEGRADED: 2,
    ResourceStatus.UNKNOWN: 1,
    ResourceStatus.HEALTHY: 0,
}


class ResourceStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._resources: dict[str, Resource] = {}
        self._own_status: dict[str, ResourceStatus] = {}
        self._seq: dict[str, int] = {}  # includes tombstones for deletes
        self._children: dict[str, set[str]] = {}
        self._version = 0

    # ---- mutation -------------------------------------------------

    def load_snapshot(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._resources.clear()
            self._own_status.clear()
            self._seq.clear()
            self._children.clear()
            self._version = snapshot.version
            for res in snapshot.resources:
                self._seq[res.id] = snapshot.version
                self._own_status[res.id] = res.status
                self._resources[res.id] = res
                if res.parent_id:
                    self._children.setdefault(res.parent_id, set()).add(res.id)
            for parent_id in list(self._children):
                self._refresh(parent_id)

    def apply(self, event: ResourceEvent) -> bool:
        """Apply one event; returns True if the view changed."""
        with self._lock:
            rid = event.resource.id
            if event.seq <= self._seq.get(rid, 0):
                return False  # duplicate or out-of-order: ignore
            self._seq[rid] = event.seq
            self._version = max(self._version, event.seq)

            old = self._resources.get(rid)
            old_parent = old.parent_id if old else None

            if event.type.value == "deleted":
                self._resources.pop(rid, None)
                self._own_status.pop(rid, None)
                if old_parent:
                    self._detach(old_parent, rid)
                # also orphan any children pointing at the deleted resource
                for cid in self._children.pop(rid, ()):
                    child = self._resources.get(cid)
                    if child:
                        self._resources[cid] = Resource(
                            id=child.id, namespace=child.namespace,
                            kind=child.kind, name=child.name,
                            status=child.status, parent_id=None,
                            labels=child.labels,
                        )
                if old_parent:
                    self._refresh(old_parent)
                return True

            res = event.resource
            self._own_status[rid] = res.status
            self._resources[rid] = res
            if old_parent and old_parent != res.parent_id:
                self._detach(old_parent, rid)
            if res.parent_id:
                self._children.setdefault(res.parent_id, set()).add(rid)
            self._refresh(rid)
            if old_parent:
                self._refresh(old_parent)
            if res.parent_id:
                self._refresh(res.parent_id)
            return True

    # ---- queries --------------------------------------------------

    def get(self, resource_id: str) -> Resource | None:
        with self._lock:
            return self._resources.get(resource_id)

    def list_all(self) -> list[Resource]:
        with self._lock:
            return [self._resources[k] for k in sorted(self._resources)]

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    # ---- internals ------------------------------------------------

    def _detach(self, parent_id: str, child_id: str) -> None:
        kids = self._children.get(parent_id)
        if kids:
            kids.discard(child_id)
            if not kids:
                del self._children[parent_id]

    def _refresh(self, rid: str) -> None:
        """Recompute rid's effective status from its children."""
        res = self._resources.get(rid)
        if res is None:
            return
        kids = self._children.get(rid)
        if not kids:
            effective = self._own_status[rid]
        else:
            effective = max(
                (self._resources[c].status for c in kids if c in self._resources),
                key=lambda s: _SEVERITY[s],
                default=self._own_status[rid],
            )
        if res.status != effective:
            self._resources[rid] = Resource(
                id=res.id, namespace=res.namespace, kind=res.kind,
                name=res.name, status=effective, parent_id=res.parent_id,
                labels=res.labels,
            )
