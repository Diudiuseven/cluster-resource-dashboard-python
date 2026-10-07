"""Deterministic, locally reproducible resource source.

Generates a snapshot and an append-only event log purely from a seed,
so tests always see identical data with no cluster or network access.
A bounded retention window simulates real sources that expire old
resource versions.
"""
from __future__ import annotations

import random

from .models import (
    EventType,
    Resource,
    ResourceEvent,
    ResourceStatus,
    Snapshot,
    VersionExpiredError,
)

_NAMESPACES = ("default", "prod", "staging", "kube-system")
_KINDS = ("Deployment", "Pod", "Service", "ConfigMap")
_STATUSES = tuple(ResourceStatus)


class ReplayableSource:
    """A replayable in-memory source.

    seq numbering: the snapshot is taken at version `num_events_base`
    (0 by default); each subsequent event increments the version by 1.
    Only the last `retention` events are kept; asking for an older
    version raises VersionExpiredError.
    """

    def __init__(
        self,
        seed: int = 42,
        num_resources: int = 50,
        num_events: int = 200,
        retention: int = 1000,
    ) -> None:
        rng = random.Random(seed)
        resources: list[Resource] = []
        for i in range(num_resources):
            kind = rng.choice(_KINDS)
            parent_id = None
            if kind == "Pod" and i > 0:
                # Pods hang under an earlier Deployment to form a hierarchy.
                parents = [r for r in resources if r.kind == "Deployment"]
                if parents:
                    parent_id = rng.choice(parents).id
            resources.append(
                Resource(
                    id=f"res-{i:04d}",
                    namespace=rng.choice(_NAMESPACES),
                    kind=kind,
                    name=f"{kind.lower()}-{i:04d}",
                    status=rng.choice(_STATUSES),
                    parent_id=parent_id,
                    labels={"seed": str(seed)},
                )
            )
        self._snapshot = Snapshot(version=0, resources=tuple(resources))

        log: list[ResourceEvent] = []
        live = {r.id: r for r in resources}
        seq = 0
        for i in range(num_events):
            seq += 1
            roll = rng.random()
            if roll < 0.2 or not live:
                # ADDED: brand-new resource.
                idx = num_resources + i
                kind = rng.choice(_KINDS)
                res = Resource(
                    id=f"res-{idx:04d}",
                    namespace=rng.choice(_NAMESPACES),
                    kind=kind,
                    name=f"{kind.lower()}-{idx:04d}",
                    status=rng.choice(_STATUSES),
                )
                log.append(ResourceEvent(seq=seq, type=EventType.ADDED, resource=res))
                live[res.id] = res
            elif roll < 0.7:
                # MODIFIED: change status of an existing resource.
                rid = rng.choice(sorted(live))
                old = live[rid]
                res = Resource(
                    id=old.id,
                    namespace=old.namespace,
                    kind=old.kind,
                    name=old.name,
                    status=rng.choice(_STATUSES),
                    parent_id=old.parent_id,
                    labels=old.labels,
                )
                log.append(ResourceEvent(seq=seq, type=EventType.MODIFIED, resource=res))
                live[rid] = res
            else:
                # DELETED: remove an existing resource.
                rid = rng.choice(sorted(live))
                log.append(ResourceEvent(seq=seq, type=EventType.DELETED, resource=live.pop(rid)))
        self._retention = retention
        self._log = log[-retention:] if retention >= 0 else list(log)
        self._total_events = seq

    def snapshot(self) -> Snapshot:
        return self._snapshot

    @property
    def current_version(self) -> int:
        return self._total_events

    @property
    def min_available_version(self) -> int:
        """Oldest version from which a complete event stream is retained."""
        if not self._log:
            return self._total_events
        return self._log[0].seq - 1

    def events_from(self, version: int) -> list[ResourceEvent]:
        if version < self.min_available_version:
            raise VersionExpiredError(
                f"version {version} expired; oldest available is "
                f"{self.min_available_version}"
            )
        return [e for e in self._log if e.seq > version]
