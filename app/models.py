"""Core domain types for the cluster resource dashboard."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Optional


class ResourceStatus(str, enum.Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"


class EventType(str, enum.Enum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    SYNC = "sync"  # marks a resync boundary in the event stream


@dataclass(frozen=True)
class Resource:
    """A single cluster resource."""

    id: str
    namespace: str
    kind: str
    name: str
    status: ResourceStatus
    parent_id: Optional[str] = None
    labels: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceEvent:
    """A change event. `seq` is the global monotonically increasing
    resource version assigned by the source."""

    seq: int
    type: EventType
    resource: Resource


@dataclass(frozen=True)
class Scope:
    """Authorization scope of a caller: which namespaces/kinds are visible."""

    namespaces: frozenset[str] = frozenset()  # empty = all namespaces
    kinds: frozenset[str] = frozenset()       # empty = all kinds


@dataclass(frozen=True)
class Snapshot:
    """A full snapshot plus the version it was taken at."""

    version: int
    resources: tuple[Resource, ...]


class VersionExpiredError(Exception):
    """Raised when a requested resource version is no longer available."""
