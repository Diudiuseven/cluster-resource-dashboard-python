"""核心数据模型：资源、事件、版本。仅类型定义，不含业务逻辑。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    SYNC = "SYNC"  # 快照对齐标记


class ResourceStatus(str, Enum):
    HEALTHY = "Healthy"
    DEGRADED = "Degraded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class ResourceKey:
    """资源的唯一标识：命名空间 + 名称。"""

    namespace: str
    name: str

    def id(self) -> str:
        return f"{self.namespace}/{self.name}"


@dataclass
class Resource:
    """一份资源视图条目。rv 为全局单调递增的资源版本。"""

    key: ResourceKey
    kind: str
    parent: str | None  # 父资源 id（"ns/name"），无父级为 None
    status: ResourceStatus
    spec: dict[str, Any] = field(default_factory=dict)
    rv: int = 0

    @property
    def id(self) -> str:
        return self.key.id()


@dataclass(frozen=True)
class ResourceEvent:
    """一条变更事件。

    seq 为源端为该资源分配的单调递增序号（幂等/乱序判定依据）；
    rv 为事件写入存储后的全局资源版本（由存储赋值）。
    """

    type: EventType
    key: ResourceKey
    seq: int
    rv: int = 0
    resource: Resource | None = None  # DELETED 事件可为 None
