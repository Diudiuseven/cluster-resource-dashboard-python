"""版本化资源存储：快照 + 增量事件，幂等、抗乱序。"""
from __future__ import annotations

import threading
from dataclasses import replace

from .derive import reconcile_parents
from .models import EventType, Resource, ResourceEvent, ResourceKey


class ResourceVersionTooOldError(Exception):
    """请求的资源版本已被压缩，调用方需重新对齐（重新取快照）。"""


class ResourceStore:
    """内存资源视图。

    一致性语义：
    - apply() 依据事件的源端序号 seq 判定：同一资源的旧序号事件（重复或乱序
      到达）被丢弃，因此任意顺序/重复投递后，视图与按序应用的结果一致。
    - 删除以墓碑记录序号，迟到的旧事件不会复活已删除资源。
    - 每次真实变更使全局 rv 单调 +1 并追加一条历史事件；历史长度有限，
      过旧的 rv 触发 ResourceVersionTooOldError。
    - 父资源状态在读取时由子资源派生，任一时刻读到的父子状态自洽。
    """

    def __init__(self, max_history: int = 1024) -> None:
        self._max_history = max_history
        self._resources: dict[ResourceKey, Resource] = {}
        self._seqs: dict[ResourceKey, int] = {}  # 存活资源已应用的最新 seq
        self._tombstones: dict[ResourceKey, int] = {}  # 已删除资源的墓碑 seq
        self._events: list[ResourceEvent] = []  # 已应用事件历史（按 rv 升序）
        self._rv: int = 0
        self._lock = threading.Lock()

    @property
    def current_rv(self) -> int:
        return self._rv

    def apply(self, event: ResourceEvent) -> ResourceEvent | None:
        """应用事件；返回写入历史的事件（rv 已赋值），被丢弃时返回 None。"""
        key = event.key
        with self._lock:
            tomb = self._tombstones.get(key)
            if tomb is not None and event.seq <= tomb:
                return None  # 已删除，迟到/重复事件
            cur_seq = self._seqs.get(key)
            if cur_seq is not None and event.seq <= cur_seq:
                return None  # 重复或乱序的旧事件

            if event.type == EventType.DELETED:
                self._resources.pop(key, None)
                self._seqs.pop(key, None)
                self._tombstones[key] = event.seq
            else:
                assert event.resource is not None
                self._resources[key] = event.resource
                self._seqs[key] = event.seq
                self._tombstones.pop(key, None)

            self._rv += 1
            stored = replace(event, rv=self._rv)
            self._events.append(stored)
            if len(self._events) > self._max_history:
                del self._events[: len(self._events) - self._max_history]
            return stored

    def _derived(self, resources: list[Resource]) -> list[Resource]:
        """读取时派生父级状态，保证层级自洽。"""
        by_id = {r.id: replace(r) for r in resources}
        reconcile_parents(by_id)
        return list(by_id.values())

    def snapshot(self) -> tuple[list[Resource], int]:
        """返回（派生后的资源列表, 当前 rv）。同一 rv 下结果可复现。"""
        with self._lock:
            rv = self._rv
            items = list(self._resources.values())
        return self._derived(items), rv

    def list_events(self, since_rv: int) -> list[ResourceEvent]:
        """返回 rv > since_rv 的事件；版本过旧抛 ResourceVersionTooOldError。"""
        with self._lock:
            if since_rv > self._rv:
                return []
            if self._events and since_rv < self._events[0].rv - 1:
                raise ResourceVersionTooOldError(
                    f"requested rv {since_rv} compacted, oldest available "
                    f"{self._events[0].rv - 1}"
                )
            return [e for e in self._events if e.rv > since_rv]

    def get(self, key: ResourceKey) -> Resource | None:
        with self._lock:
            res = self._resources.get(key)
            if res is None:
                return None
            derived = self._derived([res, *[r for r in self._resources.values()
                                           if r.parent == res.id]])
        return next(r for r in derived if r.key == key)
