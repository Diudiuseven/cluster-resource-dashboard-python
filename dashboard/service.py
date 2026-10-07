"""看板服务：装配存储、订阅中枢与授权，提供查询与连续读取。"""
from __future__ import annotations

from collections.abc import AsyncIterator

from .auth import AuthScope
from .hub import Subscription, SubscriptionClosed, SubscriptionHub
from .models import (EventType, Resource, ResourceEvent, ResourceKey,
                     ResourceStatus)
from .store import ResourceStore, ResourceVersionTooOldError

_SORT_KEYS = {
    "namespace": lambda r: (r.key.namespace, r.key.name),
    "name": lambda r: (r.key.name, r.key.namespace),
    "kind": lambda r: (r.kind, r.key.namespace, r.key.name),
    "status": lambda r: (r.status.value, r.key.namespace, r.key.name),
}


class DashboardService:
    """对外一致性语义：

    - list/get 在授权过滤后计算，分页/聚合不泄露越权资源；
    - 相同 rv 与相同参数下，list 结果逐条一致、可复现；
    - watch(since_rv) 先补发 rv 之后的存量事件再转实时，切换无断层；
      rv 过旧抛 ResourceVersionTooOldError，调用方须重新对齐。
    """

    def __init__(self, store: ResourceStore, hub: SubscriptionHub) -> None:
        self._store = store
        self._hub = hub

    # ---- 写入侧 ----

    def load_snapshot(self, resources: list[Resource]) -> int:
        """加载全量快照为 ADDED 事件序列，返回当前 rv。"""
        for r in resources:
            self.ingest(ResourceEvent(EventType.ADDED, r.key, seq=1, resource=r))
        return self._store.current_rv

    def ingest(self, event: ResourceEvent) -> bool:
        """应用一条事件并扇出；被判定为重复/乱序时不扇出。"""
        stored = self._store.apply(event)
        if stored is not None:
            self._hub.publish(stored)
            return True
        return False

    # ---- 查询侧 ----

    def list_resources(
        self,
        scope: AuthScope,
        namespaces: list[str] | None = None,
        kind: str | None = None,
        status: ResourceStatus | None = None,
        sort: str = "namespace",
        order: str = "asc",
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[Resource], int, int]:
        """过滤/排序/分页。返回 (items, total, rv)。

        total 为授权过滤后的总数，分页边界外与越权资源均不可见。
        """
        if namespaces:
            scope.check_namespaces(namespaces)  # 显式越权请求直接拒绝
        if sort not in _SORT_KEYS:
            raise ValueError(f"unsupported sort field: {sort}")
        items, rv = self._store.snapshot()
        items = scope.filter(items)
        if namespaces:
            allowed = set(namespaces)
            items = [r for r in items if r.key.namespace in allowed]
        if kind is not None:
            items = [r for r in items if r.kind == kind]
        if status is not None:
            items = [r for r in items if r.status == status]
        items.sort(key=_SORT_KEYS[sort], reverse=(order == "desc"))
        total = len(items)
        start = (page - 1) * page_size
        return items[start:start + page_size], total, rv

    def get_resource(self, scope: AuthScope, key: ResourceKey) -> Resource | None:
        """越权抛 ForbiddenError；不存在返回 None。"""
        scope.check(key)
        return self._store.get(key)

    # ---- 连续读取 ----

    def check_rv(self, since_rv: int) -> None:
        """预检 rv 是否已过期；过期抛 ResourceVersionTooOldError。"""
        self._store.list_events(since_rv)

    async def watch(self, scope: AuthScope,
                    since_rv: int | None = None) -> AsyncIterator[ResourceEvent]:
        """从指定 rv 连续读取事件（含授权过滤）。

        since_rv 为 None 时以当前快照版本为起点（只收之后的增量）。
        先补发存量、再转实时，重叠事件按 rv 去重，保证无断层。
        rv 过旧抛 ResourceVersionTooOldError。
        """
        sub = self._hub.subscribe()
        try:
            last_rv = since_rv if since_rv is not None else self._store.current_rv
            # 补发存量（可能抛 ResourceVersionTooOldError）
            for ev in self._store.list_events(last_rv):
                if scope.allows(ev.key):
                    yield ev
                last_rv = max(last_rv, ev.rv)
            # 转实时
            while True:
                try:
                    ev = await sub.next()
                except SubscriptionClosed:
                    raise  # 积压被断开，向上传播，调用方需重新对齐
                if ev.rv <= last_rv:
                    continue  # 与补发段重叠，去重
                last_rv = ev.rv
                if scope.allows(ev.key):
                    yield ev
        finally:
            self._hub.unsubscribe(sub)
