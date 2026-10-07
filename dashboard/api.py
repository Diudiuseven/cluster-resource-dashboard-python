"""HTTP/WebSocket API：列表（过滤/分页/排序）、单资源查询、实时监听。

鉴权方式（本地可复现，不依赖外部系统）：
- 请求头 X-Subject 标识调用方（缺省为 anonymous）；
- 请求头 X-Namespaces 为逗号分隔的授权命名空间；缺省表示管理员（全部可见）。

实时监听使用 WebSocket（GET /api/v1/watch）：
- 查询参数 since_rv 指定起始资源版本；过期时服务端以自定义关闭码
  4100（resync required）关闭连接，客户端须重新对齐；
- 订阅积压被断开时以关闭码 4101 通知，语义同上。
"""
from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Query, Request, WebSocket
from fastapi.responses import JSONResponse

from .auth import AuthScope, ForbiddenError
from .hub import SubscriptionClosed
from .models import Resource, ResourceEvent, ResourceKey, ResourceStatus
from .service import DashboardService
from .store import ResourceVersionTooOldError

# WebSocket 自定义关闭码
WS_CLOSE_RESYNC = 4100        # rv 过期，需要重新对齐
WS_CLOSE_OVERLOADED = 4101    # 订阅积压被断开，需要重新对齐

ScopeResolver = Callable[[str, str | None], AuthScope]


def default_scope_resolver(subject: str, namespaces: str | None) -> AuthScope:
    ns = None
    if namespaces:
        ns = frozenset(n.strip() for n in namespaces.split(",") if n.strip())
    return AuthScope(subject=subject, namespaces=ns)


def resource_to_dict(r: Resource) -> dict:
    return {
        "namespace": r.key.namespace,
        "name": r.key.name,
        "id": r.id,
        "kind": r.kind,
        "parent": r.parent,
        "status": r.status.value,
        "spec": r.spec,
        "rv": r.rv,
    }


def event_to_dict(e: ResourceEvent) -> dict:
    return {
        "type": e.type.value,
        "namespace": e.key.namespace,
        "name": e.key.name,
        "rv": e.rv,
        "resource": resource_to_dict(e.resource) if e.resource else None,
    }


def create_router(service: DashboardService,
                  scope_resolver: ScopeResolver = default_scope_resolver
                  ) -> APIRouter:
    router = APIRouter()

    def resolve_scope(headers) -> AuthScope:
        return scope_resolver(
            headers.get("x-subject", "anonymous"),
            headers.get("x-namespaces"),
        )

    @router.get("/resources")
    def list_resources(
        request: Request,
        namespaces: str | None = Query(default=None),
        kind: str | None = Query(default=None),
        status: ResourceStatus | None = Query(default=None),
        sort: str = Query(default="namespace"),
        order: str = Query(default="asc", pattern="^(asc|desc)$"),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=500),
    ):
        scope = resolve_scope(request.headers)
        ns_list = ([n.strip() for n in namespaces.split(",") if n.strip()]
                   if namespaces else None)
        try:
            items, total, rv = service.list_resources(
                scope, namespaces=ns_list, kind=kind, status=status,
                sort=sort, order=order, page=page, page_size=page_size)
        except ForbiddenError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=403)
        except ValueError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=400)
        return {
            "items": [resource_to_dict(r) for r in items],
            "total": total,
            "page": page,
            "page_size": page_size,
            "rv": rv,
        }

    @router.get("/resources/{namespace}/{name}")
    def get_resource(request: Request, namespace: str, name: str):
        scope = resolve_scope(request.headers)
        key = ResourceKey(namespace, name)
        try:
            res = service.get_resource(scope, key)
        except ForbiddenError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=403)
        if res is None:
            return JSONResponse({"detail": "not found"}, status_code=404)
        return resource_to_dict(res)

    @router.websocket("/watch")
    async def watch(websocket: WebSocket,
                    since_rv: int | None = Query(default=None, ge=0)):
        scope = resolve_scope(websocket.headers)
        if since_rv is not None:
            try:
                service.check_rv(since_rv)
            except ResourceVersionTooOldError:
                # 不接受连接即无法发送应用层消息；用关闭码告知需重新对齐
                await websocket.close(code=WS_CLOSE_RESYNC)
                return
        await websocket.accept()
        try:
            async for ev in service.watch(scope, since_rv):
                await websocket.send_json(event_to_dict(ev))
        except ResourceVersionTooOldError:
            await websocket.close(code=WS_CLOSE_RESYNC)
        except SubscriptionClosed:
            # 订阅积压被断开：通知客户端重新对齐
            await websocket.close(code=WS_CLOSE_OVERLOADED)

    return router
