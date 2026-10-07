"""FastAPI routes for the dashboard.

Caller scope is supplied via headers (a stand-in for real auth):
  X-Namespaces: prod,staging   (absent/empty = all namespaces)
  X-Kinds: Pod,Deployment      (absent/empty = all kinds)
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .models import Scope
from .service import DashboardService, EventMessage, SyncMessage


def _scope_from(request: Request) -> Scope:
    ns = request.headers.get("x-namespaces", "").strip()
    kinds = request.headers.get("x-kinds", "").strip()
    return Scope(
        namespaces=frozenset(ns.split(",")) if ns else frozenset(),
        kinds=frozenset(kinds.split(",")) if kinds else frozenset(),
    )


def _resource_json(r) -> dict:
    return {
        "id": r.id,
        "namespace": r.namespace,
        "kind": r.kind,
        "name": r.name,
        "status": r.status.value,
        "parent_id": r.parent_id,
        "labels": dict(r.labels),
    }


def create_app(service: DashboardService, pump_interval: float | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        await service.start()
        task = None
        if pump_interval is not None:
            task = asyncio.create_task(service.run_background_pump(pump_interval))
        yield
        await service.stop()
        if task:
            task.cancel()

    app = FastAPI(title="Cluster Resource Dashboard", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "version": service.version}

    @app.get("/resources")
    async def list_resources(
        request: Request,
        namespace: str | None = Query(default=None),
        kind: str | None = Query(default=None),
        sort_by: str = Query(default="name"),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict:
        try:
            items, total, version = service.list_resources(
                _scope_from(request), namespace=namespace, kind=kind,
                sort_by=sort_by, offset=offset, limit=limit,
            )
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"detail": str(exc)})
        return {
            "items": [_resource_json(r) for r in items],
            "total": total,
            "version": version,
        }

    @app.get("/resources/{resource_id}")
    async def get_resource(request: Request, resource_id: str) -> dict:
        res = service.get_resource(resource_id, _scope_from(request))
        if res is None:
            return JSONResponse(status_code=404, content={"detail": "not found"})
        return _resource_json(res)

    @app.get("/watch")
    async def watch(request: Request, since: int | None = Query(default=None)):
        scope = _scope_from(request)

        async def stream() -> AsyncGenerator[str, None]:
            async for msg in service.watch(scope, since=since):
                if isinstance(msg, SyncMessage):
                    payload = {
                        "type": "sync",
                        "version": msg.version,
                        "resources": [_resource_json(r) for r in msg.resources],
                    }
                else:
                    ev = msg.event
                    payload = {
                        "type": ev.type.value,
                        "seq": ev.seq,
                        "resource": _resource_json(ev.resource),
                    }
                yield f"data: {json.dumps(payload)}\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream")

    return app
