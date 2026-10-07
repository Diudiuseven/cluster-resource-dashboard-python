"""应用装配：配置、依赖组装、后台事件回放。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from .api import create_router
from .hub import SubscriptionHub
from .service import DashboardService
from .source import LocalDataSource
from .store import ResourceStore


@dataclass(frozen=True)
class DashboardConfig:
    """可配置项（均有默认值，测试环境用相同 seed 可复现结果）。"""

    seed: int = 42                 # 数据源随机种子
    namespaces: int = 3            # 生成的命名空间数
    parents_per_ns: int = 2        # 每命名空间的父资源数
    children_per_parent: int = 3   # 每父资源的子资源数
    num_events: int = 50           # 回放的事件总数
    event_interval: float = 0.2    # 事件回放间隔（秒），0 表示尽快
    max_history: int = 1024        # 事件历史保留条数（rv 压缩窗口）
    max_queue_per_subscriber: int = 256  # 单订阅者缓冲队列上限


def build_app(config: DashboardConfig | None = None) -> FastAPI:
    cfg = config or DashboardConfig()
    store = ResourceStore(max_history=cfg.max_history)
    hub = SubscriptionHub(max_queue_per_subscriber=cfg.max_queue_per_subscriber)
    service = DashboardService(store, hub)
    source = LocalDataSource(seed=cfg.seed, namespaces=cfg.namespaces,
                             parents_per_ns=cfg.parents_per_ns,
                             children_per_parent=cfg.children_per_parent,
                             num_events=cfg.num_events)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.load_snapshot(source.snapshot())
        task = None
        if cfg.num_events > 0:
            async def replay():
                for ev in source.events():
                    service.ingest(ev)
                    if cfg.event_interval > 0:
                        await asyncio.sleep(cfg.event_interval)
            task = asyncio.create_task(replay())
        yield
        if task is not None:
            task.cancel()

    app = FastAPI(title="cluster-resource-dashboard", lifespan=lifespan)
    app.state.config = cfg
    app.state.service = service
    app.include_router(create_router(service), prefix="/api/v1")
    return app
