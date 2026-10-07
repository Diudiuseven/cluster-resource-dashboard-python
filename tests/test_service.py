"""服务层：连续读取、重连对齐、授权过滤、分页排序可复现。"""
import asyncio

import pytest

from dashboard.auth import AuthScope, ForbiddenError
from dashboard.hub import SubscriptionHub
from dashboard.models import EventType, ResourceKey, ResourceStatus
from dashboard.service import DashboardService
from dashboard.store import ResourceStore, ResourceVersionTooOldError
from tests.conftest import make_event, make_resource

ADMIN = AuthScope("admin", None)
ALICE = AuthScope("alice", frozenset({"ns-0"}))


def make_service(max_history=1024, max_queue=256):
    store = ResourceStore(max_history=max_history)
    hub = SubscriptionHub(max_queue_per_subscriber=max_queue)
    return DashboardService(store, hub), store, hub


def seed_service(svc):
    svc.load_snapshot([
        make_resource("ns-0", "app-0", kind="App"),
        make_resource("ns-0", "pod-0", ResourceStatus.FAILED, parent="ns-0/app-0"),
        make_resource("ns-1", "app-0", kind="App"),
        make_resource("ns-1", "pod-0", parent="ns-1/app-0"),
    ])


# ---- 查询 ----

def test_list_filters_by_scope():
    svc, _, _ = make_service()
    seed_service(svc)
    items, total, _ = svc.list_resources(ALICE)
    assert total == 2
    assert all(r.key.namespace == "ns-0" for r in items)


def test_list_pagination_reproducible():
    svc, _, _ = make_service()
    svc.load_snapshot([make_resource("ns", f"r{i:02d}") for i in range(20)])
    args = dict(sort="name", order="desc", page=2, page_size=5)
    p1, total1, rv1 = svc.list_resources(ADMIN, **args)
    p2, total2, rv2 = svc.list_resources(ADMIN, **args)
    assert [r.id for r in p1] == [r.id for r in p2]
    assert (total1, rv1) == (total2, rv2) == (20, 20)
    assert [r.key.name for r in p1] == ["r14", "r13", "r12", "r11", "r10"]


def test_list_total_hides_out_of_scope():
    """total 也不得泄露越权资源数量。"""
    svc, _, _ = make_service()
    seed_service(svc)
    _, total_admin, _ = svc.list_resources(ADMIN)
    _, total_alice, _ = svc.list_resources(ALICE)
    assert total_admin == 4 and total_alice == 2


def test_list_explicit_forbidden_namespace_rejected():
    svc, _, _ = make_service()
    seed_service(svc)
    with pytest.raises(ForbiddenError):
        svc.list_resources(ALICE, namespaces=["ns-1"])


def test_get_resource_forbidden():
    svc, _, _ = make_service()
    seed_service(svc)
    with pytest.raises(ForbiddenError):
        svc.get_resource(ALICE, ResourceKey("ns-1", "app-0"))
    assert svc.get_resource(ALICE, ResourceKey("ns-0", "ghost")) is None


def test_parent_status_derived_on_read():
    """任一时刻读到的父子状态自洽：子 FAILED 则父 FAILED。"""
    svc, _, _ = make_service()
    seed_service(svc)
    parent = svc.get_resource(ADMIN, ResourceKey("ns-0", "app-0"))
    assert parent.status == ResourceStatus.FAILED
    # 子级恢复后父级同步恢复
    svc.ingest(make_event(EventType.MODIFIED, "ns-0", "pod-0", 2,
                          ResourceStatus.HEALTHY, parent="ns-0/app-0"))
    parent = svc.get_resource(ADMIN, ResourceKey("ns-0", "app-0"))
    assert parent.status == ResourceStatus.HEALTHY


# ---- 连续读取 ----

def test_watch_catchup_then_live_no_gap():
    async def main():
        svc, _, _ = make_service()
        seed_service(svc)  # rv 1..4
        gen = svc.watch(ADMIN, since_rv=2)
        got = []
        got.append(await gen.__anext__())  # 补发 rv 3
        got.append(await gen.__anext__())  # 补发 rv 4
        # 实时段
        svc.ingest(make_event(EventType.ADDED, "ns-0", "new", 1))
        got.append(await gen.__anext__())
        await gen.aclose()
        assert [e.rv for e in got] == [3, 4, 5]  # 连续无断层
    asyncio.run(main())


def test_watch_filters_by_scope():
    async def main():
        svc, _, _ = make_service()
        seed_service(svc)
        gen = svc.watch(ALICE, since_rv=0)
        got = [await gen.__anext__() for _ in range(2)]
        await gen.aclose()
        assert all(e.key.namespace == "ns-0" for e in got)
    asyncio.run(main())


def test_watch_reconnect_continues_without_gap():
    """断开后用最后收到的 rv 重连，事件连续无重复。"""
    async def main():
        svc, _, _ = make_service()
        seed_service(svc)
        # 第一次连接：收 2 条后断开
        gen = svc.watch(ADMIN, since_rv=0)
        first = [await gen.__anext__() for _ in range(2)]
        await gen.aclose()
        # 断开期间产生新事件
        svc.ingest(make_event(EventType.ADDED, "ns-1", "x", 1))
        # 重连：从最后收到的 rv 继续
        gen2 = svc.watch(ADMIN, since_rv=first[-1].rv)
        second = [await gen2.__anext__() for _ in range(3)]
        await gen2.aclose()
        rvs = [e.rv for e in first + second]
        assert rvs == [1, 2, 3, 4, 5]  # 无断层、无重复
    asyncio.run(main())


def test_watch_rv_too_old_raises():
    async def main():
        svc, _, _ = make_service(max_history=2)
        for i in range(5):
            svc.ingest(make_event(EventType.ADDED, "ns", f"r{i}", 1))
        with pytest.raises(ResourceVersionTooOldError):
            async for _ in svc.watch(ADMIN, since_rv=1):
                pass
    asyncio.run(main())


def test_watch_slow_subscriber_does_not_block_others():
    async def main():
        svc, _, hub = make_service(max_queue=4)
        fast_gen, slow_gen = svc.watch(ADMIN), svc.watch(ADMIN)
        first_fast = asyncio.create_task(fast_gen.__anext__())
        first_slow = asyncio.create_task(slow_gen.__anext__())
        # 让两个生成器推进到实时等待点（完成订阅）
        for _ in range(10):
            await asyncio.sleep(0)
            if hub.subscriber_count == 2:
                break
        assert hub.subscriber_count == 2
        svc.ingest(make_event(EventType.ADDED, "ns", "a", 1))
        await first_fast
        await first_slow
        # 快订阅者持续消费，慢订阅者停滞
        received = []
        for i in range(20):
            svc.ingest(make_event(EventType.MODIFIED, "ns", "a", i + 2))
            received.append(await asyncio.wait_for(fast_gen.__anext__(), 1))
        assert len(received) == 20  # 快订阅者完整接收、未被拖慢
        # 慢订阅者积压超限被断开：读完缓冲区后收到关闭信号
        from dashboard.hub import SubscriptionClosed
        with pytest.raises(SubscriptionClosed):
            while True:
                await asyncio.wait_for(slow_gen.__anext__(), 1)
        await fast_gen.aclose()
    asyncio.run(main())
