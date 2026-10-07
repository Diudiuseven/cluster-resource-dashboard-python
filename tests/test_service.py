"""Service: list/watch consistency, authz, version expiry, slow subscribers."""
import asyncio

import pytest

from app.models import Scope
from app.service import DashboardService, EventMessage, SyncMessage
from app.source import ReplayableSource

pytestmark = pytest.mark.asyncio


async def test_list_is_deterministic_and_paginates(service, full_scope):
    page1, total1, v1 = service.list_resources(full_scope, limit=20, offset=0)
    page2, total2, v2 = service.list_resources(full_scope, limit=20, offset=0)
    assert page1 == page2 and total1 == total2 and v1 == v2
    page3, _, _ = service.list_resources(full_scope, limit=20, offset=20)
    combined = page1 + page3
    assert len(combined) == len({r.id for r in combined})  # no overlap
    all_items, total, _ = service.list_resources(full_scope, limit=500)
    assert combined == all_items[:40] and total == len(all_items)


async def test_scope_hides_resources_and_totals_do_not_leak(service, prod_scope, full_scope):
    prod_items, prod_total, _ = service.list_resources(prod_scope, limit=500)
    all_items, all_total, _ = service.list_resources(full_scope, limit=500)
    assert all(r.namespace == "prod" for r in prod_items)
    assert prod_total == len(prod_items) < all_total
    # pagination cannot reach hidden resources
    page, total, _ = service.list_resources(prod_scope, limit=500, offset=prod_total)
    assert page == [] and total == prod_total
    # get on a hidden resource looks like "not found"
    hidden = next(r for r in all_items if r.namespace != "prod")
    assert service.get_resource(hidden.id, prod_scope) is None
    assert service.get_resource(hidden.id, full_scope) is not None


async def test_watch_from_scratch_starts_with_sync(service, full_scope):
    service.pump(5)
    agen = service.watch(full_scope)
    first = await asyncio.wait_for(agen.__anext__(), 1)
    assert isinstance(first, SyncMessage)
    assert first.version == service.version
    await agen.aclose()


async def test_list_to_watch_has_no_gap(service, full_scope):
    service.pump(10)
    _, _, v = service.list_resources(full_scope, limit=1)
    service.pump(5)  # events v+1 .. v+5 happen after the list
    agen = service.watch(full_scope, since=v)
    seen = []
    for _ in range(5):
        msg = await asyncio.wait_for(agen.__anext__(), 1)
        assert isinstance(msg, EventMessage)
        seen.append(msg.event.seq)
    assert seen == list(range(v + 1, v + 6))  # exactly the missed events
    await agen.aclose()


async def test_expired_version_triggers_resync_not_garbage(service, full_scope):
    service.pump(60)  # buffer only holds 50 -> version 0 expired
    assert service.min_available_version() > 0
    agen = service.watch(full_scope, since=0)
    first = await asyncio.wait_for(agen.__anext__(), 1)
    assert isinstance(first, SyncMessage)  # re-aligned, not inconsistent
    await agen.aclose()


async def test_watch_filters_by_scope(service):
    scope = Scope(namespaces=frozenset({"prod"}))
    service.pump(200)
    agen = service.watch(scope)
    msg = await asyncio.wait_for(agen.__anext__(), 1)
    assert isinstance(msg, SyncMessage)
    assert all(r.namespace == "prod" for r in msg.resources)
    await agen.aclose()


async def test_slow_subscriber_resyncs_without_blocking_others(service, full_scope):
    fast = service.watch(full_scope)
    slow = service.watch(full_scope)
    assert isinstance(await asyncio.wait_for(fast.__anext__(), 1), SyncMessage)
    assert isinstance(await asyncio.wait_for(slow.__anext__(), 1), SyncMessage)

    service.pump(50)  # overflow the slow subscriber's queue (size 10)
    # fast subscriber drains promptly and sees real events
    got_event = False
    for _ in range(20):
        msg = await asyncio.wait_for(fast.__anext__(), 1)
        if isinstance(msg, EventMessage):
            got_event = True
            break
    assert got_event
    # slow subscriber, once it reads, is re-aligned via a fresh SYNC
    got_sync = False
    for _ in range(20):
        msg = await asyncio.wait_for(slow.__anext__(), 1)
        if isinstance(msg, SyncMessage):
            got_sync = True
            break
    assert got_sync
    await fast.aclose()
    await slow.aclose()


async def test_subscribers_are_independent(service, full_scope):
    a = service.watch(full_scope)
    b = service.watch(full_scope)
    await asyncio.wait_for(a.__anext__(), 1)  # sync
    await asyncio.wait_for(b.__anext__(), 1)  # sync
    service.pump(3)
    # 'a' consumes everything; 'b' consumes nothing yet, then catches up
    seqs_a = []
    for _ in range(3):
        msg = await asyncio.wait_for(a.__anext__(), 1)
        seqs_a.append(msg.event.seq)
    seqs_b = []
    for _ in range(3):
        msg = await asyncio.wait_for(b.__anext__(), 1)
        seqs_b.append(msg.event.seq)
    assert seqs_a == seqs_b
    await a.aclose()
    await b.aclose()


async def test_reconnect_with_fresh_version_continues(service, full_scope):
    service.pump(10)
    agen = service.watch(full_scope)
    assert isinstance(await asyncio.wait_for(agen.__anext__(), 1), SyncMessage)
    service.pump(3)
    last_seq = None
    for _ in range(3):
        msg = await asyncio.wait_for(agen.__anext__(), 1)
        assert isinstance(msg, EventMessage)
        last_seq = msg.event.seq
    await agen.aclose()  # disconnect
    service.pump(3)
    # reconnect with last seen version: no gap, no duplicate
    agen2 = service.watch(full_scope, since=last_seq)
    seqs = []
    for _ in range(3):
        msg = await asyncio.wait_for(agen2.__anext__(), 1)
        assert isinstance(msg, EventMessage)
        seqs.append(msg.event.seq)
    assert seqs == list(range(last_seq + 1, last_seq + 4))
    await agen2.aclose()
