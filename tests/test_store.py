"""存储层：幂等、乱序、重复、墓碑、rv 过期、与按序重建等价。"""
import pytest

from dashboard.models import EventType, ResourceKey, ResourceStatus
from dashboard.store import ResourceStore, ResourceVersionTooOldError
from tests.conftest import make_event, shuffled_with_duplicates


def build_events():
    """构造一组覆盖 新增/变更/删除 的事件序列（按序）。"""
    return [
        make_event(EventType.ADDED, "ns", "a", 1),
        make_event(EventType.ADDED, "ns", "b", 1),
        make_event(EventType.MODIFIED, "ns", "a", 2, ResourceStatus.DEGRADED),
        make_event(EventType.ADDED, "ns", "c", 1),
        make_event(EventType.DELETED, "ns", "b", 2),
        make_event(EventType.MODIFIED, "ns", "a", 3, ResourceStatus.FAILED),
        make_event(EventType.MODIFIED, "ns", "c", 2, ResourceStatus.UNKNOWN),
        make_event(EventType.DELETED, "ns", "c", 3),
    ]


def view_of(store: ResourceStore):
    resources, rv = store.snapshot()
    return {r.id: (r.status, r.kind) for r in resources}, rv


def test_in_order_apply():
    store = ResourceStore()
    for ev in build_events():
        store.apply(ev)
    view, rv = view_of(store)
    assert rv == 8
    assert set(view) == {"ns/a"}
    assert view["ns/a"][0] == ResourceStatus.FAILED


def test_out_of_order_and_duplicate_equals_in_order():
    """乱序 + 重复投递后的视图与按序应用完全一致。"""
    events = build_events()
    expected_store = ResourceStore()
    for ev in events:
        expected_store.apply(ev)
    expected, _ = view_of(expected_store)

    for seed in range(10):
        store = ResourceStore()
        for ev in shuffled_with_duplicates(events, seed):
            store.apply(ev)
        view, _ = view_of(store)
        assert view == expected, f"seed={seed} diverged: {view} vs {expected}"


def test_duplicate_event_not_applied_twice():
    store = ResourceStore()
    ev = make_event(EventType.ADDED, "ns", "a", 1)
    assert store.apply(ev) is not None
    assert store.apply(ev) is None  # 重复投递被丢弃
    assert store.current_rv == 1


def test_stale_event_after_delete_does_not_resurrect():
    store = ResourceStore()
    store.apply(make_event(EventType.ADDED, "ns", "a", 1))
    store.apply(make_event(EventType.DELETED, "ns", "a", 2))
    # 迟到的旧事件不得复活已删除资源
    assert store.apply(make_event(EventType.MODIFIED, "ns", "a", 1)) is None
    assert store.apply(make_event(EventType.ADDED, "ns", "a", 2)) is None
    resources, _ = store.snapshot()
    assert resources == []


def test_recreate_after_delete_with_newer_seq():
    store = ResourceStore()
    store.apply(make_event(EventType.ADDED, "ns", "a", 1))
    store.apply(make_event(EventType.DELETED, "ns", "a", 2))
    assert store.apply(make_event(EventType.ADDED, "ns", "a", 3)) is not None
    resources, _ = store.snapshot()
    assert [r.id for r in resources] == ["ns/a"]


def test_rv_monotonic_and_history():
    store = ResourceStore()
    for ev in build_events()[:3]:
        store.apply(ev)
    assert store.current_rv == 3
    assert [e.rv for e in store.list_events(0)] == [1, 2, 3]
    assert [e.rv for e in store.list_events(2)] == [3]
    assert store.list_events(3) == []


def test_rv_too_old_raises():
    store = ResourceStore(max_history=3)
    for i in range(6):
        store.apply(make_event(EventType.ADDED, "ns", f"r{i}", 1))
    with pytest.raises(ResourceVersionTooOldError):
        store.list_events(1)
    # 边界：最旧可用版本仍可读
    assert len(store.list_events(store.current_rv - 3)) == 3


def test_snapshot_rv_consistent():
    store = ResourceStore()
    store.apply(make_event(EventType.ADDED, "ns", "a", 1))
    _, rv1 = store.snapshot()
    _, rv2 = store.snapshot()
    assert rv1 == rv2 == 1
