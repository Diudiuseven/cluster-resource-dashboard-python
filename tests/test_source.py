"""数据源：确定性、可复现、不依赖外部。"""
from dashboard.source import LocalDataSource


def test_snapshot_deterministic():
    a = LocalDataSource(seed=7).snapshot()
    b = LocalDataSource(seed=7).snapshot()
    assert [(r.id, r.kind, r.parent, r.status) for r in a] == \
           [(r.id, r.kind, r.parent, r.status) for r in b]


def test_events_deterministic():
    a = [(e.type, e.key.id(), e.seq) for e in LocalDataSource(seed=7).events()]
    b = [(e.type, e.key.id(), e.seq) for e in LocalDataSource(seed=7).events()]
    assert a == b and len(a) == 50


def test_different_seeds_differ():
    a = [(e.type, e.key.id()) for e in LocalDataSource(seed=1).events()]
    b = [(e.type, e.key.id()) for e in LocalDataSource(seed=2).events()]
    assert a != b


def test_event_seq_monotonic_per_resource():
    """同一资源的事件 seq 单调递增（快照资源从 2 开始）。"""
    src = LocalDataSource(seed=3)
    seqs = {r.key: 1 for r in src.snapshot()}
    for ev in src.events():
        assert ev.seq > seqs.get(ev.key, 0)
        seqs[ev.key] = ev.seq


def test_hierarchy_shape():
    src = LocalDataSource(seed=42, namespaces=2, parents_per_ns=2,
                          children_per_parent=3)
    snap = src.snapshot()
    parents = [r for r in snap if r.kind == "App"]
    children = [r for r in snap if r.kind == "Pod"]
    assert len(parents) == 4 and len(children) == 12
    parent_ids = {p.id for p in parents}
    assert all(c.parent in parent_ids for c in children)
