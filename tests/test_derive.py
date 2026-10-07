"""父级状态派生：聚合规则与多级一致性。"""
from dashboard.derive import derive_parent_status, reconcile_parents
from dashboard.models import ResourceStatus
from tests.conftest import make_resource


def test_derive_worst_of_children():
    kids = [make_resource("n", "c1", ResourceStatus.HEALTHY),
            make_resource("n", "c2", ResourceStatus.DEGRADED)]
    assert derive_parent_status(kids) == ResourceStatus.DEGRADED
    kids.append(make_resource("n", "c3", ResourceStatus.FAILED))
    assert derive_parent_status(kids) == ResourceStatus.FAILED


def test_derive_all_healthy():
    kids = [make_resource("n", f"c{i}") for i in range(3)]
    assert derive_parent_status(kids) == ResourceStatus.HEALTHY


def test_reconcile_parents_updates_parent():
    parent = make_resource("n", "p", ResourceStatus.HEALTHY, kind="App")
    child = make_resource("n", "c", ResourceStatus.FAILED, parent="n/p")
    res = {r.id: r for r in (parent, child)}
    reconcile_parents(res)
    assert res["n/p"].status == ResourceStatus.FAILED
    assert res["n/c"].status == ResourceStatus.FAILED  # 子级不变


def test_reconcile_multi_level_bottom_up():
    grand = make_resource("n", "g", ResourceStatus.HEALTHY, kind="App")
    parent = make_resource("n", "p", ResourceStatus.HEALTHY, kind="App",
                           parent="n/g")
    child = make_resource("n", "c", ResourceStatus.DEGRADED, parent="n/p")
    res = {r.id: r for r in (grand, parent, child)}
    reconcile_parents(res)
    assert res["n/p"].status == ResourceStatus.DEGRADED
    assert res["n/g"].status == ResourceStatus.DEGRADED  # 逐级向上传播


def test_reconcile_missing_parent_tolerated():
    orphan = make_resource("n", "o", ResourceStatus.HEALTHY, parent="n/ghost")
    res = {orphan.id: orphan}
    reconcile_parents(res)  # 不抛异常
    assert res["n/o"].status == ResourceStatus.HEALTHY
