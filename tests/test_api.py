"""API 层：越权拒绝、分页/排序可复现、实时监听、rv 过期重对齐。"""
import time

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from dashboard.api import WS_CLOSE_RESYNC
from dashboard.app import DashboardConfig, build_app

ALICE = {"X-Subject": "alice", "X-Namespaces": "ns-0"}


@pytest.fixture
def client():
    app = build_app(DashboardConfig(seed=42, num_events=0))
    with TestClient(app) as c:
        yield c


@pytest.fixture
def streaming_client():
    """带后台事件回放的应用。"""
    app = build_app(DashboardConfig(seed=42, num_events=10,
                                    event_interval=0.02))
    with TestClient(app) as c:
        yield c


# ---- 列表 ----

def test_list_resources_admin(client):
    body = client.get("/api/v1/resources").json()
    assert body["total"] == 24
    assert body["rv"] == 24
    assert len(body["items"]) == 24


def test_list_resources_scoped(client):
    body = client.get("/api/v1/resources", headers=ALICE).json()
    assert body["total"] == 8
    assert all(i["namespace"] == "ns-0" for i in body["items"])


def test_list_pagination_reproducible(client):
    params = {"sort": "name", "order": "asc", "page": 2, "page_size": 5}
    a = client.get("/api/v1/resources", params=params).json()
    b = client.get("/api/v1/resources", params=params).json()
    assert a == b
    assert len(a["items"]) == 5
    # 页之间不重叠
    p1 = client.get("/api/v1/resources",
                    params={**params, "page": 1}).json()
    assert {i["id"] for i in p1["items"]}.isdisjoint(
        {i["id"] for i in a["items"]})


def test_list_filter_by_namespace_and_kind(client):
    body = client.get("/api/v1/resources",
                      params={"namespaces": "ns-0,ns-1", "kind": "App"}).json()
    assert body["total"] == 4
    assert all(i["kind"] == "App" for i in body["items"])
    assert {i["namespace"] for i in body["items"]} <= {"ns-0", "ns-1"}


def test_list_explicit_forbidden_namespace_403(client):
    r = client.get("/api/v1/resources",
                   params={"namespaces": "ns-1"}, headers=ALICE)
    assert r.status_code == 403


def test_list_no_bypass_via_pagination(client):
    """受限调用方翻遍所有页也看不到越权资源。"""
    seen = []
    page = 1
    while True:
        body = client.get("/api/v1/resources", headers=ALICE,
                          params={"page": page, "page_size": 3}).json()
        seen.extend(body["items"])
        if len(seen) >= body["total"]:
            break
        page += 1
    assert len(seen) == 8
    assert all(i["namespace"] == "ns-0" for i in seen)


def test_list_bad_sort_400(client):
    r = client.get("/api/v1/resources", params={"sort": "bogus"})
    assert r.status_code == 400


# ---- 单资源 ----

def test_get_resource(client):
    body = client.get("/api/v1/resources/ns-0/app-0").json()
    assert body["id"] == "ns-0/app-0"
    assert body["kind"] == "App"


def test_get_resource_forbidden_403(client):
    r = client.get("/api/v1/resources/ns-1/app-0", headers=ALICE)
    assert r.status_code == 403


def test_get_resource_not_found_404(client):
    r = client.get("/api/v1/resources/ns-0/ghost")
    assert r.status_code == 404


def test_parent_status_derived_consistently(client):
    """父状态由子状态派生：读取时父子自洽。"""
    children = client.get("/api/v1/resources",
                          params={"kind": "Pod"}).json()["items"]
    by_parent = {}
    for ch in children:
        by_parent.setdefault(ch["parent"], []).append(ch["status"])
    for parent_id, kid_statuses in by_parent.items():
        parent = client.get(f"/api/v1/resources/{parent_id}").json()
        if "Failed" in kid_statuses:
            assert parent["status"] == "Failed"
        elif "Degraded" in kid_statuses:
            assert parent["status"] == "Degraded"


# ---- 实时监听 ----

def test_watch_receives_live_events(streaming_client):
    rv = streaming_client.get("/api/v1/resources").json()["rv"]
    with streaming_client.websocket_connect(
            f"/api/v1/watch?since_rv={rv}") as ws:
        events = [ws.receive_json() for _ in range(3)]
    rvs = [e["rv"] for e in events]
    assert rvs == sorted(rvs) and all(r > rv for r in rvs)


def test_watch_scoped_only_sees_own_namespace(streaming_client):
    time.sleep(0.3)  # 等回放结束，事件全部进入历史
    svc = streaming_client.app.state.service
    from dashboard.auth import AuthScope
    scope = AuthScope("alice", frozenset({"ns-0"}))
    expected = [e for e in svc._store.list_events(0) if scope.allows(e.key)]
    assert expected, "fixture should produce ns-0 events"
    with streaming_client.websocket_connect(
            "/api/v1/watch?since_rv=0", headers=ALICE) as ws:
        events = [ws.receive_json() for _ in expected]
    assert all(e["namespace"] == "ns-0" for e in events)
    assert [e["rv"] for e in events] == [e.rv for e in expected]


def test_watch_rv_too_old_close_code():
    app = build_app(DashboardConfig(num_events=0, max_history=2))
    with TestClient(app) as c:
        svc = app.state.service
        from dashboard.models import EventType, ResourceEvent
        from tests.conftest import make_resource
        for i in range(5):
            svc.ingest(ResourceEvent(
                EventType.ADDED, make_resource("n", f"r{i}").key, seq=1,
                resource=make_resource("n", f"r{i}")))
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with c.websocket_connect("/api/v1/watch?since_rv=1"):
                pass
        assert exc_info.value.code == WS_CLOSE_RESYNC


def test_watch_reconnect_continues(streaming_client):
    """断开后携带最后 rv 重连，无断层。"""
    rv0 = streaming_client.get("/api/v1/resources").json()["rv"]
    with streaming_client.websocket_connect(
            f"/api/v1/watch?since_rv={rv0}") as ws:
        first = [ws.receive_json() for _ in range(2)]
    with streaming_client.websocket_connect(
            f"/api/v1/watch?since_rv={first[-1]['rv']}") as ws:
        second = [ws.receive_json() for _ in range(2)]
    rvs = [e["rv"] for e in first + second]
    assert rvs == list(range(rv0 + 1, rv0 + 5))
