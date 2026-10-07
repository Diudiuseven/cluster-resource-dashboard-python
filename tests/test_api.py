"""HTTP API: scoped listing, no cross-endpoint leaks, SSE watch."""
import json
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.api import create_app
from app.service import DashboardService
from app.source import ReplayableSource


def make_service(**kwargs):
    source = ReplayableSource(seed=42, num_resources=50, num_events=200)
    return DashboardService(source, **kwargs)


@pytest.fixture
def client():
    service = make_service()
    app = create_app(service)  # no background pump: tests pump manually
    with TestClient(app) as c:
        c.app_service = service
        yield c


class LiveServer:
    """Real uvicorn server in a thread (TestClient cannot stream SSE)."""

    def __init__(self, service, pump_interval=None):
        self.service = service
        app = create_app(service, pump_interval=pump_interval)
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.time() + 5
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.01)
        port = self.server.servers[0].sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=5)


def sse_lines(response, count):
    """Read `count` SSE data payloads from a streaming response."""
    out = []
    for line in response.iter_lines():
        if line.startswith("data: "):
            out.append(json.loads(line[len("data: "):]))
            if len(out) == count:
                return out
    raise AssertionError(f"stream ended after {len(out)} messages")


def test_list_returns_consistent_version_and_items(client):
    r1 = client.get("/resources").json()
    r2 = client.get("/resources").json()
    assert r1 == r2
    assert r1["total"] == 50
    assert len(r1["items"]) == 50


def test_pagination_is_reproducible_and_complete(client):
    p1 = client.get("/resources", params={"limit": 20, "offset": 0}).json()
    p2 = client.get("/resources", params={"limit": 20, "offset": 20}).json()
    p3 = client.get("/resources", params={"limit": 20, "offset": 40}).json()
    ids = [i["id"] for p in (p1, p2, p3) for i in p["items"]]
    assert len(ids) == len(set(ids)) == 50
    again = client.get("/resources", params={"limit": 20, "offset": 0}).json()
    assert again == p1


def test_scope_header_filters_and_never_leaks(client):
    scoped = client.get("/resources", headers={"X-Namespaces": "prod"}).json()
    assert scoped["total"] < 50
    assert all(i["namespace"] == "prod" for i in scoped["items"])
    # beyond-the-end pagination reveals nothing
    beyond = client.get(
        "/resources",
        params={"offset": scoped["total"], "limit": 500},
        headers={"X-Namespaces": "prod"},
    ).json()
    assert beyond["items"] == []
    # direct get of a hidden resource is 404, indistinguishable from missing
    full = client.get("/resources", params={"limit": 500}).json()
    hidden = next(i for i in full["items"] if i["namespace"] != "prod")
    assert client.get(f"/resources/{hidden['id']}",
                      headers={"X-Namespaces": "prod"}).status_code == 404
    assert client.get(f"/resources/{hidden['id']}").status_code == 200


def test_kind_filter_and_sort(client):
    r = client.get("/resources", params={"kind": "Pod", "sort_by": "namespace",
                                         "limit": 500}).json()
    assert all(i["kind"] == "Pod" for i in r["items"])
    keys = [(i["namespace"], i["name"], i["id"]) for i in r["items"]]
    assert keys == sorted(keys)
    assert client.get("/resources", params={"sort_by": "bogus"}).status_code == 422


def test_watch_emits_sync_then_live_events():
    service = make_service()
    with LiveServer(service, pump_interval=0.02) as srv:
        with httpx.stream("GET", srv.base_url + "/watch", timeout=5) as resp:
            assert resp.status_code == 200
            msgs = sse_lines(resp, 3)
    assert msgs[0]["type"] == "sync"
    assert msgs[0]["version"] >= 0
    assert len(msgs[0]["resources"]) > 0
    assert msgs[1]["type"] in ("added", "modified", "deleted")
    assert msgs[2]["seq"] == msgs[1]["seq"] + 1  # continuous stream


def test_watch_resyncs_on_expired_version():
    service = make_service(event_buffer_size=10)
    with LiveServer(service) as srv:
        service.pump(30)  # buffer holds only the last 10 -> version 0 expired
        with httpx.stream("GET", srv.base_url + "/watch",
                          params={"since": 0}, timeout=5) as resp:
            (msg,) = sse_lines(resp, 1)
    assert msg["type"] == "sync"  # re-aligned, not an inconsistent stream
    assert msg["version"] == 30


def test_watch_resumes_from_list_version_without_gap():
    service = make_service()
    with LiveServer(service) as srv:
        service.pump(10)
        listed = httpx.get(srv.base_url + "/resources",
                           params={"limit": 1}).json()
        version = listed["version"]
        service.pump(4)
        with httpx.stream("GET", srv.base_url + "/watch",
                          params={"since": version}, timeout=5) as resp:
            msgs = sse_lines(resp, 4)
    assert [m["seq"] for m in msgs] == [version + 1, version + 2,
                                        version + 3, version + 4]
