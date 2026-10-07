import pytest

from app.models import (
    EventType,
    Resource,
    ResourceEvent,
    ResourceStatus,
    Scope,
    Snapshot,
)
from app.service import DashboardService
from app.source import ReplayableSource


def make_resource(rid, status=ResourceStatus.HEALTHY, parent=None,
                  namespace="prod", kind="Pod", name=None):
    return Resource(
        id=rid, namespace=namespace, kind=kind, name=name or rid,
        status=status, parent_id=parent,
    )


def make_event(seq, etype, resource):
    return ResourceEvent(seq=seq, type=etype, resource=resource)


@pytest.fixture
def source():
    return ReplayableSource(seed=42, num_resources=50, num_events=200)


@pytest.fixture
async def service(source):
    svc = DashboardService(source, max_queue_size=10, event_buffer_size=50)
    await svc.start()
    yield svc
    await svc.stop()


@pytest.fixture
def full_scope():
    return Scope()


@pytest.fixture
def prod_scope():
    return Scope(namespaces=frozenset({"prod"}))
