"""Store: idempotent, order-tolerant event application; derived parents."""
import random

from app.models import EventType, ResourceStatus, Snapshot
from app.store import ResourceStore

from .conftest import make_event, make_resource

H, D, F, U = (ResourceStatus.HEALTHY, ResourceStatus.DEGRADED,
              ResourceStatus.FAILED, ResourceStatus.UNKNOWN)


def test_duplicate_events_are_ignored():
    st = ResourceStore()
    st.load_snapshot(Snapshot(version=0, resources=()))
    ev = make_event(1, EventType.ADDED, make_resource("a", H))
    assert st.apply(ev) is True
    assert st.apply(ev) is False
    assert st.apply(ev) is False
    assert len(st.list_all()) == 1
    assert st.version == 1


def test_out_of_order_events_keep_highest_seq():
    st = ResourceStore()
    st.load_snapshot(Snapshot(version=0, resources=(make_resource("a", H),)))
    st.apply(make_event(5, EventType.MODIFIED, make_resource("a", F)))
    # older events must not move the view backwards
    assert st.apply(make_event(3, EventType.MODIFIED, make_resource("a", D))) is False
    assert st.get("a").status == F
    assert st.version == 5


def test_delete_then_stale_add_does_not_resurrect():
    st = ResourceStore()
    st.load_snapshot(Snapshot(version=0, resources=(make_resource("a", H),)))
    st.apply(make_event(2, EventType.DELETED, make_resource("a", H)))
    assert st.get("a") is None
    assert st.apply(make_event(1, EventType.ADDED, make_resource("a", H))) is False
    assert st.get("a") is None


def test_shuffled_and_duplicated_log_equals_in_order_replay():
    base = (make_resource("p", H, kind="Deployment"),
            make_resource("c1", H, parent="p"),
            make_resource("c2", H, parent="p"))
    events = [
        make_event(1, EventType.MODIFIED, make_resource("c1", D, parent="p")),
        make_event(2, EventType.ADDED, make_resource("c3", F, parent="p")),
        make_event(3, EventType.MODIFIED, make_resource("c1", H, parent="p")),
        make_event(4, EventType.DELETED, make_resource("c2", H, parent="p")),
        make_event(5, EventType.MODIFIED, make_resource("c3", H, parent="p")),
        make_event(6, EventType.DELETED, make_resource("c3", H, parent="p")),
    ]
    ordered = ResourceStore()
    ordered.load_snapshot(Snapshot(version=0, resources=base))
    for e in events:
        ordered.apply(e)

    rng = random.Random(1234)
    for _ in range(20):
        shuffled = ResourceStore()
        shuffled.load_snapshot(Snapshot(version=0, resources=base))
        noisy = events + events  # duplicate every event
        rng.shuffle(noisy)
        for e in noisy:
            shuffled.apply(e)
        assert shuffled.list_all() == ordered.list_all()
        assert shuffled.version == ordered.version


def test_parent_status_derived_from_children():
    st = ResourceStore()
    st.load_snapshot(Snapshot(version=0, resources=(
        make_resource("p", H, kind="Deployment"),
        make_resource("c1", H, parent="p"),
        make_resource("c2", H, parent="p"),
    )))
    assert st.get("p").status == H
    st.apply(make_event(1, EventType.MODIFIED, make_resource("c1", D, parent="p")))
    assert st.get("p").status == D
    st.apply(make_event(2, EventType.MODIFIED, make_resource("c2", F, parent="p")))
    assert st.get("p").status == F
    # child recovers -> parent recovers accordingly
    st.apply(make_event(3, EventType.MODIFIED, make_resource("c2", H, parent="p")))
    assert st.get("p").status == D
    # deleting the degraded child leaves parent healthy
    st.apply(make_event(4, EventType.DELETED, make_resource("c1", D, parent="p")))
    assert st.get("p").status == H


def test_childless_parent_reports_own_status():
    st = ResourceStore()
    st.load_snapshot(Snapshot(version=0, resources=(
        make_resource("p", D, kind="Deployment"),)))
    assert st.get("p").status == D
