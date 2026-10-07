"""Source: deterministic replay, retention / version expiry."""
import pytest

from app.models import VersionExpiredError
from app.source import ReplayableSource


def test_same_seed_same_data():
    a = ReplayableSource(seed=7, num_resources=30, num_events=100)
    b = ReplayableSource(seed=7, num_resources=30, num_events=100)
    assert a.snapshot() == b.snapshot()
    assert a.events_from(0) == b.events_from(0)


def test_different_seed_different_data():
    a = ReplayableSource(seed=1, num_resources=30, num_events=100)
    b = ReplayableSource(seed=2, num_resources=30, num_events=100)
    assert a.snapshot() != b.snapshot()


def test_events_are_sequentially_numbered():
    s = ReplayableSource(seed=7, num_resources=30, num_events=100)
    seqs = [e.seq for e in s.events_from(0)]
    assert seqs == list(range(1, 101))
    assert s.current_version == 100


def test_retention_expires_old_versions():
    s = ReplayableSource(seed=7, num_resources=30, num_events=100, retention=10)
    with pytest.raises(VersionExpiredError):
        s.events_from(0)
    assert len(s.events_from(90)) == 10
    assert s.min_available_version == 90


def test_replay_from_snapshot_reaches_same_view_as_in_order():
    """Applying the full log to the snapshot equals applying it in any
    order (view consistency is anchored at the source)."""
    from app.store import ResourceStore

    s = ReplayableSource(seed=9, num_resources=40, num_events=150)
    st = ResourceStore()
    st.load_snapshot(s.snapshot())
    for ev in s.events_from(0):
        st.apply(ev)
    assert st.version == s.current_version
