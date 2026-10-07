import random

import pytest

from dashboard.models import (EventType, Resource, ResourceEvent, ResourceKey,
                              ResourceStatus)


def make_resource(ns: str, name: str, status=ResourceStatus.HEALTHY,
                  kind="Pod", parent=None, **spec) -> Resource:
    return Resource(ResourceKey(ns, name), kind, parent, status, spec or {})


def make_event(type_: EventType, ns: str, name: str, seq: int,
               status=ResourceStatus.HEALTHY, kind="Pod", parent=None
               ) -> ResourceEvent:
    key = ResourceKey(ns, name)
    res = None if type_ == EventType.DELETED else make_resource(
        ns, name, status, kind, parent)
    return ResourceEvent(type_, key, seq=seq, resource=res)


@pytest.fixture
def resource_factory():
    return make_resource


@pytest.fixture
def event_factory():
    return make_event


def shuffled_with_duplicates(events: list, seed: int) -> list:
    """确定性乱序 + 重复投递序列。"""
    rng = random.Random(seed)
    out = []
    for ev in events:
        out.append(ev)
        if rng.random() < 0.3:  # 30% 重复投递
            out.append(ev)
    rng.shuffle(out)
    return out
