"""订阅中枢：多订阅者独立、慢订阅者隔离。"""
import asyncio

import pytest

from dashboard.hub import SubscriptionClosed, SubscriptionHub
from tests.conftest import make_event
from dashboard.models import EventType


def run(coro):
    return asyncio.run(coro)


def test_fanout_independent_progress():
    async def main():
        hub = SubscriptionHub()
        s1, s2 = hub.subscribe(), hub.subscribe()
        hub.publish(make_event(EventType.ADDED, "n", "a", 1, ))
        # s1 先消费，s2 后消费，互不影响
        assert (await s1.next()).key.name == "a"
        hub.publish(make_event(EventType.ADDED, "n", "b", 1))
        assert (await s1.next()).key.name == "b"
        assert (await s2.next()).key.name == "a"
        assert (await s2.next()).key.name == "b"
    run(main())


def test_slow_subscriber_isolated():
    async def main():
        hub = SubscriptionHub(max_queue_per_subscriber=4)
        fast, slow = hub.subscribe(), hub.subscribe()
        received = []
        for i in range(10):
            hub.publish(make_event(EventType.MODIFIED, "n", "x", i + 1))
            if not fast.closed:
                received.append((await fast.next()).seq)
        assert received == list(range(1, 11))  # 快订阅者不丢、不被阻塞
        assert slow.closed                      # 慢订阅者被隔离关闭
        assert hub.subscriber_count == 1
        with pytest.raises(SubscriptionClosed):
            while True:
                await slow.next()
    run(main())


def test_unsubscribe_stops_delivery():
    async def main():
        hub = SubscriptionHub()
        sub = hub.subscribe()
        hub.unsubscribe(sub)
        assert hub.subscriber_count == 0
        hub.publish(make_event(EventType.ADDED, "n", "a", 1))  # 不抛异常
        with pytest.raises(SubscriptionClosed):
            await sub.next()
    run(main())
