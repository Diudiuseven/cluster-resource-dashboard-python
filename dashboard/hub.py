"""订阅中枢：向多个订阅者扇出实时事件，慢订阅者隔离。

设计要点：
- 每个订阅者有独立有界队列，进度互不影响；
- publish() 使用非阻塞投递，任何订阅者都不会阻塞发布者与其他订阅者；
- 队列积满时关闭该订阅者（而非阻塞或丢给其他订阅者制造延迟），
  被关闭的订阅者需通过快照 + rv 重新对齐，保证不会拿到不一致视图。
"""
from __future__ import annotations

import asyncio

from .models import ResourceEvent


class SubscriptionClosed(Exception):
    """订阅因积压超限或主动关闭而终止，调用方应重新对齐。"""


_CLOSE = object()  # 关闭哨兵


class Subscription:
    """单个订阅者的事件队列，进度独立。"""

    def __init__(self, max_queue: int = 256) -> None:
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def next(self) -> ResourceEvent:
        """取下一条事件；订阅关闭后抛 SubscriptionClosed。"""
        item = await self._queue.get()
        if item is _CLOSE:
            raise SubscriptionClosed("subscription closed")
        return item

    def publish(self, event: ResourceEvent) -> bool:
        """非阻塞投递；队列满或已关闭返回 False。"""
        if self._closed:
            return False
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self.close()
            return False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(_CLOSE)
        except asyncio.QueueFull:
            # 队列已满：腾出一个位置放关闭哨兵，确保 next() 能终止
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(_CLOSE)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass


class SubscriptionHub:
    """管理全部订阅者，发布事件时互不阻塞。"""

    def __init__(self, max_queue_per_subscriber: int = 256) -> None:
        self._max_queue = max_queue_per_subscriber
        self._subscribers: set[Subscription] = set()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def subscribe(self) -> Subscription:
        sub = Subscription(self._max_queue)
        self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        self._subscribers.discard(sub)
        sub.close()

    def publish(self, event: ResourceEvent) -> None:
        """非阻塞扇出；失败的订阅者被移除并关闭。"""
        stale = []
        for sub in self._subscribers:
            if not sub.publish(event):
                stale.append(sub)
        for sub in stale:
            self._subscribers.discard(sub)
