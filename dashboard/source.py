"""本地可复现数据源：确定性生成快照与事件流，不依赖真实集群或网络。

相同 seed 与参数下，snapshot() 与 events() 的输出逐条一致，测试环境
可稳定复现相同结果。事件按每个资源的 seq 单调递增发出；乱序/重复
场景由调用方（测试）在投递时构造。
"""
from __future__ import annotations

import random
from collections.abc import Iterator

from .models import (EventType, Resource, ResourceEvent, ResourceKey,
                     ResourceStatus)

_STATUSES = list(ResourceStatus)


class LocalDataSource:
    """基于固定随机种子的确定性数据源。"""

    def __init__(self, seed: int = 42, namespaces: int = 3,
                 parents_per_ns: int = 2, children_per_parent: int = 3,
                 num_events: int = 50) -> None:
        self._seed = seed
        self._ns_names = [f"ns-{i}" for i in range(namespaces)]
        self._parents_per_ns = parents_per_ns
        self._children_per_parent = children_per_parent
        self._num_events = num_events

    def _key(self, ns: str, name: str) -> ResourceKey:
        return ResourceKey(ns, name)

    def _initial_resources(self) -> list[Resource]:
        """确定性初始拓扑：每个命名空间下若干 App（父），各带若干 Pod（子）。"""
        rng = random.Random(self._seed)
        out: list[Resource] = []
        for ns in self._ns_names:
            for p in range(self._parents_per_ns):
                parent_id = f"{ns}/app-{p}"
                out.append(Resource(self._key(ns, f"app-{p}"), "App", None,
                                    rng.choice(_STATUSES),
                                    {"replicas": self._children_per_parent}))
                for c in range(self._children_per_parent):
                    out.append(Resource(self._key(ns, f"pod-{p}-{c}"), "Pod",
                                        parent_id, rng.choice(_STATUSES),
                                        {"cpu": rng.randint(1, 4)}))
        return out

    def snapshot(self) -> list[Resource]:
        """确定性全量快照（初始拓扑）。"""
        return self._initial_resources()

    def events(self) -> Iterator[ResourceEvent]:
        """确定性变更事件流：新增/变更/删除混合，每资源 seq 单调递增。"""
        rng = random.Random(self._seed + 1)
        live: dict[ResourceKey, Resource] = {
            r.key: r for r in self._initial_resources()}
        # 快照中的资源视为已消费 seq=1（与服务侧 load_snapshot 的约定一致），
        # 事件流从 seq=2 继续，保证每资源序号单调递增
        seqs: dict[ResourceKey, int] = {k: 1 for k in live}

        def next_seq(key: ResourceKey) -> int:
            seqs[key] = seqs.get(key, 0) + 1
            return seqs[key]

        counter = 0
        for _ in range(self._num_events):
            op = rng.choices(["add", "mod", "del"], weights=[3, 5, 2])[0]
            if op == "add" or not live:
                ns = rng.choice(self._ns_names)
                counter += 1
                parent = f"{ns}/app-{rng.randrange(self._parents_per_ns)}"
                res = Resource(self._key(ns, f"pod-x-{counter}"), "Pod",
                               parent, rng.choice(_STATUSES),
                               {"cpu": rng.randint(1, 4)})
                live[res.key] = res
                yield ResourceEvent(EventType.ADDED, res.key, next_seq(res.key),
                                    resource=res)
            elif op == "mod":
                res = rng.choice(sorted(live.values(), key=lambda r: r.id))
                updated = Resource(res.key, res.kind, res.parent,
                                   rng.choice(_STATUSES), dict(res.spec))
                live[res.key] = updated
                yield ResourceEvent(EventType.MODIFIED, res.key,
                                    next_seq(res.key), resource=updated)
            else:  # del
                res = rng.choice(sorted(live.values(), key=lambda r: r.id))
                del live[res.key]
                yield ResourceEvent(EventType.DELETED, res.key,
                                    next_seq(res.key))
