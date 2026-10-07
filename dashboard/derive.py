"""父级状态派生：父资源状态由子资源状态聚合而来。

派生规则（确定性、与子级顺序无关）：
- 无子级：保持自身状态；
- 任一子级 FAILED    -> FAILED
- 任一子级 DEGRADED  -> DEGRADED
- 任一子级 UNKNOWN   -> UNKNOWN
- 全部 HEALTHY       -> HEALTHY
"""
from __future__ import annotations

from .models import Resource, ResourceStatus

_WORST_ORDER = [
    ResourceStatus.FAILED,
    ResourceStatus.DEGRADED,
    ResourceStatus.UNKNOWN,
    ResourceStatus.HEALTHY,
]


def derive_parent_status(children: list[Resource]) -> ResourceStatus:
    """由子资源状态派生父级状态；无子级返回 None 由调用方保留原值。"""
    if not children:
        raise ValueError("derive_parent_status requires at least one child")
    statuses = {c.status for c in children}
    for s in _WORST_ORDER:
        if s in statuses:
            return s
    return ResourceStatus.UNKNOWN


def reconcile_parents(resources: dict[str, Resource]) -> None:
    """就地修正所有父资源状态，使层级归属关系自洽。

    支持多级层级：按深度自底向上逐层派生，保证任一时刻读取到的
    父子状态不自相矛盾。检测不到父级的资源按顶层处理。
    """
    children_of: dict[str, list[Resource]] = {}
    for r in resources.values():
        if r.parent is not None and r.parent in resources:
            children_of.setdefault(r.parent, []).append(r)

    def depth(r: Resource) -> int:
        d, seen, cur = 0, set(), r
        while cur.parent is not None and cur.parent in resources \
                and cur.parent not in seen:
            seen.add(cur.id)
            cur = resources[cur.parent]
            d += 1
        return d

    # 深度大的先处理（叶子向上），父级派生时使用已修正的子级状态
    for r in sorted(resources.values(), key=depth, reverse=True):
        kids = children_of.get(r.id)
        if kids:
            r.status = derive_parent_status(kids)
