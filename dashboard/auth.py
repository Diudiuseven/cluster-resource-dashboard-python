"""调用方授权范围：按命名空间过滤可见资源，越权拒绝。"""
from __future__ import annotations

from dataclasses import dataclass

from .models import Resource, ResourceKey


class ForbiddenError(Exception):
    """访问超出授权范围的资源。"""


@dataclass(frozen=True)
class AuthScope:
    """授权范围。namespaces 为 None 表示全部可见（管理员）。

    过滤语义：filter() 只返回可见资源，返回的分页/聚合结果中不出现
    任何被隐藏资源的痕迹（数量、占位、统计均按过滤后计算）。
    """

    subject: str
    namespaces: frozenset[str] | None = None

    def allows_namespace(self, namespace: str) -> bool:
        return self.namespaces is None or namespace in self.namespaces

    def allows(self, key: ResourceKey) -> bool:
        return self.allows_namespace(key.namespace)

    def filter(self, resources: list[Resource]) -> list[Resource]:
        """过滤出可见资源，不泄露被隐藏资源的存在。"""
        return [r for r in resources if self.allows(r.key)]

    def check(self, key: ResourceKey) -> None:
        """越权则抛 ForbiddenError。"""
        if not self.allows(key):
            raise ForbiddenError(
                f"subject {self.subject!r} may not access {key.id()!r}")

    def check_namespaces(self, requested: list[str]) -> None:
        """请求显式指定了越权命名空间时拒绝（而非静默过滤）。"""
        for ns in requested:
            if not self.allows_namespace(ns):
                raise ForbiddenError(
                    f"subject {self.subject!r} may not access namespace {ns!r}")
