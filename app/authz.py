"""Authorization scope checks.

A resource is visible to a caller iff its namespace is in the scope's
namespace set (or the set is empty, meaning "all") AND its kind is in
the scope's kind set (same empty-means-all rule). Everything else is
denied, and denied resources are filtered out *before* pagination and
aggregation so totals never leak hidden resources.
"""
from __future__ import annotations

from typing import Iterable, Iterator

from .models import Resource, Scope


def visible(resource: Resource, scope: Scope) -> bool:
    if scope.namespaces and resource.namespace not in scope.namespaces:
        return False
    if scope.kinds and resource.kind not in scope.kinds:
        return False
    return True


def filter_visible(resources: Iterable[Resource], scope: Scope) -> Iterator[Resource]:
    return (r for r in resources if visible(r, scope))
