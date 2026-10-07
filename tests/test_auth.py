"""授权：越权拒绝、过滤不泄露。"""
import pytest

from dashboard.auth import AuthScope, ForbiddenError
from dashboard.models import ResourceKey
from tests.conftest import make_resource


def test_admin_sees_all():
    scope = AuthScope("admin", None)
    rs = [make_resource("ns-0", "a"), make_resource("ns-1", "b")]
    assert len(scope.filter(rs)) == 2
    scope.check(ResourceKey("anything", "x"))


def test_restricted_scope_filters():
    scope = AuthScope("alice", frozenset({"ns-0"}))
    rs = [make_resource("ns-0", "a"), make_resource("ns-1", "b"),
          make_resource("ns-0", "c")]
    visible = scope.filter(rs)
    assert [r.id for r in visible] == ["ns-0/a", "ns-0/c"]


def test_forbidden_single_resource():
    scope = AuthScope("alice", frozenset({"ns-0"}))
    with pytest.raises(ForbiddenError):
        scope.check(ResourceKey("ns-1", "b"))


def test_forbidden_explicit_namespace():
    scope = AuthScope("alice", frozenset({"ns-0"}))
    scope.check_namespaces(["ns-0"])  # 不抛
    with pytest.raises(ForbiddenError):
        scope.check_namespaces(["ns-0", "ns-1"])
