# -*- coding: utf-8 -*-
"""Strict read-only SQL validation for user-entered mirror / preview queries."""

from __future__ import annotations

import logging
from typing import Iterable, Sequence, Type

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - enforced via requirements.txt
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

SECURITY_POLICY_ERROR = (
    "🔒 Security Policy Violation: Only single read-only SELECT operations are allowed."
)

_PARSE_DIALECT = "duckdb"

# AST node types that perform or request writes, DDL, or procedural execution.
_FORBIDDEN_EXPR_TYPES: tuple[Type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.TruncateTable,
    exp.Merge,
    exp.Command,
    exp.Copy,
    exp.Alter,
    exp.Grant,
    exp.Revoke,
    exp.Attach,
    exp.Detach,
    exp.LoadData,
    exp.Replace,
    exp.MultitableInserts,
    exp.ConditionalInsert,
    exp.Into,
    exp.Lock,
)

_READ_ROOT_TYPES: tuple[Type[exp.Expression], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
)


def _meaningful_statements(parsed: Sequence[exp.Expression | None]) -> list[exp.Expression]:
    """Drop empty parse slots and trailing ``;`` comment placeholders."""
    statements: list[exp.Expression] = []
    for expr in parsed:
        if expr is None:
            continue
        if isinstance(expr, exp.Semicolon):
            continue
        statements.append(expr)
    return statements


def _contains_forbidden_node(root: exp.Expression) -> bool:
    for node in root.walk():
        if isinstance(node, _FORBIDDEN_EXPR_TYPES):
            return True
    return False


def _cte_bodies_are_readonly(root: exp.Expression) -> bool:
    """Reject write-based CTEs (e.g. ``WITH x AS (DELETE …) SELECT …``)."""
    for cte in root.find_all(exp.CTE):
        body = cte.this
        if body is None:
            return False
        if not isinstance(body, _READ_ROOT_TYPES):
            return False
        if _contains_forbidden_node(body):
            return False
    return True


def _is_readonly_query_root(expr: exp.Expression) -> bool:
    if not isinstance(expr, _READ_ROOT_TYPES):
        return False
    if _contains_forbidden_node(expr):
        return False
    return _cte_bodies_are_readonly(expr)


def validate_safe_select_query(sql_text: str) -> bool:
    """
    Return ``True`` only when ``sql_text`` is a single read-only ``SELECT`` (or
    ``WITH`` / set-operation composition of selects) with no nested writes.

    Uses ``sqlglot.parse(..., read="duckdb")``. Multiple executable statements,
    DDL/DML roots, ``Command``/``COPY`` nodes, write CTEs, ``SELECT INTO``, and
    locking reads are rejected.
    """
    text = str(sql_text or "").strip()
    if not text:
        return False

    if sqlglot is None or exp is None:
        logger.warning("validate_safe_select_query: sqlglot is not installed")
        return False

    try:
        parsed = sqlglot.parse(text, read=_PARSE_DIALECT)
    except Exception as exc:  # noqa: BLE001
        logger.debug("validate_safe_select_query: parse failed: %s", exc)
        return False

    statements = _meaningful_statements(parsed)
    if len(statements) != 1:
        return False

    root = statements[0]
    if not _is_readonly_query_root(root):
        return False

    return True
