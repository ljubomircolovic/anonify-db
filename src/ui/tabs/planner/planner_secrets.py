# -*- coding: utf-8 -*-
"""Deterministic salt / seed helpers for planner, comparison, and export tabs."""

from __future__ import annotations

from typing import Any

import streamlit as st


def resolve_plan_salt(db: Any, schema_name: str, table_name: str) -> Any:
    """Return per-table plan salt, creating metadata via ``ensure_plan_security_metadata`` if missing."""
    saved = db.get_saved_plan(schema_name, table_name)
    if saved and saved.get("salt"):
        return saved.get("salt")
    salt_val, _, _ = db.ensure_plan_security_metadata(schema_name, table_name)
    return salt_val


def resolve_active_plan_seed(db: Any, schema_name: str, table_name: str) -> Any:
    """Prefer active plan database id as global seed; fall back to stored plan salt."""
    plan_id = (
        str((st.session_state.get("plan_metadata", {}) or {}).get("plan_db_name", "")).strip()
        or str(st.session_state.get("active_plan_db_name", "")).strip()
    )
    return plan_id or resolve_plan_salt(db, schema_name, table_name)


def build_consistency_seed_maps(
    db: Any, schema_name: str, execution_order: list[str], global_seed: str
) -> dict[str, dict[str, str]]:
    """Build per-table column→seed maps so FK/PK hash columns stay aligned."""
    relations = db.get_all_foreign_keys(schema_name) or []
    adjacency: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for table_name in execution_order or []:
        for pk_col in db.get_primary_keys(schema_name, table_name) or []:
            adjacency.setdefault((table_name, pk_col), set())
    for child_table, child_col, parent_table, parent_col in relations:
        left = (child_table, child_col)
        right = (parent_table, parent_col)
        adjacency.setdefault(left, set()).add(right)
        adjacency.setdefault(right, set()).add(left)

    seed_by_node: dict[tuple[str, str], str] = {}
    visited: set[tuple[str, str]] = set()
    for node in list(adjacency.keys()):
        if node in visited:
            continue
        component: list[tuple[str, str]] = []
        stack = [node]
        visited.add(node)
        while stack:
            current = stack.pop()
            component.append(current)
            for neigh in adjacency.get(current, set()):
                if neigh not in visited:
                    visited.add(neigh)
                    stack.append(neigh)
        seed = str(global_seed or "default_plan_salt")
        for member in component:
            seed_by_node[member] = seed

    per_table: dict[str, dict[str, str]] = {}
    for table_name in execution_order or []:
        table_map: dict[str, str] = {}
        for (tbl, col), seed in seed_by_node.items():
            if tbl == table_name:
                table_map[col] = seed
        per_table[table_name] = table_map
    return per_table
