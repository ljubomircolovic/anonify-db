# -*- coding: utf-8 -*-
"""Planner validation helpers, persistence bridges, and save pipeline."""

from __future__ import annotations

import json
import logging
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.core.domain.services.unsaved_changes_guard import UnsavedChangesGuard
from src.ui.tabs.planner.planner_secrets import resolve_plan_salt as _resolve_plan_salt

logger = logging.getLogger(__name__)
unsaved_changes_guard = UnsavedChangesGuard()


def _get_live_preview_once(db, schema_name, table_name, where_clause, row_limit, force_refresh=False):
    """Session-level lock/cache to avoid duplicate preview reads on reruns."""
    cache_df_key = f"live_preview_df_{schema_name}_{table_name}"
    cache_sig_key = f"live_preview_sig_{schema_name}_{table_name}"
    signature = (schema_name, table_name, str(where_clause or "").strip(), int(row_limit))
    if force_refresh or st.session_state.get(cache_sig_key) != signature or cache_df_key not in st.session_state:
        st.session_state[cache_df_key] = db.read_table(
            table_name,
            schema_name,
            where=signature[2],
            limit=signature[3],
        )
        st.session_state[cache_sig_key] = signature
    return st.session_state.get(cache_df_key)


def _table_plan_cache_key(schema_name, table_name):
    return f"{schema_name}.{table_name}"


def _get_persisted_plan_for_table(schema_name, table_name):
    # Primary persistence format requested: active_plan[table_name]['mappings']
    active_plan = st.session_state.get('active_plan', {})
    table_block = active_plan.get(table_name) if isinstance(active_plan, dict) else None
    if isinstance(table_block, dict) and isinstance(table_block.get('mappings'), list):
        return table_block.get('mappings')

    # Backward compatibility with schema-qualified key.
    skey = _table_plan_cache_key(schema_name, table_name)
    table_block = active_plan.get(skey) if isinstance(active_plan, dict) else None
    if isinstance(table_block, dict) and isinstance(table_block.get('mappings'), list):
        return table_block.get('mappings')

    # Legacy store.
    return st.session_state.get('active_plan_by_table', {}).get(skey)


def _persist_current_plan_for_table(schema_name, table_name):
    plan = st.session_state.get('current_plan')
    if not plan:
        return
    if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
        st.session_state['active_plan'] = {}
    st.session_state['active_plan'][table_name] = {"mappings": plan}
    st.session_state['active_plan'][_table_plan_cache_key(schema_name, table_name)] = {"mappings": plan}
    if 'active_plan_by_table' not in st.session_state:
        st.session_state['active_plan_by_table'] = {}
    st.session_state['active_plan_by_table'][_table_plan_cache_key(schema_name, table_name)] = plan


def _default_manual_plan(db, schema_name, table_name):
    return [
        {"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual default (keep original)"}
        for c in db.get_columns(table_name, schema_name)
    ]


def _normalize_plan_by_column(plan_rows):
    out = {}
    for row in plan_rows or []:
        if not isinstance(row, dict):
            continue
        col = str(row.get("column", "")).strip()
        if not col:
            continue
        out[col] = row
    return out


def _merge_ai_into_existing_plan(existing_plan, ai_plan, table_name, force_overwrite=False):
    """
    Merges AI rows into existing plan while preserving manual overrides by default.
    """
    manual_overrides = st.session_state.get("manual_overrides_by_table", {}).get(table_name, set())
    existing_map = _normalize_plan_by_column(existing_plan)
    ai_map = _normalize_plan_by_column(ai_plan)

    merged_map = dict(existing_map)
    for col, ai_row in ai_map.items():
        if (not force_overwrite) and col in manual_overrides:
            continue
        merged_map[col] = {
            "column": col,
            "is_pii": bool(ai_row.get("is_pii", False)),
            "strategy": str(ai_row.get("strategy", "keep")).lower(),
            "reason": str(ai_row.get("reason", "")),
        }

    return list(merged_map.values())


def _track_manual_overrides_for_table(table_name, edited_rows):
    if "manual_overrides_by_table" not in st.session_state:
        st.session_state["manual_overrides_by_table"] = {}
    if table_name not in st.session_state["manual_overrides_by_table"]:
        st.session_state["manual_overrides_by_table"][table_name] = set()

    overrides = st.session_state["manual_overrides_by_table"][table_name]
    ai_baseline = st.session_state.get("last_ai_plan_by_table", {}).get(table_name, [])
    ai_map = _normalize_plan_by_column(ai_baseline)

    for row in edited_rows or []:
        col = str(row.get("column", "")).strip()
        if not col:
            continue
        baseline = ai_map.get(col, {"strategy": "keep", "is_pii": False, "reason": ""})
        if (
            str(row.get("strategy", "keep")).lower() != str(baseline.get("strategy", "keep")).lower()
            or bool(row.get("is_pii", False)) != bool(baseline.get("is_pii", False))
            or str(row.get("reason", "")).strip() != str(baseline.get("reason", "")).strip()
        ):
            overrides.add(col)


def _normalized_plan_rows(rows):
    normalized = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        normalized.append({
            "column": str(row.get("column", "")),
            "is_pii": bool(row.get("is_pii", False)),
            "strategy": str(row.get("strategy", "keep")).lower(),
            "reason": str(row.get("reason", "")),
        })
    normalized.sort(key=lambda r: r["column"])
    return normalized


def _set_current_plan_data(schema_name, table_name, plan_rows, where_clause, dirty):
    if "current_plan_data" not in st.session_state or not isinstance(st.session_state["current_plan_data"], dict):
        st.session_state["current_plan_data"] = {}
    st.session_state["current_plan_data"][f"{schema_name}.{table_name}"] = {
        "plan": list(plan_rows or []),
        "where": str(where_clause or "").strip(),
        "dirty": bool(dirty),
    }


def _get_unsaved_tables():
    current_data = st.session_state.get("current_plan_data", {})
    return unsaved_changes_guard.get_unsaved_tables(current_data)


def _is_sensitive_row(row):
    if not isinstance(row, dict):
        return False
    return bool(row.get("is_sensitive", row.get("is_pii", False)))


def _get_sensitive_keep_violations(plan_rows, table_name=None):
    violations = []
    for row in plan_rows or []:
        if not isinstance(row, dict):
            continue
        if not _is_sensitive_row(row):
            continue
        strategy = str(row.get("strategy", "keep")).lower()
        if strategy == "keep":
            col_name = str(row.get("column", ""))
            if table_name:
                violations.append(f"{table_name}.{col_name}")
            else:
                violations.append(col_name)
    return violations


def _collect_sensitive_keep_violations(db, schema_name, table_names):
    violations = []
    current_data = st.session_state.get("current_plan_data", {})
    for table_name in table_names or []:
        table_key = f"{schema_name}.{table_name}"
        if table_key in current_data and isinstance(current_data.get(table_key), dict):
            plan_rows = current_data.get(table_key, {}).get("plan", [])
        else:
            saved = db.get_saved_plan(schema_name, table_name)
            plan_rows = saved.get("plan", []) if saved else []
        violations.extend(_get_sensitive_keep_violations(plan_rows, table_name))
    return violations


def _finalize_close_project():
    # Clear entire session state and return app to initial screen.
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()



def _resolve_target_connection_from_plan(db):
    """
    Enforces plan-bound target database connection before execution starts.
    """
    plan_meta = st.session_state.get("plan_metadata", {}) or {}
    active_plan_db_name = str(st.session_state.get("active_plan_db_name", "")).strip()
    target_db_connection = str(plan_meta.get("target_db_connection", "")).strip()
    target_db_name = str(plan_meta.get("plan_db_name", "")).strip() or active_plan_db_name

    if target_db_name:
        db.connect_to_existing_plan_database(target_db_name)
        st.session_state["connected_plan_db_name"] = target_db_name
        target_db_connection = db.target_db_url
        if "plan_metadata" not in st.session_state or not isinstance(st.session_state["plan_metadata"], dict):
            st.session_state["plan_metadata"] = {}
        st.session_state["plan_metadata"]["plan_db_name"] = target_db_name
        st.session_state["plan_metadata"]["target_db_connection"] = target_db_connection
    elif target_db_connection:
        parsed = urlparse(target_db_connection)
        inferred_db = str(parsed.path or "").lstrip("/")
        if inferred_db:
            db.connect_to_existing_plan_database(inferred_db)
            st.session_state["connected_plan_db_name"] = inferred_db
            target_db_name = inferred_db
    return target_db_name, target_db_connection


def _get_quoted_value(column_name, col_details, value):
    if column_name in col_details:
        col_type = col_details[column_name]["type"].lower()
        if any(t in col_type for t in ["char", "text", "uuid", "date", "time", "varchar"]):
            return f"'{value}'"
    return str(value)


def _find_multi_result(multi_results, table_name, schema_name):
    """Robust lookup for scan results regardless of key format."""
    if not isinstance(multi_results, dict):
        return None

    base_name = str(table_name).split('.')[-1]
    candidates = [
        str(table_name),
        base_name,
        f"{schema_name}.{table_name}",
        f"{schema_name}.{base_name}",
    ]

    seen = set()
    for key in candidates:
        if key in seen:
            continue
        seen.add(key)
        if key in multi_results and multi_results.get(key):
            return multi_results.get(key)

    # Fallback: match by base table name when key formats differ
    for result_key, result_value in multi_results.items():
        if not result_value:
            continue
        if str(result_key).split('.')[-1] == base_name:
            return result_value

    return None

def _pick_first_table_by_execution_order(result_keys, execution_order):
    """
    Returns the first table from execution_order that exists in scan results.
    result_keys can be schema-qualified or base table names.
    """
    normalized_result_keys = {str(k).split('.')[-1] for k in result_keys}
    for ordered_table in execution_order or []:
        if ordered_table in normalized_result_keys:
            return ordered_table
    return None
