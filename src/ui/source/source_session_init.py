# -*- coding: utf-8 -*-
"""Source session initialization for ``st.button`` callbacks (single-click scan)."""

from __future__ import annotations

import logging
from typing import Any, MutableMapping

import streamlit as st

from src.logic.workflow import bind_plan_metadata_to_source
from src.ui.source import source_utils as su

logger = logging.getLogger(__name__)


def _session() -> MutableMapping[str, Any]:
    return st.session_state


def handle_initialization(db: Any) -> None:
    """Test the source connection, index schema/tables, and optionally bind the active plan.

    Designed for ``st.button(..., on_click=...)`` so this runs at click time (before the rest
    of the script), avoiding a one-rerun lag when initialization was triggered below tab render
    order in ``app_ui.py``.
    """
    m = _session()
    selected_env = str(m.get("selected_env_label") or "").strip()

    if not m.get("source_confirmed"):
        m["_source_init_feedback"] = (
            "warn",
            "Confirm Source in the **Source** tab before running Initialize Session.",
        )
        return

    su.sync_db_config_dict_from_session(m)
    m["source_db_connection"] = dict((m.get("db_config") or {}).get("connection") or {})

    with st.spinner("Establishing secure connection and indexing source metadata..."):
        try:
            success, message = db.test_connection()
        except Exception as exc:  # noqa: BLE001
            success, message = False, str(exc)

        if not success:
            m["source_connected"] = False
            m["_source_init_feedback"] = ("err", message or "Connection failed.")
            return

        m["source_connected"] = True
        m["source_db_connection"] = dict((m.get("db_config") or {}).get("connection") or {})

        selected_schema = m.get("selected_schema")
        schemas = db.get_all_schemas()
        if not selected_schema:
            selected_schema = schemas[0] if schemas else "public"
            m["selected_schema"] = selected_schema

        tables = db.get_tables_in_schema(selected_schema) if selected_schema else []
        ordered_tables = (
            db.get_execution_order(tables, selected_schema) if selected_schema else []
        )
        m["last_confirmed_tables"] = tables
        m["all_tables_list"] = ordered_tables
        m["selected_tables"] = ordered_tables
        if ordered_tables:
            m["selected_table_info"] = (ordered_tables[0], selected_schema)

        is_plan_ready = (
            bool(m.get("active_plan_db_key"))
            and m.get("project_initialized", False)
            and str(m.get("active_plan_db_key", "")).startswith(f"{selected_env}:")
        )
        if is_plan_ready:
            bind_plan_metadata_to_source(db, selected_schema, ordered_tables, m)
        else:
            logger.info(
                "[DB_MANAGER] Source scanned without an active plan; binding deferred."
            )

    active_schema = str(m.get("selected_schema", "") or "")
    selected_tbls = list(m.get("db_source_selected_tables") or [])
    su.log_source_event(
        m,
        "database",
        "init_triggered",
        schema=active_schema,
        tables=selected_tbls,
    )
    m.pop("_source_init_feedback", None)


def handle_test_connection(db: Any) -> None:
    """Run a source connectivity check (``st.button`` callback)."""
    m = _session()
    su.sync_db_config_dict_from_session(m)
    try:
        ok, msg = db.test_connection()
    except Exception as exc:  # noqa: BLE001
        ok, msg = False, str(exc)
    m["source_connected"] = bool(ok)
    su.log_source_event(m, "database", "test_connection", ok=bool(ok), msg=str(msg))
    if ok:
        m["_db_source_test_feedback"] = ("ok", msg or "Connection successful! ✅")
    else:
        m["_db_source_test_feedback"] = ("err", msg or "Connection failed.")
