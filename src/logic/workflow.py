# -*- coding: utf-8 -*-
"""Plan ↔ source binding and tab readiness checks.

These routines orchestrate session-state updates that used to live inline in
``app_ui.py``. Streamlit is imported only where UI feedback (warnings) is
required.
"""

from __future__ import annotations

import logging
from typing import Any, MutableMapping, Sequence

from src.logic.app_state import AppState

logger = logging.getLogger(__name__)


def bind_plan_metadata_to_source(
    db: Any,
    selected_schema: str | None,
    ordered_tables: Sequence[str] | None,
    state: MutableMapping[str, Any],
) -> None:
    """Persist plan metadata and ensure per-table plan security rows exist.

    Writes ``plan_metadata`` for the currently scanned source and calls
    :meth:`ensure_plan_security_metadata` for every discovered table. Safe to
    call repeatedly; storage operations are treated as idempotent.

    Parameters
    ----------
    db:
        Database façade (typically :class:`src.db.DBManager` or adapter) exposing
        ``target_db_url`` and ``ensure_plan_security_metadata``.
    selected_schema:
        PostgreSQL schema bound to the active source scan.
    ordered_tables:
        Tables in execution order (may be empty).
    state:
        Streamlit session mapping to mutate.
    """
    if "plan_metadata" not in state or not isinstance(state["plan_metadata"], dict):
        state["plan_metadata"] = {}
    pm: dict[str, Any] = state["plan_metadata"]
    pm["schema_name"] = selected_schema or "public"
    pm["db_name"] = str(state.get("last_env", "None"))
    pm["plan_db_name"] = state.get("active_plan_db_name", "None")
    pm["target_db_connection"] = getattr(db, "target_db_url", "")

    tables = list(ordered_tables or [])
    logger.info("[DB_MANAGER] Binding plan to source: %s tables", len(tables))
    for t_name in tables:
        try:
            db.ensure_plan_security_metadata(selected_schema or "public", t_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ensure_plan_security_metadata failed for %s: %s", t_name, exc)

    state.pop("multi_ai_analysis", None)
    state["project_initialized"] = True
    state["plan_active"] = False
    state["planning_initialized"] = True
    state["plan_source_binding_key"] = state.get("active_plan_db_key")
    for key in ("ai_analysis", "current_plan", "plan_snapshot", "last_rendered_table"):
        state.pop(key, None)


def maybe_auto_bind_plan_to_source(db: Any, state: MutableMapping[str, Any]) -> None:
    """Wire plan metadata to the source scan when both halves become ready.

    Intended to run once per script execution after Mappings handlers. If the
    user scanned the source first and then chose a plan (or the reverse), this
    connects the two without an extra button click.

    Parameters
    ----------
    db:
        Active database façade.
    state:
        Streamlit session mapping.
    """
    app = AppState(state)
    if not app.get_source_confirmed():
        return
    if not app.get_source_connected():
        return
    plan_db_key = app.get_active_plan_db_key()
    if not plan_db_key:
        return
    if not app.get_project_initialized():
        return
    if app.get_plan_source_binding_key() == plan_db_key:
        return

    bind_plan_metadata_to_source(
        db,
        selected_schema=str(app.get_selected_schema() or "public"),
        ordered_tables=app.get_all_tables_list(),
        state=state,
    )


def render_workflow_readiness_warning(state: MutableMapping[str, Any] | None = None) -> bool:
    """Show inline warnings when Mappings-dependent tabs lack prerequisites.

    Validates the shared workflow gates used by **Mappings**, **Target Database
    Transfer** (Verify / Execute / History wizard steps), and other plan-bound
    tabs. Callers pass ``st.session_state`` (or omit it to read the live session).

    Parameters
    ----------
    state:
        Streamlit session mapping. When omitted, the active ``st.session_state``
        is used.

    Returns
    -------
    bool
        ``True`` when both source and plan halves are ready; ``False`` when a
        warning was rendered and the caller should skip downstream UI.
    """
    import streamlit as st

    store = state if state is not None else st.session_state

    source_confirmed = bool(store.get("source_confirmed", False))
    source_connected = bool(store.get("source_connected", False))
    active_plan_db_key = str(store.get("active_plan_db_key", "") or "").strip()
    project_initialized = bool(store.get("project_initialized", False))

    if not source_confirmed:
        st.warning(
            "Please click **Confirm Source** in the **Source** tab before using "
            "**Mappings** or **Target Database Transfer**."
        )
        return False

    missing_source = not source_connected
    missing_plan = not (bool(active_plan_db_key) and project_initialized)
    if not (missing_source or missing_plan):
        return True

    if missing_source and missing_plan:
        st.warning(
            "Please complete **Source connection** in the **Source** tab "
            "(Initialize Session after confirming source) and **Plan selection** "
            "in **Mappings** before using **Target Database Transfer**."
        )
    elif missing_source:
        st.warning(
            "Please run **Initialize Session** in the **Source** tab after confirming your source."
        )
    else:
        st.warning(
            "Please complete **Plan selection** in the **Mappings** tab to proceed."
        )
    return False
