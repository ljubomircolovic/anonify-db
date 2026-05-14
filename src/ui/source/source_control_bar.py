# -*- coding: utf-8 -*-
"""Source control bar (actions + domain) and master source-type selector."""

from __future__ import annotations

import html
from typing import Any

import streamlit as st

from src.logic.app_state import AppState
from src.logic.source_constants import DOMAIN_OPTIONS, SOURCE_TYPES
from src.ui.source import source_utils as su

__all__ = [
    "handle_confirm_source_click",
    "render_master_source_selector",
    "render_source_control_bar_and_domain",
    "unlock_confirmed_source",
]


def handle_confirm_source_click(app: AppState) -> None:
    """Persist confirmation to ``.env`` and lock the Source tab."""
    m = app.mapping
    ok, err = su.persist_source_confirmation_to_env(m)
    if not ok:
        st.error(err)
        return
    app.set_source_confirmed(True)
    su.log_source_event(m, "system", "source_confirmed", source_type=m.get("source_type"))
    st.rerun()


def unlock_confirmed_source(app: AppState) -> None:
    """Leave confirmed state and allow editing again."""
    m = app.mapping
    app.set_source_confirmed(False)
    su.clear_source_confirmation_env()
    su.log_source_event(m, "system", "source_confirmation_cleared")
    st.rerun()


def _on_source_type_change(app: AppState) -> None:
    """Streamlit on_change callback for the master source-type radio."""
    m = app.mapping
    new_type = su.normalize_source_type(m.get("source_type"))
    m["source_type"] = new_type
    su.clear_inactive_source_state(m, new_type)
    su.persist_source_type_env(new_type)
    su.log_source_event(m, "system", "source_type_changed", to=new_type)


def render_source_control_bar_and_domain(db: Any | None, app: AppState, locked: bool) -> None:
    """Render status row, database action buttons, and domain selector."""
    m = app.mapping
    su.inject_source_control_bar_styles()
    src_type = su.normalize_source_type(m.get("source_type"))
    pill_label = "Confirmed" if locked else "Draft"
    pill_cls = "adb-src-pill adb-src-pill--ok" if locked else "adb-src-pill adb-src-pill--draft"

    try:
        bar = st.container(border=True)
    except TypeError:
        bar = st.container()
    with bar:
        try:
            row_left, row_right = st.columns([1.25, 3.75], vertical_alignment="center")
        except TypeError:
            row_left, row_right = st.columns([1.25, 3.75])
        with row_left:
            st.markdown(
                f'<div class="adb-src-control-left">'
                f'<span class="adb-src-heading">Source</span>'
                f'<span class="{pill_cls}">{pill_label}</span>'
                f'<span class="adb-src-type">{html.escape(src_type)}</span>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with row_right:
            if src_type == "Database":
                conn_btn_disabled = su.connection_test_and_init_disabled_for_store(locked, m)
                try:
                    b0, b1, b2, b3 = st.columns(4, gap="small", vertical_alignment="center")
                except TypeError:
                    b0, b1, b2, b3 = st.columns(4, gap="small")
                with b0:
                    test_clicked = st.button(
                        "Test Connection",
                        key="db_source_test_btn",
                        disabled=conn_btn_disabled,
                        use_container_width=True,
                        help="Verify reachability with the current source settings.",
                    )
                    if test_clicked and db is not None:
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
                with b1:
                    if st.button(
                        "Initialize Session",
                        type="primary",
                        key="db_source_initialize_btn",
                        disabled=conn_btn_disabled,
                        use_container_width=True,
                        help=(
                            "Test the source connection, index schema/tables, and wire them "
                            "into the rest of the workflow."
                        ),
                    ):
                        m["trigger_session_initialize"] = True
                        su.sync_db_config_dict_from_session(m)
                        m["source_db_connection"] = dict(m["db_config"]["connection"])
                        active_schema = str(m.get("selected_schema", "") or "")
                        selected_tables = list(m.get("db_source_selected_tables") or [])
                        su.log_source_event(
                            m,
                            "database",
                            "init_triggered",
                            schema=active_schema,
                            tables=selected_tables,
                        )
                with b2:
                    if st.button(
                        "Change",
                        key="source_control_change_btn",
                        disabled=not locked,
                        use_container_width=True,
                        help="Unlock source fields to edit configuration.",
                    ):
                        unlock_confirmed_source(app)
                with b3:
                    if st.button(
                        "Confirm",
                        type="primary",
                        key="source_confirm_btn",
                        disabled=locked,
                        use_container_width=True,
                        help="Save settings to session and `.env`, then lock inputs.",
                    ):
                        handle_confirm_source_click(app)

                fb = m.get("_db_source_test_feedback")
                if isinstance(fb, (tuple, list)) and len(fb) == 2:
                    kind, text = str(fb[0]), str(fb[1])
                    if kind == "ok":
                        st.success(html.escape(text))
                    else:
                        st.error(html.escape(text))
                st.caption(
                    "Test Connection checks connectivity. Initialize Session indexes metadata. "
                    "Confirm writes settings to `.env`."
                )
            else:
                try:
                    b2, b3 = st.columns(2, gap="small", vertical_alignment="center")
                except TypeError:
                    b2, b3 = st.columns(2, gap="small")
                with b2:
                    if st.button(
                        "Change",
                        key="source_control_change_btn",
                        disabled=not locked,
                        use_container_width=True,
                        help="Unlock source fields to edit configuration.",
                    ):
                        unlock_confirmed_source(app)
                with b3:
                    if st.button(
                        "Confirm",
                        type="primary",
                        key="source_confirm_btn",
                        disabled=locked,
                        use_container_width=True,
                        help="Save settings to session and `.env`, then lock inputs.",
                    ):
                        handle_confirm_source_click(app)

    try:
        di = DOMAIN_OPTIONS.index(m.get("source_domain", DOMAIN_OPTIONS[0]))
    except ValueError:
        di = 0
    st.selectbox(
        "Domain",
        options=DOMAIN_OPTIONS,
        index=di,
        key="source_domain",
        disabled=locked,
        label_visibility="collapsed",
        help="Data domain for downstream AI heuristics.",
    )
    su.sync_domain_from_session(m)


def render_master_source_selector(app: AppState, locked: bool) -> None:
    """Top-level radio that determines which source section is rendered."""
    m = app.mapping
    m["source_type"] = su.normalize_source_type(m.get("source_type"))

    st.markdown("#### Active Data Source")
    st.radio(
        "Select Active Data Source",
        options=SOURCE_TYPES,
        horizontal=True,
        key="source_type",
        label_visibility="collapsed",
        on_change=lambda: _on_source_type_change(app),
        disabled=locked,
        help=(
            "Only the selected source's configuration is rendered. Switching clears "
            "the previous preview/metadata. The choice is persisted to .env as "
            "`SOURCE_TYPE` so the next session opens on the same source."
        ),
    )
