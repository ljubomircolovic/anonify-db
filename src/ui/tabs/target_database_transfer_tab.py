# -*- coding: utf-8 -*-
"""Unified Target Database Transfer workflow (Verify, Execute, History)."""

from __future__ import annotations

from typing import Any

import streamlit as st

from src.logic.workflow import render_workflow_readiness_warning
from src.ui.tabs.comparison_tab import render_comparison_tab
from src.ui.tabs.planner.planner_secrets import resolve_active_plan_seed


def _render_verify_step(db: Any) -> None:
    """Step 1: render the existing Comparison content."""
    if render_workflow_readiness_warning(st.session_state):
        render_comparison_tab(db)


def _render_execute_step(db: Any) -> None:
    """Step 2: render the existing Export content."""
    st.markdown("### 📤 Export Anonymized Data")
    st.caption(
        "Generate a full anonymized export of the active table using the current plan, "
        "deterministic salt, and locale."
    )
    if not render_workflow_readiness_warning(st.session_state):
        return
    if "current_plan" not in st.session_state:
        st.info(
            "Define and save anonymization rules in the **Mappings** tab to enable export."
        )
        return

    export_table_name, export_schema_name = st.session_state["selected_table_info"]
    export_plan = st.session_state["current_plan"]
    export_salt = resolve_active_plan_seed(db, export_schema_name, export_table_name)
    export_locale = st.session_state.get("selected_locale", "de")

    st.markdown(
        f"**Active table:** `{export_schema_name}.{export_table_name}` &nbsp;·&nbsp; "
        f"**Locale:** `{export_locale.upper()}`"
    )

    if st.button("Prepare Full Download (CSV)", key="export_tab_prepare_csv"):
        with st.spinner("Generating CSV..."):
            full_df = db.read_table(export_table_name, export_schema_name)
            full_anon = db.apply_anonymization_rules(
                full_df,
                export_plan,
                salt=export_salt,
            )
            csv_bytes = full_anon.to_csv(
                index=False, encoding="utf-8-sig"
            ).encode("utf-8-sig")
            st.download_button(
                label="Click to Download CSV",
                data=csv_bytes,
                file_name=f"anon_{export_table_name}.csv",
                mime="text/csv",
                key="export_tab_download_csv",
            )


def _render_history_step(db: Any) -> None:
    """Step 3: render the existing Audit content."""
    st.markdown("### 📜 Audit Log")
    st.caption("Most recent 50 audit events recorded by the metadata database.")
    try:
        log_df = db.get_audit_logs(limit=50)
        if log_df.empty:
            st.info("No audit logs found yet.")
        else:
            st.dataframe(log_df, width="stretch")
    except Exception as exc:
        st.error(f"Could not load audit logs: {exc}")


def render_target_database_transfer_tab(db: Any) -> None:
    """Render unified transfer wizard with three internal steps."""
    st.markdown("### 🎯 Target Database Transfer")
    st.caption(
        "Unified transfer workflow: verify anonymization preview, execute export, "
        "and review audit history."
    )

    step_verify, step_execute, step_history = st.tabs(
        [
            "1️⃣ Verify (Comparison)",
            "2️⃣ Execute (Export)",
            "3️⃣ History (Audit)",
        ]
    )

    with step_verify:
        _render_verify_step(db)
    with step_execute:
        _render_execute_step(db)
    with step_history:
        _render_history_step(db)
