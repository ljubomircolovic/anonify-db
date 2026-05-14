# -*- coding: utf-8 -*-
"""Global actions, review, execution controls for the planner tab."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

import streamlit as st

from src.logic.app_state import AppState
from src.ui.tabs.planner.planner_logic import get_clean_plan
from src.ui.tabs.planner.planner_navigation import get_next_table_in_chain
from src.ui.tabs.execute_tab import (
    render_execute_confirmation_dialog,
    render_finalize_confirmation_dialog,
)
from src.ui.tabs.planner.planner_header import render_target_context_banner
from src.ui.tabs.planner.planner_save_pipeline import save_and_move_to_next
from src.ui.tabs.planner.planner_validation import (
    _collect_sensitive_keep_violations,
    _finalize_close_project,
    _get_persisted_plan_for_table,
    _get_unsaved_tables,
    _resolve_target_connection_from_plan,
)

logger = logging.getLogger(__name__)


def render_planner_actions_block(
    db: Any,
    app: AppState,
    *,
    selected_schema: str,
    violation_found: bool,
    review_progress_slot: Any,
    review_status_slot: Any,
    go_to_table_by_index: Callable[..., None],
    set_active_table_by_index: Callable[..., None],
) -> None:
    """Renders Global Actions, Review, Execute, and Finalize (after filter/preview expander)."""
    m = app.mapping

    st.markdown("---")
    st.markdown("### Global Actions")
    plans_initialized = bool(
        m.get("plan_active") or m.get("active_plan_by_table") or m.get("active_plan")
    )
    if "selected_table_info" in m and plans_initialized:
        table_info = m["selected_table_info"]
        table_name = table_info[0] if isinstance(table_info, tuple) else table_info
        schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
        ordered_tables = m.get("all_tables_list", []) or m.get("selected_tables", [])
        current_idx = m.get("current_table_index", m.get("active_table_index", 0))
        current_idx = max(0, min(current_idx, len(ordered_tables) - 1)) if ordered_tables else -1
        next_t = get_next_table_in_chain(table_name, ordered_tables, m.get("completed_tables", set()))
        where_key = f"where_clause_{table_name}"

        action_cols = st.columns([1, 1, 1], gap="small")
        with action_cols[0]:
            if st.button(
                "⬅️ Previous",
                width="stretch",
                disabled=(current_idx <= 0),
                key=f"global_prev_{schema_name}_{table_name}",
            ):
                go_to_table_by_index(current_idx - 1, ordered_tables, schema_name)
        with action_cols[1]:
            if st.button(
                "Next ➡️",
                width="stretch",
                disabled=not (ordered_tables and current_idx < len(ordered_tables) - 1),
                key=f"global_next_{schema_name}_{table_name}",
            ):
                go_to_table_by_index(current_idx + 1, ordered_tables, schema_name)
        with action_cols[2]:
            save_label = "💾 Save and Next" if next_t else "💾 Save Table Configuration"
            if st.button(
                save_label,
                type="primary",
                width="stretch",
                disabled=violation_found,
                key=f"global_conf_{table_name}",
            ):
                current_plan = m.get("current_plan")
                if not current_plan:
                    current_plan = _get_persisted_plan_for_table(schema_name, table_name)
                if current_plan is None:
                    current_plan = m.get("ai_analysis")

                if current_plan is None:
                    st.error("No active table plan found to save.")
                    st.stop()

                save_ok = save_and_move_to_next(
                    db,
                    table_name,
                    schema_name,
                    current_plan,
                    m.get(where_key, ""),
                    advance=False,
                )
                if not save_ok:
                    st.stop()

                if ordered_tables and current_idx < len(ordered_tables) - 1:
                    set_active_table_by_index(current_idx + 1, ordered_tables, schema_name)
                    st.success(f"Plan saved. Moving to {ordered_tables[m['active_table_index']]}...")
                else:
                    st.success("✅ Table configuration saved.")
                m["current_plan_data"][f"{schema_name}.{table_name}"]["dirty"] = False

            st.caption(
                "Finalize: Saves the final state, clears the active session, and prepares the app for a new project."
            )
    else:
        st.caption("Global actions become available after a plan is initialized.")

    st.markdown("### Review")
    unsaved_tables = _get_unsaved_tables()
    ordered_tables_for_execution = m.get("all_tables_list", []) or m.get("selected_tables", [])
    sensitive_keep_violations = _collect_sensitive_keep_violations(
        db,
        selected_schema,
        ordered_tables_for_execution,
    )
    if unsaved_tables:
        pending_names = [str(t).split(".")[-1] for t in unsaved_tables]
        st.warning(f"Pending: {', '.join(pending_names)}")
    if sensitive_keep_violations:
        st.error(
            "Execution blocked. Sensitive columns still set to `keep`: "
            + ", ".join(sensitive_keep_violations)
        )

    save_all_label = f"💾 Save Changes for {len(unsaved_tables)} Tables"
    if st.button(
        save_all_label,
        type="primary",
        width="stretch",
        key="save_all_unsaved_tables_btn",
        disabled=(len(unsaved_tables) == 0),
    ):
        failed_tables = []
        for table_key in unsaved_tables:
            table_state = m.get("current_plan_data", {}).get(table_key, {})
            plan_rows = table_state.get("plan", [])
            where_clause = table_state.get("where", "")
            try:
                schema_name, table_name = str(table_key).rsplit(".", 1)
            except ValueError:
                failed_tables.append(str(table_key))
                continue
            save_ok = db.save_ai_plan(
                schema_name=schema_name,
                table_name=table_name,
                plan_data=get_clean_plan(plan_rows),
                where_condition=str(where_clause or "").strip(),
            )
            if save_ok:
                m["current_plan_data"][table_key]["dirty"] = False
            else:
                failed_tables.append(table_name)
        if failed_tables:
            st.error(f"Failed saving: {', '.join(failed_tables)}")
        else:
            st.toast("✅ All table configurations are now synchronized with the plan.")

    write_mode_ui = st.selectbox(
        "Write Mode",
        options=["Overwrite (Truncate)", "Append"],
        index=0,
        key="execution_write_mode",
        help="Overwrite truncates target tables before insert. Append keeps existing rows and adds new ones.",
    )
    overwrite_clear_mode = "truncate_cascade"
    st.markdown(
        """
        <style>
        div[data-testid="stCheckbox"] input[type="checkbox"] {
            accent-color: #0078d4 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    truncate_before_migration = st.checkbox(
        "⚠️ Truncate target tables before migration",
        value=True,
        key="truncate_before_migration",
        help="Ensures the target database is clean before re-establishing Foreign Key constraints.",
    )
    if write_mode_ui.startswith("Overwrite"):
        overwrite_clear_mode_ui = st.selectbox(
            "Overwrite Clear Strategy",
            options=["TRUNCATE ... CASCADE (recommended)", "session_replication_role = replica"],
            index=0,
            key="overwrite_clear_mode_ui",
            help="Use TRUNCATE CASCADE for fast, constraint-safe clearing, or replica mode if your environment requires it.",
        )
        overwrite_clear_mode = (
            "session_replica"
            if overwrite_clear_mode_ui.startswith("session_replication_role")
            else "truncate_cascade"
        )
    summary_mode = "Overwrite" if write_mode_ui.startswith("Overwrite") else "Append"
    summary_strategy = (
        "N/A"
        if summary_mode == "Append"
        else (
            "session_replication_role = replica"
            if overwrite_clear_mode == "session_replica"
            else "TRUNCATE ... CASCADE"
        )
    )
    db_name = m.get("active_plan_db_name", "None")
    data_domain = m.get("data_domain", "General")
    st.info(
        f"Plan: {db_name} | Domain: {data_domain} | Write Mode: {summary_mode} | Clear Strategy: {summary_strategy}"
    )

    execute_disabled = (
        bool(unsaved_tables) or bool(sensitive_keep_violations) or (len(ordered_tables_for_execution) == 0)
    )
    consistency_seed_badge = str(m.get("consistency_check_seed", "")).strip()
    if consistency_seed_badge:
        st.caption(f"🧩 Consistency Check active | Plan seed: `{consistency_seed_badge}`")

    run_completed = bool(m.get("execution_completed", False))
    st.markdown("---")
    render_target_context_banner(db, context="exec")
    if not run_completed:
        st.caption("Complete anonymization run before closing the project.")
    else:
        run_at = m.get("execution_last_run_at", "unknown-time")
        run_db = m.get("execution_last_run_db_name", "unknown-db")
        run_host = m.get("execution_last_run_host", "unknown-host")
        st.markdown(
            f"""
            <div style="border:1px solid #0078d4; border-radius:8px; padding:0.55rem 0.75rem; background:#f0f7ff; color:#0b3d6e; margin-bottom:0.65rem;">
              <strong>Last Run</strong> | {run_at} | DB: <code>{run_db}</code> | Host: <code>{run_host}</code>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander("🔍 Verify Integrity", expanded=False):
        if st.button(
            "Run FK Integrity Check",
            key="run_fk_integrity_check_btn",
            width="stretch",
            disabled=not run_completed,
        ):
            try:
                violations = db.check_fk_integrity(
                    source_schema=selected_schema,
                    target_schema="anon",
                    ordered_tables=ordered_tables_for_execution,
                )
                if not violations:
                    st.success("✅ Referential Integrity Verified")
                else:
                    st.error("Foreign key integrity violations detected:")
                    for item in violations:
                        st.write(
                            f"- `{item['child_table']}.{item['child_column']}` -> "
                            f"`{item['parent_table']}.{item['parent_column']}` | "
                            f"orphan rows: {item['orphan_count']}"
                        )
            except Exception as e:
                st.error(f"FK integrity check failed: {e}")

    if unsaved_tables:
        st.warning("⚠️ Cannot finalize. You have unsaved changes in your table configurations.")

    st.markdown("---")
    cols = st.columns(2)
    with cols[0]:
        if st.button(
            "🚀 Execute Anonymization Pipeline",
            type="primary",
            use_container_width=True,
            key="global_run_all_tables_bottom",
            disabled=execute_disabled,
        ):
            plan_db_name, target_conn_url = _resolve_target_connection_from_plan(db)
            conn_url = str(target_conn_url or getattr(db, "target_db_url", "") or "")
            parsed = urlparse(conn_url)
            target_host = parsed.hostname or "unknown-host"
            target_db_name = plan_db_name or (parsed.path.lstrip("/") if parsed.path else "unknown-db")
            m["pending_execute_target_db_name"] = target_db_name
            m["pending_execute_target_host"] = target_host
            st.info(f"Targeting: {target_db_name} on {target_host}")
            logger.info("Targeting: %s on %s", target_db_name, target_host)
            mode = "overwrite" if write_mode_ui.startswith("Overwrite") else "append"
            render_execute_confirmation_dialog(
                db=db,
                schema_name=selected_schema,
                execution_order=ordered_tables_for_execution,
                progress_slot=review_progress_slot,
                status_slot=review_status_slot,
                write_mode=mode,
                overwrite_clear_mode=overwrite_clear_mode,
                truncate_before_migration=truncate_before_migration,
                execute_disabled=execute_disabled,
            )
    with cols[1]:
        if st.button(
            "🏁 Finalize & Close Project",
            type="primary" if run_completed else "secondary",
            use_container_width=True,
            key="finalize_close_project_btn",
            disabled=bool(unsaved_tables) or (not run_completed),
        ):
            render_finalize_confirmation_dialog(_finalize_close_project)
    st.caption(
        "🏁 Finalize: Saves the final plan metadata, closes the active database connections, and resets the "
        "application state for a new session. Ensure all tables are executed before finalizing."
    )
