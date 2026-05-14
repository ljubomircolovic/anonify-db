# -*- coding: utf-8 -*-
"""Per-table planner UI: plan resolution, editor, dirty-state."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from src.ui.tabs.planner.planner_components import render_planner_action_buttons
from src.ui.tabs.planner.planner_navigation import handle_navigation_history
from src.ui.tabs.planner.planner_validation import (
    _default_manual_plan,
    _find_multi_result,
    _get_live_preview_once,
    _get_persisted_plan_for_table,
    _get_quoted_value,
    _normalized_plan_rows,
    _set_current_plan_data,
    _table_plan_cache_key,
    _track_manual_overrides_for_table,
)


def render_planner_table_plan_editor(
    db: Any,
    m: dict[str, Any],
    *,
    selected_schema: str,
    all_fks: list[tuple[Any, ...]],
    violation_found: bool,
) -> tuple[bool, bool]:
    """
    Renders the main per-table plan editor block.

    Returns ``(continue_tab, violation_found)``. ``continue_tab`` is False when the
    parent should stop rendering (legacy early return on empty/invalid plan).
    """
    v = violation_found
    st.info("ℹ️ ID/PK/FK columns are protected. If AI proposes `mask`, it is automatically forced to `hash`.")
    if "selected_table_info" not in m:
        st.info("👋 Select a table to start.")
        return True, v

    table_info = m["selected_table_info"]
    table_name = table_info[0] if isinstance(table_info, tuple) else table_info
    schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
    current_saved_plan_meta = db.get_saved_plan(schema_name, table_name)
    if current_saved_plan_meta and current_saved_plan_meta.get("source_list_mismatch"):
        st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
    editor_key = f"plan_editor_{table_name}"
    table_cache_key = _table_plan_cache_key(schema_name, table_name)
    cached_active_plan = _get_persisted_plan_for_table(schema_name, table_name)

    multi_results = m.get("multi_ai_analysis", {})
    if "ai_analysis" not in m or m.get("last_rendered_table") != table_name:
        if cached_active_plan:
            m["ai_analysis"] = cached_active_plan
            m["last_rendered_table"] = table_name
            m["plan_active"] = True
        else:
            found_res = _find_multi_result(multi_results, table_name, schema_name)
            found_plan = (
                found_res.get("plan")
                if isinstance(found_res, dict)
                else getattr(found_res, "plan", None)
                if found_res
                else None
            )
            found_audit = (
                found_res.get("audit", [])
                if isinstance(found_res, dict)
                else getattr(found_res, "audit", [])
                if found_res
                else []
            )
            found_error = found_res.get("error") if isinstance(found_res, dict) else None
            if found_plan:
                m["ai_analysis"] = found_plan
                m["last_ai_audit"] = found_audit or []
                m["last_rendered_table"] = table_name
                m["plan_active"] = True
            else:
                if found_error:
                    v = True
                    st.error(f"❌ Parallel scan failed for `{table_name}`: {found_error}")
                saved_data = db.get_saved_plan(schema_name, table_name)
                if saved_data:
                    if saved_data.get("source_list_mismatch"):
                        st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
                    m["ai_analysis"] = saved_data["plan"]
                    m["plan_snapshot"] = saved_data["plan"]
                    m[f"where_clause_{table_name}"] = saved_data["where"]
                    m["plan_origin"] = "saved"
                    m["last_rendered_table"] = table_name
                    m["plan_active"] = True
                    m["active_plan_by_table"][table_cache_key] = saved_data["plan"]
                    if "active_plan" not in m or not isinstance(m["active_plan"], dict):
                        m["active_plan"] = {}
                    m["active_plan"][table_name] = {"mappings": saved_data["plan"]}
                else:
                    if "ai_analysis" in m:
                        del m["ai_analysis"]
                    manual_plan = _default_manual_plan(db, schema_name, table_name)
                    m["ai_analysis"] = manual_plan
                    m["active_plan_by_table"][table_cache_key] = manual_plan
                    if "active_plan" not in m or not isinstance(m["active_plan"], dict):
                        m["active_plan"] = {}
                    m["active_plan"][table_name] = {"mappings": manual_plan}
                    m["last_rendered_table"] = table_name
                    m["plan_active"] = False

    handle_navigation_history(table_name, schema_name)
    current_table_fks = [fk[1] for fk in all_fks if fk[0] == table_name]
    if f"pk_{table_name}" not in m:
        m[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)
    real_pks = m[f"pk_{table_name}"]

    left_col, right_col = st.columns([1, 2.2], gap="large")
    with left_col:
        render_planner_action_buttons(db, table_name, schema_name)
        if st.button("🗑️ Clear AI Suggestions", width="stretch", key=f"clear_ai_{schema_name}_{table_name}"):
            cleared_plan = _default_manual_plan(db, schema_name, table_name)
            m["ai_analysis"] = cleared_plan
            m["current_plan"] = cleared_plan
            m["active_plan_by_table"][table_cache_key] = cleared_plan
            if "active_plan" not in m or not isinstance(m["active_plan"], dict):
                m["active_plan"] = {}
            m["active_plan"][table_name] = {"mappings": cleared_plan}
            if "manual_overrides_by_table" in m:
                m["manual_overrides_by_table"][table_name] = set()
    with right_col:
        if "ai_analysis" in m and m["ai_analysis"]:
            plan_df = pd.DataFrame(m["ai_analysis"])
            if plan_df.empty:
                st.warning("⚠️ Anonymization plan is empty. Run scan again.")
                return False, True
            available_cols = plan_df.columns.tolist()
            col_key = next((c for c in available_cols if c.lower() == "column"), None)
            if col_key is None:
                st.error(f"❌ Data structure error. Found columns: {available_cols}")
                return False, True

            def get_col_status(col):
                if col in real_pks:
                    return "🔑 PK (Locked)"
                if col in current_table_fks:
                    return "🔗 FK (Dependent)"
                return "✅ Normal"

            plan_df["status"] = plan_df[col_key].apply(get_col_status)
            plan_df["is_sensitive"] = plan_df.apply(
                lambda r: bool(r.get("is_sensitive", r.get("is_pii", False))),
                axis=1,
            )
            plan_df["guard"] = plan_df[col_key].apply(
                lambda c: "ℹ️ Mask disabled for ID safety"
                if any(token in str(c).lower() for token in ["id", "pk", "fk"])
                else ""
            )
            locked_mask = plan_df["status"].str.contains("Locked|Dependent", na=False)
            id_name_mask = plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
            if "strategy" in plan_df.columns:
                plan_df.loc[(locked_mask | id_name_mask) & (plan_df["strategy"] == "mask"), "strategy"] = "hash"
            editor_left, editor_center, editor_right = st.columns([0.2, 4, 0.2], gap="small")
            with editor_center:
                edited_plan_df = st.data_editor(
                    plan_df,
                    column_config={
                        "status": st.column_config.TextColumn("Status", disabled=True),
                        "is_sensitive": st.column_config.CheckboxColumn("Sensitive", disabled=True),
                        "guard": st.column_config.TextColumn("Constraint", disabled=True),
                        col_key: st.column_config.TextColumn("Column", disabled=True),
                        "strategy": st.column_config.SelectboxColumn(
                            "Strategy",
                            options=["keep", "hash", "mask", "null", "faker_name"],
                            required=True,
                        ),
                    },
                    hide_index=True,
                    key=editor_key,
                    width="stretch",
                )
            if "strategy" in edited_plan_df.columns:
                edited_id_mask = edited_plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
                edited_plan_df.loc[edited_id_mask & (edited_plan_df["strategy"] == "mask"), "strategy"] = "hash"
                if "is_sensitive" in edited_plan_df.columns:
                    sensitive_keep_mask = edited_plan_df["is_sensitive"] & (edited_plan_df["strategy"] == "keep")
                    if sensitive_keep_mask.any():
                        v = True
                        blocked_cols = edited_plan_df.loc[sensitive_keep_mask, col_key].astype(str).tolist()
                        st.error(
                            "Sensitive columns cannot use `keep`. Update strategies for: "
                            f"{', '.join(blocked_cols)}."
                        )
            m["current_plan"] = edited_plan_df.to_dict("records")
            _track_manual_overrides_for_table(table_name, m["current_plan"])
            m["active_plan_by_table"][table_cache_key] = m["current_plan"]
            if "active_plan" not in m or not isinstance(m["active_plan"], dict):
                m["active_plan"] = {}
            m["active_plan"][table_name] = {"mappings": m["current_plan"]}
        else:
            st.info("💡 Select a table and run analysis to view the anonymization plan.")

    where_key_table = f"where_clause_{table_name}"
    current_plan_rows = m.get("current_plan") or m.get("ai_analysis") or []
    current_where_val = m.get(where_key_table, "")
    saved_plan_rows = current_saved_plan_meta.get("plan", []) if current_saved_plan_meta else []
    saved_where_val = current_saved_plan_meta.get("where", "") if current_saved_plan_meta else ""
    is_dirty = _normalized_plan_rows(current_plan_rows) != _normalized_plan_rows(
        saved_plan_rows
    ) or str(current_where_val or "").strip() != str(saved_where_val or "").strip()
    _set_current_plan_data(
        schema_name=schema_name,
        table_name=table_name,
        plan_rows=current_plan_rows,
        where_clause=current_where_val,
        dirty=is_dirty,
    )
    if is_dirty:
        st.warning("⚠️ You have unsaved changes for this table.")
    return True, v


def render_planner_filter_preview_expander(
    db: Any,
    m: dict[str, Any],
    *,
    selected_schema: str,
) -> None:
    """Expander 3: SQL filter, integrity lock helpers, live preview."""
    with st.expander("3) 🔍 Filter, Consistency & Preview", expanded=False):
        if "selected_table_info" not in m:
            st.info("Select a table in Planning to configure filters.")
            return

        table_info = m["selected_table_info"]
        table_name = table_info[0] if isinstance(table_info, tuple) else table_info
        schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
        where_key = f"where_clause_{table_name}"
        current_table_col_details = db.get_column_details(table_name, schema_name)
        table_columns = list(current_table_col_details.keys())

        is_locked = m.get("global_lock_check", False)
        g_val = m.get("global_integrity_val", "1")
        preview_limit_col, preview_refresh_col = st.columns([3, 1], gap="small", vertical_alignment="bottom")
        with preview_limit_col:
            row_limit = st.number_input(
                "Row preview limit",
                min_value=1,
                max_value=1000,
                value=int(m.get(f"row_limit_{table_name}", 100)),
                step=1,
                help="Tip: focus this field, then use keyboard Up/Down arrows for precise adjustments.",
                key=f"row_limit_{table_name}",
            )
        with preview_refresh_col:
            refresh_preview = st.button(
                "👁️ Refresh Preview", width="stretch", key=f"refresh_preview_{table_name}"
            )
        m["last_limit_val"] = int(row_limit)
        integrity_where = None
        if is_locked:
            if "customer_id" in table_columns and table_name != "order_items":
                q_val = _get_quoted_value("customer_id", current_table_col_details, g_val)
                integrity_where = f"customer_id = {q_val}"
            elif table_name == "customers" and "id" in table_columns:
                q_val = _get_quoted_value("id", current_table_col_details, g_val)
                integrity_where = f"id = {q_val}"
            elif table_name == "order_items" and "order_id" in table_columns:
                orders_meta = db.get_column_details("orders", schema_name)
                q_val = _get_quoted_value("customer_id", orders_meta, g_val)
                integrity_where = (
                    f"order_id IN (SELECT order_id FROM {schema_name}.orders WHERE customer_id = {q_val})"
                )

        if is_locked and integrity_where:
            deep_sync_col, deep_sync_help_col = st.columns([12, 1], gap="small")
            with deep_sync_col:
                use_suggestion = st.checkbox(
                    "Use deep integrity sync", value=True, key=f"suggest_chk_{table_name}"
                )
            with deep_sync_help_col:
                st.markdown(
                    '<div style="padding-top: 0.35rem; font-size: 1.05rem;" title="Performs recursive updates across related tables to maintain referential integrity.">⭐</div>',
                    unsafe_allow_html=True,
                )
            if use_suggestion:
                if m.get(where_key) != integrity_where:
                    m[where_key] = integrity_where
                st.info(f"⛓️ **Deep RI Lock Active:** `{integrity_where}`")
                st.text_input(
                    "Active SQL Filter:", value=integrity_where, disabled=True, key=f"locked_in_{where_key}"
                )
            else:
                m[where_key] = st.text_input(
                    "SQL WHERE clause (custom)",
                    value=m.get(where_key, ""),
                    key=f"in_custom_{where_key}",
                )
        else:
            m[where_key] = st.text_input(
                "SQL WHERE clause (independent)",
                value=m.get(where_key, ""),
                key=f"in_{where_key}",
            )

        st.divider()
        st.markdown("#### Live Data Preview")

        live_preview_df = _get_live_preview_once(
            db=db,
            schema_name=schema_name,
            table_name=table_name,
            where_clause=m.get(where_key, ""),
            row_limit=int(row_limit),
            force_refresh=bool(refresh_preview),
        )
        st.dataframe(live_preview_df, height=200, width="stretch")
