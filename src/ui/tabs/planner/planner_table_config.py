# -*- coding: utf-8 -*-
"""Planner tab: parallel AI scan, per-table editor, execution controls."""

from __future__ import annotations

import html
import json
import logging
import time

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.logic.app_state import AppState
from src.ui.planner import analyze_tables_parallel
from src.ui.tabs.planner.planner_actions import render_planner_actions_block
from src.ui.tabs.planner.planner_header import render_target_context_banner
from src.ui.tabs.planner.planner_validation import (
    _default_manual_plan,
    _find_multi_result,
    _get_persisted_plan_for_table,
    _merge_ai_into_existing_plan,
    _persist_current_plan_for_table,
    _pick_first_table_by_execution_order,
    _table_plan_cache_key,
)
from src.ui.tabs.planner.table_render_utils import (
    render_planner_filter_preview_expander,
    render_planner_table_plan_editor,
)

logger = logging.getLogger(__name__)


def render_planner_tab(db, app: AppState | None = None) -> None:
    """Render the Plan tab (strategy, roadmap, editor, execution)."""
    app = app or AppState()
    m = app.mapping

    st.subheader("Parallel AI Strategy Planner")
    st.caption("Scan selected tables first, then review and refine anonymization plans in dependency order.")
    render_target_context_banner(db, context="planner")
    review_progress_slot = st.empty()
    review_status_slot = st.empty()
    # Always initialize early so downstream button disabled state is safe.
    violation_found = False
    selected_schema = m.get('selected_schema', 'public')
    if 'active_plan_by_table' not in m:
        m['active_plan_by_table'] = {}
    if 'current_plan_data' not in m:
        m['current_plan_data'] = {}
    if 'active_table_index' not in m:
        m['active_table_index'] = 0
    if 'current_table_index' not in m:
        m['current_table_index'] = m['active_table_index']

    def _set_active_table_by_index(index, ordered_tables, schema_name):
        """Single source of truth navigation setter based on execution order index."""
        if not ordered_tables:
            return

        safe_index = max(0, min(index, len(ordered_tables) - 1))
        next_table = ordered_tables[safe_index]
        m['active_table_index'] = safe_index
        m['current_table_index'] = safe_index
        m['selected_table_info'] = (next_table, schema_name)

        multi_results = m.get('multi_ai_analysis', {})
        found_res = _find_multi_result(multi_results, next_table, schema_name)
        found_plan = found_res.get('plan') if isinstance(found_res, dict) else getattr(found_res, 'plan', None) if found_res else None
        found_audit = found_res.get('audit', []) if isinstance(found_res, dict) else getattr(found_res, 'audit', []) if found_res else []
        found_error = found_res.get('error') if isinstance(found_res, dict) else None

        if found_plan:
            m['ai_analysis'] = found_plan
            m['last_ai_audit'] = found_audit or []
            m['last_rendered_table'] = next_table
            m['plan_active'] = True
        else:
            if found_error:
                st.warning(f"⚠️ Parallel scan failed for `{next_table}`: {found_error}")
            saved_data = db.get_saved_plan(schema_name, next_table)
            if saved_data:
                if saved_data.get("source_list_mismatch"):
                    st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
                m['ai_analysis'] = saved_data['plan']
                m['plan_snapshot'] = saved_data['plan']
                m[f"where_clause_{next_table}"] = saved_data['where']
                m['plan_origin'] = 'saved'
                m['last_rendered_table'] = next_table
                m['plan_active'] = True
            else:
                m.pop('ai_analysis', None)
                m.pop('last_rendered_table', None)
                m['plan_active'] = False

        m.pop('current_plan', None)

    def _go_to_table_by_index(target_index, ordered_tables, schema_name):
        current_table_info = m.get('selected_table_info')
        if current_table_info:
            prev_table = current_table_info[0] if isinstance(current_table_info, tuple) else current_table_info
            prev_schema = current_table_info[1] if isinstance(current_table_info, tuple) else schema_name
            _persist_current_plan_for_table(prev_schema, prev_table)
        _set_active_table_by_index(target_index, ordered_tables, schema_name)
        st.rerun()

    def _render_execution_roadmap(ordered_tables, completed_tables, schema_name):
        if not ordered_tables:
            return
        st.markdown("**Execution Roadmap**")
        cols = st.columns(min(5, len(ordered_tables)))
        current_idx_local = m.get('current_table_index', m.get('active_table_index', 0))
        for idx, t_name in enumerate(ordered_tables):
            icon = "✅" if t_name in completed_tables else "⏳"
            label = f"{icon} {t_name}"
            with cols[idx % len(cols)]:
                if st.button(
                    label,
                    key=f"roadmap_btn_{schema_name}_{t_name}",
                    width="stretch",
                    type="primary" if idx == current_idx_local else "secondary"
                ):
                    m['current_table_index'] = idx
                    _go_to_table_by_index(idx, ordered_tables, schema_name)

    # 1. Load FK relations
    if "all_schema_fks_by_schema" not in m:
        m["all_schema_fks_by_schema"] = {}
    if selected_schema not in m["all_schema_fks_by_schema"]:
        m["all_schema_fks_by_schema"][selected_schema] = db.get_all_foreign_keys(selected_schema)
    all_fks = m["all_schema_fks_by_schema"][selected_schema]

    # Initialize plan_active in session state
    if 'plan_active' not in m:
        m['plan_active'] = False

    # 2. Global integrity settings (sidebar)
    with st.sidebar:
        lock_title_col, lock_help_col = st.columns([12, 1], gap="small")
        with lock_title_col:
            st.markdown("### ⛓️ Integrity Lock Settings")
        with lock_help_col:
            st.markdown(
                '<div style="padding-top: 0.6rem; font-size: 1.1rem;" title="Controls foreign key enforcement. Strict ensures all relations are valid.">⭐</div>',
                unsafe_allow_html=True
            )
        global_lock = st.checkbox("Force Referential Integrity", value=True, key="global_lock_check")
        global_integrity_val = st.text_input("Global ID Sync:", value="1", key="global_integrity_val")

    multi_scan_errors = {
        table_key: table_result.get('error')
        for table_key, table_result in m.get('multi_ai_analysis', {}).items()
        if isinstance(table_result, dict) and table_result.get('error')
    }
    if multi_scan_errors:
        st.warning("⚠️ Some tables failed during Parallel AI Scan.")
        for table_key, err_msg in multi_scan_errors.items():
            st.error(f"`{table_key}`: {err_msg}")

    # --- 1. Global analysis ---
    available_tables = db.get_tables(schema_name=selected_schema)

    # Ensure 'completed_tables' is initialized
    if 'completed_tables' not in m:
        m['completed_tables'] = set()

    # Populate 'completed_tables' from saved plans on initial load/rerun
    # This ensures tables with saved plans are marked complete
    for table_name_check in available_tables:
        if db.get_saved_plan(schema_name=selected_schema, table_name=table_name_check):
            m['completed_tables'].add(table_name_check)

    # Combine selected and completed tables for multiselect default
    current_user_selection = m.get('selected_tables', [])
    initial_multiselect_default = list(set(current_user_selection) | m['completed_tables'])
    # Filter out tables that no longer exist
    initial_multiselect_default = [t for t in initial_multiselect_default if t in available_tables]

    all_tables_list = m.get('all_tables_list', [])
    completed_tables = m.get('completed_tables', set())

    with st.expander("1) ⚙️ Anonymization Strategy & Design", expanded=True):
        st.caption(f"Schema: `{selected_schema}`")
        force_overwrite_manual = st.checkbox(
            "Force Overwrite Manual Changes",
            value=False,
            key="force_overwrite_manual_changes"
        )
        st.caption("AI will update only checked tables. Manual-edited rows remain protected unless overwrite is enabled.")

        ai_scope_tables = m.get('all_tables_list', []) or m.get('selected_tables', [])
        m["ai_scope_tables_current"] = list(ai_scope_tables)

        def _on_select_all_ai_change():
            desired = bool(m.get("select_all_ai", False))
            for _table_name in m.get("ai_scope_tables_current", []):
                m[f"ai_include_{_table_name}"] = desired

        def _on_individual_ai_change():
            table_names = m.get("ai_scope_tables_current", [])
            all_checked = all(bool(m.get(f"ai_include_{t_name}", False)) for t_name in table_names)
            m["select_all_ai"] = all_checked

        if ai_scope_tables:
            with st.container():
                st.markdown("**Include in AI Scan**")
                include_keys = [f"ai_include_{t_name}" for t_name in ai_scope_tables]
                all_checked_now = all(bool(m.get(k, True)) for k in include_keys)
                if "select_all_ai" not in m:
                    m["select_all_ai"] = all_checked_now
                st.checkbox(
                    "✅ Select/Deselect All",
                    key="select_all_ai",
                    on_change=_on_select_all_ai_change
                )
                include_cols = st.columns(4)
                for i, t_name in enumerate(ai_scope_tables):
                    include_key = f"ai_include_{t_name}"
                    if include_key not in m:
                        m[include_key] = True
                    with include_cols[i % len(include_cols)]:
                        st.checkbox(t_name, key=include_key, on_change=_on_individual_ai_change)

        if m.pop("trigger_unified_scan", False):
            selected_tables_set = set(m.get('selected_tables', []) or [])
            candidate_tables = m.get('all_tables_list', []) or m.get('selected_tables', [])
            tables_to_scan = [
                t for t in candidate_tables
                if t in selected_tables_set and m.get(f"ai_include_{t}", True)
            ]
            if not tables_to_scan:
                st.warning("No tables selected for AI scan. Enable 'Include in AI Scan' for at least one table.")
            else:
                with st.status("Running unified AI scan...", expanded=True) as scan_status:
                    scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                    scan_status.write("Submitting unified request...")
                    all_results = analyze_tables_parallel(
                        db, tables_to_scan, schema=selected_schema,
                        allow_sampling=bool(m.get("bulk_allow_sample", True)),
                        sample_limit=int(m.get("bulk_sample_rows", 5))
                    )
                    scan_status.write("Collecting results...")
                    existing_results = m.get('multi_ai_analysis', {})
                    merged_results = dict(existing_results) if isinstance(existing_results, dict) else {}
                    if "last_ai_plan_by_table" not in m:
                        m["last_ai_plan_by_table"] = {}

                    for t_name in tables_to_scan:
                        ai_entry = all_results.get(t_name)
                        if not isinstance(ai_entry, dict) or not ai_entry.get("plan"):
                            merged_results[t_name] = ai_entry
                            continue
                        current_plan = _get_persisted_plan_for_table(selected_schema, t_name) or _default_manual_plan(db, selected_schema, t_name)
                        merged_plan = _merge_ai_into_existing_plan(
                            existing_plan=current_plan,
                            ai_plan=ai_entry.get("plan", []),
                            table_name=t_name,
                            force_overwrite=bool(force_overwrite_manual),
                        )
                        merged_results[t_name] = {"plan": merged_plan, "audit": ai_entry.get("audit", [])}
                        m["last_ai_plan_by_table"][t_name] = ai_entry.get("plan", [])
                        m['active_plan_by_table'][_table_plan_cache_key(selected_schema, t_name)] = merged_plan
                        if 'active_plan' not in m or not isinstance(m['active_plan'], dict):
                            m['active_plan'] = {}
                        m['active_plan'][t_name] = {"mappings": merged_plan}

                    m['multi_ai_analysis'] = merged_results
                    m['plan_active'] = True
                    scan_status.update(label="Unified AI scan completed", state="complete")
                    st.rerun()

        selected_multi_tables = st.multiselect(
            "Tables for parallel scan",
            options=available_tables,
            default=initial_multiselect_default,
            key="planner_multiselect"
        )
        m['selected_tables'] = selected_multi_tables

        c1, c2 = st.columns([1, 2])
        with c1:
            bulk_allow_sampling = st.checkbox("Enable sampling", value=True, key="bulk_allow_sample")
        with c2:
            bulk_sample_rows = st.number_input(
                "Sample rows",
                min_value=1,
                max_value=5000,
                value=int(m.get("bulk_sample_rows", 5)),
                step=1,
                help="Tip: focus this field, then use keyboard Up/Down arrows for precise adjustments.",
                key="bulk_sample_rows"
            ) if bulk_allow_sampling else 0

        scan_btn_col1, scan_btn_col2 = st.columns(2)
        with scan_btn_col1:
            unified_scan_clicked = st.button(
                "⭐ 🤖 Suggest with AI (Unified Scan)",
                type="primary",
                use_container_width=True,
                disabled=not m.get('planning_initialized', False),
                key="explicit_unified_ai_scan_btn",
                help=(
                    "Unified Scan (Recommended for Integrity)\n\n"
                    "How: Sends all table schemas and samples in a single AI request.\n\n"
                    "Pros: AI understands Foreign Key relationships between tables; More cost-effective "
                    "(lower token overhead).\n\n"
                    "Cons: Limited by AI context window (max ~50-100 tables)."
                ),
            )
            if unified_scan_clicked:
                m["trigger_unified_scan"] = True
                st.rerun()
        with scan_btn_col2:
            parallel_scan_clicked = st.button(
                "⭐ 🪄 Parallel AI Scan",
                type="secondary",
                use_container_width=True,
                disabled=not selected_multi_tables,
                key="parallel_ai_scan_btn_row",
                help=(
                    "Parallel AI Scan (Recommended for Scale)\n\n"
                    "How: Each table is processed independently in parallel threads.\n\n"
                    "Pros: Extremely fast for large schemas; No context window limits.\n\n"
                    "Cons: AI doesn't see cross-table relationships; Slightly higher token usage due to "
                    "repeated prompts."
                ),
            )
            if parallel_scan_clicked:
                m["trigger_parallel_scan"] = True
                st.rerun()

        if m.pop("trigger_parallel_scan", False):
            with st.status("Running parallel scan...", expanded=True) as scan_status:
                tables_to_scan = m.get('planner_multiselect', selected_multi_tables)
                scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                scan_status.write("Submitting scan tasks...")
                all_results = analyze_tables_parallel(
                    db, tables_to_scan, schema=selected_schema,
                    allow_sampling=bulk_allow_sampling, sample_limit=bulk_sample_rows
                )
                scan_status.write("Collecting results...")
                m['multi_ai_analysis'] = all_results
                if all_results:
                    execution_order = m.get('all_tables_list', [])
                    if not execution_order:
                        execution_order = db.get_execution_order(tables_to_scan, selected_schema)
                        m['all_tables_list'] = execution_order

                    first_scanned_table = _pick_first_table_by_execution_order(
                        all_results.keys(),
                        execution_order
                    ) or str(next(iter(all_results.keys()))).split('.')[-1]
                    if first_scanned_table in execution_order:
                        _set_active_table_by_index(execution_order.index(first_scanned_table), execution_order, selected_schema)
                    else:
                        _set_active_table_by_index(0, execution_order, selected_schema)

                    first_result = _find_multi_result(all_results, first_scanned_table, selected_schema)
                    if first_result:
                        first_plan = first_result.get('plan') if isinstance(first_result, dict) else getattr(first_result, 'plan', None)
                        first_audit = first_result.get('audit', []) if isinstance(first_result, dict) else getattr(first_result, 'audit', [])
                        first_error = first_result.get('error') if isinstance(first_result, dict) else None
                        if first_plan:
                            m['ai_analysis'] = first_plan
                            m['last_ai_audit'] = first_audit or []
                            m['last_rendered_table'] = first_scanned_table
                            m['plan_active'] = True
                        else:
                            m.pop('ai_analysis', None)
                            m.pop('last_rendered_table', None)
                            if first_error:
                                st.warning(f"⚠️ First scanned table `{first_scanned_table}` failed: {first_error}")

                    m.pop('current_plan', None)
                st.success("Scan completed successfully.")
                m['plan_active'] = True
                scan_status.update(label="Parallel scan completed", state="complete")
                st.rerun()

        if 'selected_table_info' not in m and m.get('multi_ai_analysis'):
            multi_results = m['multi_ai_analysis']
            execution_order = m.get('all_tables_list', [])
            if not execution_order:
                fallback_selected = m.get('selected_tables', [])
                if fallback_selected:
                    execution_order = db.get_execution_order(fallback_selected, selected_schema)
                    m['all_tables_list'] = execution_order
            fallback_table = _pick_first_table_by_execution_order(multi_results.keys(), execution_order)
            if not fallback_table:
                first_result_key = next(iter(multi_results.keys()), None)
                fallback_table = str(first_result_key).split('.')[-1] if first_result_key else None
            if fallback_table:
                if fallback_table in execution_order:
                    _set_active_table_by_index(execution_order.index(fallback_table), execution_order, selected_schema)
                else:
                    _set_active_table_by_index(0, execution_order, selected_schema)

        all_tables_list = m.get('all_tables_list', [])
        completed_tables = m.get('completed_tables', set())
        _render_execution_roadmap(all_tables_list, completed_tables, selected_schema)

        selectable_tables = all_tables_list or m.get('selected_tables', [])
        if selectable_tables:
            max_idx = len(selectable_tables) - 1
            current_idx = m.get('current_table_index', m.get('active_table_index', 0))
            clamped_idx = max(0, min(current_idx, max_idx))
            if clamped_idx != current_idx:
                m['active_table_index'] = clamped_idx
                m['current_table_index'] = clamped_idx
            selected_active_table = st.selectbox("Planning table", selectable_tables, index=clamped_idx, label_visibility="collapsed")
            selected_idx = selectable_tables.index(selected_active_table)
            if selected_idx != m.get('current_table_index', m.get('active_table_index', 0)):
                _go_to_table_by_index(selected_idx, selectable_tables, selected_schema)

        cont, violation_found = render_planner_table_plan_editor(
            db,
            m,
            selected_schema=selected_schema,
            all_fks=all_fks,
            violation_found=violation_found,
        )
        if not cont:
            return

        if m.get('multi_ai_analysis'):
            scan_results = m.get('multi_ai_analysis', {})
            raw_scan_results_json = html.escape(json.dumps(scan_results, indent=2, ensure_ascii=False))
            st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
            st.markdown(
                """
                <style>
                    details.system-trace > summary { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 0.55rem 0.75rem; cursor: pointer; font-weight: 600; list-style: none; }
                    details.system-trace > summary::-webkit-details-marker { display: none; }
                    details.system-trace > .system-trace-box { margin-top: 0.5rem; max-height: 400px; overflow-y: auto; border: 1px solid #2d2d2d; border-radius: 8px; background: #1e1e1e; padding: 0.7rem 0.85rem; }
                    details.system-trace > .system-trace-box pre { margin: 0; white-space: pre; }
                    details.system-trace > .system-trace-box code { font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; color: #93c5fd; font-size: 0.8rem; line-height: 1.35; }
                </style>
                """,
                unsafe_allow_html=True
            )
            st.markdown(
                f"""
                <details class="system-trace">
                    <summary>🛠️ System Trace: AI Scan Payload</summary>
                    <div class="system-trace-box"><pre><code>{raw_scan_results_json}</code></pre></div>
                </details>
                """,
                unsafe_allow_html=True
            )

    with st.expander("2) 🔗 Integrity & Relationships", expanded=False):
        p_col1, p_col2, p_col3 = st.columns(3)
        with p_col1:
            m['anonify_level'] = st.selectbox(
                "Anonify Level",
                options=["Balanced", "Strict", "Maximum"],
                index=["Balanced", "Strict", "Maximum"].index(m.get('anonify_level', 'Balanced'))
            )
        with p_col2:
            m['compliance_mode'] = st.selectbox(
                "Compliance Mode",
                options=["GDPR", "HIPAA"],
                index=["GDPR", "HIPAA"].index(m.get('compliance_mode', 'GDPR'))
            )
        with p_col3:
            m['seed_management'] = st.text_input(
                "Seed Management",
                value=m.get('seed_management', 'default_seed')
            )
        st.caption("Privacy settings are applied as policy guidance for planning and execution.")

    render_planner_filter_preview_expander(db, m, selected_schema=selected_schema)

    render_planner_actions_block(
        db,
        app,
        selected_schema=selected_schema,
        violation_found=violation_found,
        review_progress_slot=review_progress_slot,
        review_status_slot=review_status_slot,
        go_to_table_by_index=_go_to_table_by_index,
        set_active_table_by_index=_set_active_table_by_index,
    )

