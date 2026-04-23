# -*- coding: utf-8 -*-
# --- File imports ---
import streamlit as st
import pandas as pd
import time
import logging
from sqlalchemy import text
# Shared planner modules
from src.ui.planner import AnonymizationPlanner, analyze_tables_parallel
from src.ui.planner_logic import validate_plan_row, calculate_privacy_score, get_clean_plan
from src.ui.planner_components import render_status_chain, render_ai_audit_log
from src.ui.planner_navigation import handle_navigation_history, get_next_table_in_chain

logger = logging.getLogger(__name__)

# Helper function for dynamic quoting based on DDL type
def _get_quoted_value(column_name, col_details, value):
    if column_name in col_details:
        col_type = col_details[column_name]['type'].lower()
        if any(t in col_type for t in ['char', 'text', 'uuid', 'date', 'time', 'varchar']):
            return f"'{value}'"
    return str(value)



def save_and_move_to_next(db, table_name, schema_name, plan_data, where_clause="", advance=True):
    """
    Performs strict DDL validation, PII checks, and RI synchronization,
    saves the plan, and moves navigation to the next table.
    """
    # --- 0. Save progress feedback ---
    st.toast(f"⏳ Saving plan for {table_name}...", icon="💾")

    # --- 1. Compatibility definitions ---
    COMPATIBILITY = {
        "numeric": ["keep", "hash", "mapping", "noise", "null"],
        "text": ["keep", "hash", "mask", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "pii": ["keep", "hash", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "date": ["keep", "date_shift", "null"],
        "boolean": ["keep", "null"]
    }

    TYPE_GROUPS = {
        "int": "numeric", "bigint": "numeric", "numeric": "numeric", "double": "numeric",
        "date": "date", "timestamp": "date", "time": "date",
        "bool": "boolean"
    }

    try:
        col_details = db.get_column_details(table_name, schema_name)
        table_pk_columns = set(db.get_primary_keys(schema_name, table_name))
        actual_db_columns = list(col_details.keys())
        invalid_selections = []

        # Load all relations (global map) and all saved plans
        all_relations = db.get_all_foreign_keys(schema_name)
        all_saved_plans = db.get_all_saved_plans(schema_name)

        for row in plan_data:
            col_name = row.get('column', '')
            strategy = row.get('strategy', 'keep').lower()

            if col_name in col_details:
                col_info = col_details[col_name]
                sql_type = col_info['type'].lower()
                is_nullable = col_info['nullable']

                # --- VALIDATION 0: Strategy compatibility with DB type ---
                type_group = "text"
                for type_token, mapped_group in TYPE_GROUPS.items():
                    if type_token in sql_type:
                        type_group = mapped_group
                        break
                if strategy not in COMPATIBILITY.get(type_group, COMPATIBILITY["text"]):
                    invalid_selections.append(
                        f"❌ Column `{col_name}` ({sql_type}) does not support strategy `{strategy}`."
                    )
                    continue

                # --- VALIDATION 1: NOT NULL guard ---
                if strategy == 'null' and is_nullable == 'NO':
                    invalid_selections.append(f"❌ Column `{col_name}` is **NOT NULL**.")
                    continue

                # --- VALIDATION 2: PK/FK numeric IDs must stay KEEP ---
                is_numeric_id_type = any(token in sql_type for token in ['bigint', 'integer', 'smallint', 'int'])
                if is_numeric_id_type:
                    if col_name in table_pk_columns and strategy != 'keep':
                        invalid_selections.append(
                            f"❌ RI Conflict: `{col_name}` must be 'keep' to match referenced keys "
                            f"(e.g., `{table_name}.{col_name}`)."
                        )
                        continue

                # --- VALIDATION 3: Referential Integrity (FK & PK) ---
                for rel in all_relations:
                    # Case A: Current column is FK
                    if rel[0] == table_name and rel[1] == col_name:
                        parent_table, parent_col = rel[2], rel[3]
                        if is_numeric_id_type and strategy != 'keep':
                            invalid_selections.append(
                                f"❌ RI Conflict: `{col_name}` must be 'keep' to match referenced keys "
                                f"(e.g., `{parent_table}.{parent_col}`)."
                            )
                            continue
                        p_plan = all_saved_plans.get(parent_table)
                        if p_plan:
                            p_strat = next((p['strategy'] for p in p_plan if p['column'] == parent_col), 'keep').lower()
                            if strategy != p_strat:
                                invalid_selections.append(
                                    f"❌ RI Conflict: `{col_name}` must be `{p_strat}` to match referenced keys "
                                    f"(e.g., `{parent_table}.{parent_col}`)."
                                )

                    # Case B: Current column is PK
                    elif rel[2] == table_name and rel[3] == col_name:
                        child_table, child_col = rel[0], rel[1]
                        c_plan = all_saved_plans.get(child_table)
                        if c_plan:
                            c_strat = next((c['strategy'] for c in c_plan if c['column'] == child_col), 'keep').lower()
                            if strategy != c_strat:
                                invalid_selections.append(
                                    f"❌ RI Conflict: Child rows in `{child_table}` already use `{c_strat}`."
                                )

        if invalid_selections:
            st.error("🛑 **Integrity Violation** - Plan was not saved.")
            for err in invalid_selections:
                st.write(err)
            return False # Stop here if integrity validation fails

    except Exception as e:
        st.error(f"System validation error: {e}")
        return False

    # --- 6. Clean and save ---
    clean_plan = get_clean_plan(plan_data)
    safe_where = str(where_clause or "").strip()

    # --- 6b. Pre-save type validation using transformed sample ---
    try:
        raw_sample = db.read_table(table_name, schema_name, where=safe_where, limit=10)
        if not raw_sample.empty:
            anon_sample = db.apply_anonymization_rules(raw_sample, clean_plan)
            for col_name, col_meta in col_details.items():
                if col_name not in anon_sample.columns:
                    continue
                col_type = str(col_meta.get("type", "")).lower()
                if any(t in col_type for t in ["bigint", "integer", "smallint", "int"]):
                    series = anon_sample[col_name].dropna()
                    if not series.empty:
                        if not series.map(lambda v: isinstance(v, int) and not isinstance(v, bool)).all():
                            st.error(
                                f"❌ Type mismatch on `{col_name}`: anonymized values are not valid integers for `{col_type}`."
                            )
                            return False
                elif any(t in col_type for t in ["numeric", "double", "real", "decimal"]):
                    series = anon_sample[col_name].dropna()
                    if not series.empty:
                        if not series.map(lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)).all():
                            st.error(
                                f"❌ Type mismatch on `{col_name}`: anonymized values are not valid numbers for `{col_type}`."
                            )
                            return False
                elif "boolean" in col_type:
                    series = anon_sample[col_name].dropna()
                    if not series.empty and not series.map(lambda v: isinstance(v, bool)).all():
                        st.error(
                            f"❌ Type mismatch on `{col_name}`: anonymized values are not valid booleans."
                        )
                        return False
    except Exception as e:
        st.error(f"System type validation error: {e}")
        return False

    save_success = db.save_ai_plan(
        schema_name=schema_name,
        table_name=table_name,
        plan_data=clean_plan,
        where_condition=safe_where
    )

    if not save_success:
        st.error(f"❌ Critical error: Plan for `{table_name}` was not saved to the database.")
        return False

    # --- 7. Navigation ---
    # Add table to completed set
    if 'completed_tables' not in st.session_state:
        st.session_state['completed_tables'] = set()
    st.session_state['completed_tables'].add(table_name)

    # Ensure 'selected_tables' is updated to reflect the new completed table
    if 'selected_tables' in st.session_state and table_name not in st.session_state['selected_tables']:
        st.session_state['selected_tables'].append(table_name)

    if not advance:
        st.success(f"✅ Plan saved for {table_name}.")
        return True

    all_tables = st.session_state.get('all_tables_list', [])

    # Use chain-aware navigation
    next_table = get_next_table_in_chain(table_name, all_tables, st.session_state['completed_tables'])

    if next_table:
        # Check if the next table has a saved plan to determine if 'plan_active' should remain True
        next_table_plan = db.get_saved_plan(schema_name, next_table)
        if next_table_plan:
            st.session_state['plan_active'] = True
            # Preload the plan for the next table immediately
            st.session_state['ai_analysis'] = next_table_plan['plan']
            st.session_state['plan_snapshot'] = next_table_plan['plan']
            st.session_state[f"where_clause_{next_table}"] = next_table_plan['where']
            st.session_state['plan_origin'] = 'saved'
        else:
            st.session_state['plan_active'] = False

        st.session_state['selected_table_info'] = (next_table, schema_name)

        # Reset state for the next table
        keys_to_reset = ['ai_analysis', 'current_plan', 'last_rendered_table', 'plan_snapshot', 'plan_active']
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]

        st.success(f"✅ Saved! Moving to {next_table}...")
        time.sleep(0.5) # Short delay so the user can see the message
        st.rerun()
    else:
        st.success("🎯 All tables finalized! Ready for Batch execution.")
        st.session_state['plan_active'] = False # All tables finalized, no active plan
        time.sleep(1)
        st.rerun()
    return True

def render_explorer_tab(db):
    st.subheader("🔍 Data Inspection")
    st.caption("Use the live SQL-aware preview below to inspect current table data and filters.")

def render_fk_explanation():
    """Shows a detailed explanation of foreign key strategies."""
    st.markdown("""
    ### 📘 HASH vs KEEP: Referential Integrity

    When anonymizing related tables (e.g. `customers` and `orders`), keys must stay aligned.

    | Characteristic | **HASH (Recommended)** | **KEEP (Original ID)** |
    | :--- | :--- | :--- |
    | **Security** | **High.** ID cannot be reversed to original. | **Low.** IDs remain exposed. |
    | **Integrity** | **Perfect.** JOINs continue to work (with same salt). | **Perfect.** JOINs continue to work. |
    | **Data Type** | Becomes **String** (hash). | Remains **Integer**. |
    | **Risk** | Minimal. | High (possible linkage risk). |

    **Recommendation:** Use **HASH** for all foreign key columns to maximize privacy while preserving DB functionality.
    """)

def get_next_table_in_chain(current_table, all_tables, completed_tables):
    """
    Smart navigation that finds the next logical table to process.
    Priority:
    1. First unfinished table after current.
    2. If at end, return the first unfinished table earlier in the chain.
    """
    if not all_tables:
        return None

    # If completed_tables is not a set (e.g. None), initialize it
    if completed_tables is None:
        completed_tables = set()

    try:
        current_idx = all_tables.index(current_table)
    except ValueError:
        # If current table is missing from list, return first unfinished table
        for table in all_tables:
            if table not in completed_tables:
                return table
        return all_tables[0]

    # --- STEP 1: First unfinished table after current ---
    for next_table in all_tables[current_idx + 1:]:
        if next_table not in completed_tables:
            return next_table

    # --- STEP 2: If at end, check if any unfinished tables remain before current ---
    # Important when user jumps around using sidebar
    for table in all_tables:
        if table not in completed_tables:
            return table

    # --- STEP 3: All tables completed ---
    return None

def render_planner_action_buttons(db, table_name, schema_name):
    """Renders stacked action buttons and audit log."""
    allow_sampling = st.checkbox("Enable sampling", value=True, key=f"sample_check_{table_name}")
    sample_rows = st.slider("Sample rows", 1, 20, 5) if allow_sampling else 0

    if st.button("🤖 AI Scan", width="stretch", type="secondary", key=f"ai_btn_{table_name}"):
        with st.spinner("Consulting AI..."):
            planner = AnonymizationPlanner(db)
            ai_plan, audit_data = planner.generate_suggestion_plan(schema_name, table_name, allow_sampling, sample_rows)
            if ai_plan:
                st.session_state['ai_analysis'] = ai_plan
                st.session_state['last_ai_audit'] = audit_data
                st.session_state['plan_active'] = True
                st.rerun()

    if st.button("📂 Load Saved", width="stretch", key=f"load_btn_{table_name}"):
        saved_data = db.get_saved_plan(schema_name, table_name)
        if saved_data:
            st.session_state['ai_analysis'] = saved_data['plan']
            st.session_state['plan_snapshot'] = saved_data['plan']
            st.session_state[f"where_clause_{table_name}"] = saved_data['where']
            st.session_state['plan_origin'] = 'saved'
            st.session_state['plan_active'] = True
            st.rerun()

    if st.button("✍️ Manual", width="stretch", key=f"man_btn_{table_name}"):
        columns = db.get_columns(table_name, schema_name)
        st.session_state['ai_analysis'] = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
        st.session_state['plan_origin'] = 'new'
        st.session_state['plan_active'] = True
        st.rerun()

    if st.button("👁️ View Data", width="stretch", key=f"view_btn_{table_name}"):
        st.info("Live Preview is available below.")

    # Important: render audit log immediately below action buttons
    render_ai_audit_log()

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

def render_planner_tab(db):
    st.subheader("Parallel AI Strategy Planner")
    st.caption("Scan selected tables first, then review and refine anonymization plans in dependency order.")
    selected_schema = st.session_state.get('selected_schema', 'public')
    if 'active_table_index' not in st.session_state:
        st.session_state['active_table_index'] = 0

    def _set_active_table_by_index(index, ordered_tables, schema_name):
        """Single source of truth navigation setter based on execution order index."""
        if not ordered_tables:
            return

        safe_index = max(0, min(index, len(ordered_tables) - 1))
        next_table = ordered_tables[safe_index]
        st.session_state['active_table_index'] = safe_index
        st.session_state['selected_table_info'] = (next_table, schema_name)

        multi_results = st.session_state.get('multi_ai_analysis', {})
        found_res = _find_multi_result(multi_results, next_table, schema_name)
        found_plan = found_res.get('plan') if isinstance(found_res, dict) else getattr(found_res, 'plan', None) if found_res else None
        found_audit = found_res.get('audit', []) if isinstance(found_res, dict) else getattr(found_res, 'audit', []) if found_res else []
        found_error = found_res.get('error') if isinstance(found_res, dict) else None

        if found_plan:
            st.session_state['ai_analysis'] = found_plan
            st.session_state['last_ai_audit'] = found_audit or []
            st.session_state['last_rendered_table'] = next_table
            st.session_state['plan_active'] = True
        else:
            if found_error:
                st.warning(f"⚠️ Parallel scan failed for `{next_table}`: {found_error}")
            saved_data = db.get_saved_plan(schema_name, next_table)
            if saved_data:
                st.session_state['ai_analysis'] = saved_data['plan']
                st.session_state['plan_snapshot'] = saved_data['plan']
                st.session_state[f"where_clause_{next_table}"] = saved_data['where']
                st.session_state['plan_origin'] = 'saved'
                st.session_state['last_rendered_table'] = next_table
                st.session_state['plan_active'] = True
            else:
                st.session_state.pop('ai_analysis', None)
                st.session_state.pop('last_rendered_table', None)
                st.session_state['plan_active'] = False

        st.session_state.pop('current_plan', None)

    # 1. Load FK relations
    if "all_schema_fks_by_schema" not in st.session_state:
        st.session_state["all_schema_fks_by_schema"] = {}
    if selected_schema not in st.session_state["all_schema_fks_by_schema"]:
        st.session_state["all_schema_fks_by_schema"][selected_schema] = db.get_all_foreign_keys(selected_schema)
    all_fks = st.session_state["all_schema_fks_by_schema"][selected_schema]

    # Initialize plan_active in session state
    if 'plan_active' not in st.session_state:
        st.session_state['plan_active'] = False

    # 2. Global integrity settings (sidebar)
    with st.sidebar:
        st.markdown("### ⛓️ Integrity Lock Settings")
        global_lock = st.checkbox("Force Referential Integrity", value=True, key="global_lock_check")
        global_integrity_val = st.text_input("Global ID Sync:", value="1", key="global_integrity_val")

    multi_scan_errors = {
        table_key: table_result.get('error')
        for table_key, table_result in st.session_state.get('multi_ai_analysis', {}).items()
        if isinstance(table_result, dict) and table_result.get('error')
    }
    if multi_scan_errors:
        st.warning("⚠️ Some tables failed during Parallel AI Scan.")
        for table_key, err_msg in multi_scan_errors.items():
            st.error(f"`{table_key}`: {err_msg}")

    # --- 1. Global analysis ---
    available_tables = db.get_tables(schema_name=selected_schema)

    # Ensure 'completed_tables' is initialized
    if 'completed_tables' not in st.session_state:
        st.session_state['completed_tables'] = set()

    # Populate 'completed_tables' from saved plans on initial load/rerun
    # This ensures tables with saved plans are marked complete
    for table_name_check in available_tables:
        if db.get_saved_plan(schema_name=selected_schema, table_name=table_name_check):
            st.session_state['completed_tables'].add(table_name_check)

    # Combine selected and completed tables for multiselect default
    current_user_selection = st.session_state.get('selected_tables', [])
    initial_multiselect_default = list(set(current_user_selection) | st.session_state['completed_tables'])
    # Filter out tables that no longer exist
    initial_multiselect_default = [t for t in initial_multiselect_default if t in available_tables]

    with st.expander("1) 🪄 Scanning", expanded=True):
        st.caption(f"Schema: `{selected_schema}`")
        selected_multi_tables = st.multiselect("Tables for parallel scan",
                                               options=available_tables,
                                               default=initial_multiselect_default,
                                               key="planner_multiselect")
        st.session_state['selected_tables'] = selected_multi_tables

        c1, c2 = st.columns([1, 2])
        with c1:
            bulk_allow_sampling = st.checkbox("Enable sampling", value=True, key="bulk_allow_sample")
        with c2:
            bulk_sample_rows = st.slider("Sample rows", 1, 20, 5, key="bulk_sample_rows") if bulk_allow_sampling else 0

        if st.button("🪄 Parallel AI Scan", disabled=not selected_multi_tables, type="primary"):
            with st.status("Running parallel scan...", expanded=True) as scan_status:
                tables_to_scan = st.session_state.get('planner_multiselect', selected_multi_tables)
                scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                scan_status.write("Submitting scan tasks...")
                all_results = analyze_tables_parallel(db, tables_to_scan, schema=selected_schema,
                                                   allow_sampling=bulk_allow_sampling, sample_limit=bulk_sample_rows)
                scan_status.write("Collecting results...")
                st.session_state['multi_ai_analysis'] = all_results
                if all_results:
                    execution_order = st.session_state.get('all_tables_list', [])
                    if not execution_order:
                        execution_order = db.get_execution_order(tables_to_scan, selected_schema)
                        st.session_state['all_tables_list'] = execution_order

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
                            st.session_state['ai_analysis'] = first_plan
                            st.session_state['last_ai_audit'] = first_audit or []
                            st.session_state['last_rendered_table'] = first_scanned_table
                            st.session_state['plan_active'] = True
                        else:
                            st.session_state.pop('ai_analysis', None)
                            st.session_state.pop('last_rendered_table', None)
                            if first_error:
                                st.warning(f"⚠️ First scanned table `{first_scanned_table}` failed: {first_error}")

                    st.session_state.pop('current_plan', None)
                st.success("Scan completed successfully.")
                st.session_state['plan_active'] = True
                scan_status.update(label="Parallel scan completed", state="complete")
                st.rerun()

        if st.session_state.get('multi_ai_analysis'):
            st.write("**Scan results**")
            st.json(st.session_state.get('multi_ai_analysis', {}))

    # --- 2. Progress chain ---
    all_tables_list = st.session_state.get('all_tables_list', [])
    completed_tables = st.session_state.get('completed_tables', set())

    # --- 3. Single-table workflow ---
    if 'selected_table_info' not in st.session_state and st.session_state.get('multi_ai_analysis'):
        multi_results = st.session_state['multi_ai_analysis']
        execution_order = st.session_state.get('all_tables_list', [])
        if not execution_order:
            fallback_selected = st.session_state.get('selected_tables', [])
            if fallback_selected:
                execution_order = db.get_execution_order(fallback_selected, selected_schema)
                st.session_state['all_tables_list'] = execution_order

        fallback_table = _pick_first_table_by_execution_order(
            multi_results.keys(),
            execution_order
        )
        if not fallback_table:
            first_result_key = next(iter(multi_results.keys()), None)
            fallback_table = str(first_result_key).split('.')[-1] if first_result_key else None

        if fallback_table:
            if fallback_table in execution_order:
                _set_active_table_by_index(execution_order.index(fallback_table), execution_order, selected_schema)
            else:
                _set_active_table_by_index(0, execution_order, selected_schema)

    remaining_count = max(0, len(all_tables_list) - len(completed_tables)) if all_tables_list else 0
    planning_expanded = remaining_count > 0 and bool(st.session_state.get('selected_table_info') or st.session_state.get('multi_ai_analysis'))
    with st.expander("2) 📋 Planning", expanded=planning_expanded):
        render_status_chain(all_tables_list, completed_tables)

        selectable_tables = all_tables_list or st.session_state.get('selected_tables', [])
        if selectable_tables:
            max_idx = len(selectable_tables) - 1
            current_idx = st.session_state.get('active_table_index', 0)
            clamped_idx = max(0, min(current_idx, max_idx))
            if clamped_idx != current_idx:
                st.session_state['active_table_index'] = clamped_idx
            selected_active_table = st.selectbox("Planning table", selectable_tables, index=clamped_idx, label_visibility="collapsed")
            selected_idx = selectable_tables.index(selected_active_table)
            if selected_idx != st.session_state.get('active_table_index', 0):
                _set_active_table_by_index(selected_idx, selectable_tables, selected_schema)
                st.rerun()

        st.info("ℹ️ ID/PK/FK columns are protected. If AI proposes `mask`, it is automatically forced to `hash`.")

        if 'selected_table_info' in st.session_state:
            table_info = st.session_state['selected_table_info']
            table_name = table_info[0] if isinstance(table_info, tuple) else table_info
            schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema

            # Initialize vital variables
            violation_found = False
            editor_key = f"plan_editor_{table_name}"
            where_key = f"where_clause_{table_name}"

            # Synchronization (auto-load)
            multi_results = st.session_state.get('multi_ai_analysis', {})
            if 'ai_analysis' not in st.session_state or st.session_state.get('last_rendered_table') != table_name:
                found_res = _find_multi_result(multi_results, table_name, schema_name)
                found_plan = found_res.get('plan') if isinstance(found_res, dict) else getattr(found_res, 'plan', None) if found_res else None
                found_audit = found_res.get('audit', []) if isinstance(found_res, dict) else getattr(found_res, 'audit', []) if found_res else []
                found_error = found_res.get('error') if isinstance(found_res, dict) else None
                if found_plan:
                    st.session_state['ai_analysis'] = found_plan
                    st.session_state['last_ai_audit'] = found_audit or []
                    st.session_state['last_rendered_table'] = table_name
                    st.session_state['plan_active'] = True  # Set plan active
                else:
                    if found_error:
                        st.error(f"❌ Parallel scan failed for `{table_name}`: {found_error}")
                    # No plan in multi_ai_analysis, try loading from saved DB plans
                    saved_data = db.get_saved_plan(schema_name, table_name)
                    if saved_data:
                        st.session_state['ai_analysis'] = saved_data['plan']
                        st.session_state['plan_snapshot'] = saved_data['plan']  # Keep plan_snapshot consistent
                        st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                        st.session_state['plan_origin'] = 'saved'
                        st.session_state['last_rendered_table'] = table_name
                        st.session_state['plan_active'] = True  # Set plan active
                    else:
                        # If plan is not found anywhere, clear ai_analysis and set plan_active False
                        if 'ai_analysis' in st.session_state:
                            del st.session_state['ai_analysis']
                        st.session_state['plan_active'] = False

            handle_navigation_history(table_name, schema_name)
            # FK/PK identification
            current_table_col_details = db.get_column_details(table_name, schema_name)
            current_table_fks = [fk[1] for fk in all_fks if fk[0] == table_name]
            if f"pk_{table_name}" not in st.session_state:
                st.session_state[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)
            real_pks = st.session_state[f"pk_{table_name}"]
            table_columns = list(current_table_col_details.keys())

            left_col, right_col = st.columns([1, 2.2], gap="large")

            with left_col:
                render_planner_action_buttons(db, table_name, schema_name)

            with right_col:
                # --- Data editor and validation ---
                if 'ai_analysis' in st.session_state and st.session_state['ai_analysis']:
                    plan_df = pd.DataFrame(st.session_state['ai_analysis'])

                    # If DataFrame is empty (no rows)
                    if plan_df.empty:
                        st.warning("⚠️ Anonymization plan is empty. Run scan again.")
                        return

                    # Defensive column validation
                    available_cols = plan_df.columns.tolist()
                    col_key = next((c for c in available_cols if c.lower() == 'column'), None)

                    if col_key is None:
                        st.error(f"❌ Data structure error. Found columns: {available_cols}")
                        return

                    def get_col_status(col):
                        if col in real_pks: return "🔑 PK (Locked)"
                        if col in current_table_fks: return "🔗 FK (Dependent)"
                        return "✅ Normal"

                    plan_df['status'] = plan_df[col_key].apply(get_col_status)
                    plan_df['guard'] = plan_df[col_key].apply(
                        lambda c: "ℹ️ Mask disabled for ID safety" if any(token in str(c).lower() for token in ["id", "pk", "fk"]) else ""
                    )

                    # Auto-fix for FK/PK columns: mask is not allowed on identifiers
                    locked_mask = plan_df['status'].str.contains("Locked|Dependent", na=False)
                    id_name_mask = plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
                    if 'strategy' in plan_df.columns:
                        plan_df.loc[(locked_mask | id_name_mask) & (plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'

                    editor_col_1, editor_col_2, editor_col_3 = st.columns([1, 1, 2], gap="small")
                    with editor_col_3:
                        edited_plan_df = st.data_editor(
                            plan_df,
                            column_config={
                                "status": st.column_config.TextColumn("Status", disabled=True),
                                "guard": st.column_config.TextColumn("Constraint", disabled=True),
                                col_key: st.column_config.TextColumn("Column", disabled=True),
                                "strategy": st.column_config.SelectboxColumn("Strategy", options=["keep", "hash", "mask", "null", "faker_name"], required=True),
                            },
                            hide_index=True, key=editor_key, width="stretch"
                        )
                    # Hard post-edit constraint: ID-like columns cannot remain as mask.
                    if 'strategy' in edited_plan_df.columns:
                        edited_id_mask = edited_plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
                        edited_plan_df.loc[edited_id_mask & (edited_plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'
                    st.session_state['current_plan'] = edited_plan_df.to_dict('records')

                    st.markdown("#### Live Data Preview")
                    preview_rows = st.slider(
                        "Preview rows",
                        min_value=5,
                        max_value=10,
                        value=5,
                        key=f"planner_live_preview_rows_{table_name}"
                    )
                    live_preview_df = db.read_table(
                        table_name,
                        schema_name,
                        where=st.session_state.get(where_key, ""),
                        limit=preview_rows
                    )
                    st.dataframe(live_preview_df, height=200, width="stretch")
                else:
                    st.info("💡 Select a table and run analysis to view the anonymization plan.")

                st.write("")
                if st.button("🚀 Run & Save to Anon", width="stretch", disabled=violation_found, key=f"run_btn_{table_name}"):
                    clean_plan = get_clean_plan(st.session_state.get('current_plan', []))
                    with st.spinner(f"Saving to anon.{table_name}..."):
                        try:
                            full_data = db.read_table(table_name, schema_name, where=st.session_state.get(where_key, ""))
                            final_df = db.apply_anonymization_rules(full_data, clean_plan)
                            db.save_anonymized_table(final_df, table_name, target_schema='anon', source_schema=schema_name)
                            st.success("✅ Migration completed successfully.")
                        except Exception as e:
                            st.error(f"Error: {e}")

                next_t = get_next_table_in_chain(table_name, all_tables_list, completed_tables)
                confirm_label = "💾 Confirm & Next" if next_t else "🏁 Finish"

            nav_cols = st.columns([1, 1, 2])
            ordered_tables = all_tables_list or selectable_tables
            current_idx = st.session_state.get('active_table_index', 0)
            current_idx = max(0, min(current_idx, len(ordered_tables) - 1)) if ordered_tables else -1
            with nav_cols[0]:
                if st.button("⬅️ Back", disabled=(current_idx <= 0), width="stretch", key=f"flow_back_{table_name}"):
                    _set_active_table_by_index(current_idx - 1, ordered_tables, schema_name)
                    st.rerun()
            with nav_cols[1]:
                if st.button("Next ➡️", disabled=not (ordered_tables and current_idx < len(ordered_tables) - 1), width="stretch", key=f"flow_next_{table_name}"):
                    _set_active_table_by_index(current_idx + 1, ordered_tables, schema_name)
                    st.rerun()
            with nav_cols[2]:
                if st.button(confirm_label, type="primary", width="stretch", disabled=violation_found, key=f"conf_btn_{table_name}"):
                    current_plan = st.session_state.get('current_plan') or st.session_state.get('ai_analysis')
                    current_table_ref = st.session_state.get('selected_table_info')
                    if current_plan is None or not current_table_ref:
                        st.error("No active table plan found to save.")
                        st.stop()

                    save_ok = save_and_move_to_next(
                        db,
                        table_name,
                        schema_name,
                        current_plan,
                        st.session_state.get(where_key, ""),
                        advance=False
                    )
                    if not save_ok:
                        st.stop()

                    current_idx = st.session_state.get('active_table_index', 0)
                    if ordered_tables and current_idx < len(ordered_tables) - 1:
                        _set_active_table_by_index(current_idx + 1, ordered_tables, schema_name)
                        st.success(f"Plan saved. Moving to {ordered_tables[st.session_state['active_table_index']]}...")
                    else:
                        st.success("🎉 All tables in the execution chain have been planned!")
                        st.session_state['plan_active'] = False

                    st.rerun()

            if all_tables_list:
                in_middle = 0 < current_idx < (len(all_tables_list) - 1)
                if not in_middle:
                    st.caption("Execution Order: " + " -> ".join(all_tables_list))
        else:
            st.info("👋 Select a table to start.")

    with st.expander("3) 🔒 Privacy", expanded=False):
        p_col1, p_col2, p_col3 = st.columns(3)
        with p_col1:
            st.session_state['anonify_level'] = st.selectbox(
                "Anonify Level",
                options=["Balanced", "Strict", "Maximum"],
                index=["Balanced", "Strict", "Maximum"].index(st.session_state.get('anonify_level', 'Balanced'))
            )
        with p_col2:
            st.session_state['compliance_mode'] = st.selectbox(
                "Compliance Mode",
                options=["GDPR", "HIPAA"],
                index=["GDPR", "HIPAA"].index(st.session_state.get('compliance_mode', 'GDPR'))
            )
        with p_col3:
            st.session_state['seed_management'] = st.text_input(
                "Seed Management",
                value=st.session_state.get('seed_management', 'default_seed')
            )
        st.caption("Privacy settings are applied as policy guidance for planning and execution.")

    with st.expander("4) ⚙️ Filter & Consistency", expanded=False):
        if 'selected_table_info' in st.session_state:
            table_info = st.session_state['selected_table_info']
            table_name = table_info[0] if isinstance(table_info, tuple) else table_info
            schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
            where_key = f"where_clause_{table_name}"
            current_table_col_details = db.get_column_details(table_name, schema_name)
            table_columns = list(current_table_col_details.keys())

            is_locked = st.session_state.get('global_lock_check', False)
            g_val = st.session_state.get('global_integrity_val', '1')
            row_limit = st.number_input("Row preview limit", min_value=10, max_value=1000, value=100, step=10, key=f"row_limit_{table_name}")
            st.session_state['last_limit_val'] = int(row_limit)
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
                    integrity_where = f"order_id IN (SELECT order_id FROM {schema_name}.orders WHERE customer_id = {q_val})"

            if is_locked and integrity_where:
                use_suggestion = st.checkbox("Use deep integrity sync", value=True, key=f"suggest_chk_{table_name}")
                if use_suggestion:
                    if st.session_state.get(where_key) != integrity_where:
                        st.session_state[where_key] = integrity_where
                    st.info(f"⛓️ **Deep RI Lock Active:** `{integrity_where}`")
                    st.text_input("Active SQL Filter:", value=integrity_where, disabled=True, key=f"locked_in_{where_key}")
                else:
                    st.session_state[where_key] = st.text_input(
                        "SQL WHERE clause (custom)",
                        value=st.session_state.get(where_key, ""),
                        key=f"in_custom_{where_key}"
                    )
            else:
                st.session_state[where_key] = st.text_input(
                    "SQL WHERE clause (independent)",
                    value=st.session_state.get(where_key, ""),
                    key=f"in_{where_key}"
                )
        else:
            st.info("Select a table in Planning to configure filters.")

def render_comparison_tab(db):
    st.subheader("🔍 Side-by-Side Comparison")

    if 'current_plan' in st.session_state and 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_plan = st.session_state['current_plan']
        current_salt = st.session_state.get('salt_input', 'default_salt')
        current_locale = st.session_state.get('selected_locale', 'de')  # New

        # Fetch sample for comparison
        raw_sample = db.read_table(
            table_name,
            schema_name,
            limit=st.session_state.get('last_limit_val', 10),
            where=st.session_state.get(f"where_clause_{table_name}", "")  # Use the correct key and argument
        )

        if not raw_sample.empty:
            # Apply anonymization to sample with locale support
            anon_sample = db.apply_anonymization_rules(
                raw_sample,
                current_plan,
                salt=current_salt
            )
            notes = []  # Keep empty list so downstream UI remains stable


            if notes:
                for n in set(notes): st.info(n)

            c1, c2 = st.columns(2)
            with c1:
                st.write("**📄 Original Data**")
                st.dataframe(raw_sample, width="stretch")
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, width="stretch")

            # Export section
            if st.button("Prepare Full Download (CSV)"):
                with st.spinner("Generating CSV..."):
                    # Full export flow
                    full_df = db.read_table(table_name, schema_name)
                    # Method returns a single object, no tuple unpacking needed
                    full_anon = db.apply_anonymization_rules(
                        full_df,
                        current_plan,
                        salt=current_salt
                    )
                    csv = full_anon.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Click to Download CSV",
                        data=csv,
                        file_name=f"anon_{table_name}.csv",
                        mime="text/csv",
                    )
        else:
            st.warning("No records to compare.")

def render_tabs(db):
    tab_list = ["🛠️ Plan", "🔍 Comparison", "📜 Audit"]
    tabs = st.tabs(tab_list)

    with tabs[0]:
        render_planner_tab(db)

    with tabs[1]:
        render_comparison_tab(db)

    with tabs[2]:
        # Audit log fetch
        log_query = "SELECT * FROM metadata.audit_log ORDER BY execution_time DESC LIMIT 50"
        try:
            log_df = pd.read_sql(log_query, db.engine)
            st.dataframe(log_df, width="stretch")
        except:
            st.info("No audit logs found yet.")


def sync_anon_ddl_with_plan(db, target_schema, table_name, plan):
    """
    Aligns anon schema data types with anonymization plan.
    Receives 'db' (DBManager instance) instead of self.
    """
    from sqlalchemy import text

    text_strategies = ['hash', 'faker_name', 'faker_email', 'faker_phone', 'mask', 'mapping']

    # Use db.engine because this function is outside class scope
    with db.engine.connect() as conn:
        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()

            if strategy in text_strategies:
                check_query = text("""
                    SELECT data_type FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t AND column_name = :c
                """)
                current_type = conn.execute(check_query, {"s": target_schema, "t": table_name, "c": col}).scalar()

                if current_type and any(num_type in current_type.lower() for num_type in ['int', 'numeric', 'double', 'real']):
                    print(f"🔧 DDL Sync: Menjam {table_name}.{col} iz {current_type} u VARCHAR(255)...")

                    alter_query = text(f"""
                        ALTER TABLE "{target_schema}"."{table_name}"
                        ALTER COLUMN "{col}" TYPE VARCHAR(255)
                        USING "{col}"::VARCHAR
                    """)
                    conn.execute(alter_query)
                    conn.commit()
                    print(f"✅ DDL Aligned: {table_name}.{col} converted to VARCHAR")

def get_all_foreign_keys(db, schema_name):
    """
    Fetches all FK relations for a schema.
    Receives 'db' (DBManager instance) instead of self.
    """
    from sqlalchemy import text

    query = text("""
        SELECT
            kcu.table_name as source_table,
            kcu.column_name as source_column,
            rel_kcu.table_name as target_table,
            rel_kcu.column_name as target_column
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu
          ON tco.constraint_name = kcu.constraint_name
        JOIN information_schema.referential_constraints rco
          ON tco.constraint_name = rco.constraint_name
        JOIN information_schema.key_column_usage rel_kcu
          ON rco.unique_constraint_name = rel_kcu.constraint_name
        WHERE tco.constraint_type = 'FOREIGN KEY'
          AND tco.table_schema = :s
    """)

    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"s": schema_name})
            return [(row[0], row[1], row[2], row[3]) for row in result]
    except Exception as e:
        print(f"❌ Error fetching foreign keys: {e}")
        return []

def render_global_preview_section(db):
    """Persistent live preview panel with SQL context."""

    if 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_full_name = f"{schema_name}.{table_name}"

        # --- 1. Auto-reset synchronization ---
        # If user changes table in sidebar, clear old DataFrame
        # to prevent showing stale data from previous table
        if st.session_state.get('last_previewed_table') != current_full_name:
            if 'current_df' in st.session_state:
                del st.session_state['current_df']
            st.session_state['last_previewed_table'] = current_full_name

        with st.container():
            p_col1, p_col2 = st.columns([3, 7])

            with p_col1:
                st.write(f"**Current Context:** `{current_full_name}`")

                # Read filter entered in Planner tab
                where_clause = st.session_state.get(f"where_clause_{table_name}", "")

                if where_clause:
                    st.info(f"🔍 **Active Filter:**\n`{where_clause}`")
                    st.caption("Debug SQL Query:")
                    st.code(f"SELECT * FROM {current_full_name} WHERE {where_clause} LIMIT 100;", language="sql")
                else:
                    st.caption("No active filter. Showing top 100 records.")

                if st.button("🔄 Refresh Data", key="global_preview_refresh_btn", width="stretch"):
                    with st.spinner(f"Fetching {table_name}..."):
                        try:
                            df = db.read_table(table_name, schema_name, where=where_clause, limit=100)
                            st.session_state['current_df'] = df
                            st.rerun()
                        except Exception as e:
                            st.error(f"SQL Error: {str(e)}")

            with p_col2:
                if 'current_df' in st.session_state:
                    df = st.session_state['current_df']
                    if df.empty:
                        st.warning("⚠️ This table is empty or no records match your WHERE clause.")
                    else:
                        st.dataframe(
                            df,
                                width="stretch",
                            hide_index=True
                        )
                        st.caption(f"Showing up to 100 rows from {current_full_name}")
                else:
                    st.info("💡 Data not loaded yet. Click **'Refresh Data'** to fetch a snippet.")
    else:
        # If nothing is selected, show a minimal prompt
        st.info("👋 Select a table in the **Explorer** or **Plan** tab to enable live preview here.")