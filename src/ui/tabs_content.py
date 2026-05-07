# -*- coding: utf-8 -*-
# --- File imports ---
import streamlit as st
import pandas as pd
import time
import logging
import json
import html
import numbers
from decimal import Decimal
from sqlalchemy import text
from src.core.domain.services.unsaved_changes_guard import UnsavedChangesGuard
# Shared planner modules
from src.ui.planner import analyze_tables_parallel
from src.ui.planner_logic import validate_plan_row, calculate_privacy_score, get_clean_plan
from src.ui.planner_components import render_ai_audit_log
from src.ui.planner_navigation import handle_navigation_history, get_next_table_in_chain

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


@st.dialog("Finalize Project")
def _render_finalize_confirmation_dialog():
    st.warning(
        "Are you sure? This will close the current session. "
        "Make sure you have executed the pipeline if you wanted to move data."
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel", width="stretch", key="finalize_cancel_btn"):
            st.rerun()
    with c2:
        if st.button("Yes, finalize", type="primary", width="stretch", key="finalize_confirm_btn"):
            _finalize_close_project()


def run_all_anonymization(
    db,
    schema_name,
    execution_order,
    progress_slot=None,
    status_slot=None,
    write_mode="overwrite",
    overwrite_clear_mode="truncate_cascade",
):
    if not execution_order:
        st.error("No execution order found. Run AI scan first.")
        return False
    if hasattr(db, "reset_structural_sync_counters"):
        db.reset_structural_sync_counters()

    progress = progress_slot if progress_slot is not None else st.progress(0)
    status = status_slot if status_slot is not None else st.empty()
    progress.progress(0.0)
    total = len(execution_order)

    if str(write_mode).lower() == "overwrite":
        status.info("Applying overwrite mode: truncating target tables before insert...")
        try:
            db.truncate_anon_tables("anon", execution_order, clear_mode=overwrite_clear_mode)
        except Exception as e:
            st.error(f"Failed truncating target tables: {e}")
            status.error("Stopped before execution due to truncate failure.")
            return False

    for idx, table_name in enumerate(execution_order, start=1):
        pct = int((idx / total) * 100)
        status.info(f"Processing {table_name}... {pct}%")

        saved_data = db.get_saved_plan(schema_name, table_name)
        if not saved_data or not saved_data.get('plan'):
            st.error(f"Missing saved strategy for `{table_name}`. Save all plans first.")
            status.error(f"Stopped at `{table_name}`. Downstream tables were skipped for RI safety.")
            return False

        where_clause = str(saved_data.get('where', '') or '').strip()
        clean_plan = get_clean_plan(saved_data.get('plan', []))
        table_plan_salt = _resolve_plan_salt(db, schema_name, table_name)

        table_ready, table_create_msg = db.create_anonymized_table(
            source_schema=schema_name,
            table_name=table_name,
            target_db=db.target_db_url.split("/")[-1]
        )
        if not table_ready:
            st.error(f"Failed preparing target table `{table_name}`: {table_create_msg}")
            status.error(f"Stopped at `{table_name}`. Downstream tables were skipped for RI safety.")
            return False

        full_data = db.read_table(table_name, schema_name, where=where_clause)
        final_df = db.apply_anonymization_rules(full_data, clean_plan, salt=table_plan_salt)
        save_ok = db.save_anonymized_table(
            final_df,
            table_name,
            target_schema='anon',
            source_schema=schema_name,
            preserve_native_columns=[
                i.get("column") for i in clean_plan
                if isinstance(i, dict) and str(i.get("strategy", "keep")).lower() == "keep"
            ],
        )
        if not save_ok:
            st.error(f"Saving anonymized data failed for `{table_name}`.")
            status.error(f"Stopped at `{table_name}`. Downstream tables were skipped for RI safety.")
            return False
        progress.progress(idx / total)

    try:
        db.sync_foreign_keys_for_tables(
            source_schema=schema_name,
            target_schema="anon",
            ordered_tables=execution_order,
        )
    except Exception as e:
        st.error(f"Failed restoring foreign keys/indexes: {e}")
        status.error("Execution finished with structural sync errors.")
        return False

    status.success("✅ Run & Save All completed for execution order.")
    return True



def _resolve_plan_salt(db, schema_name, table_name):
    """Returns per-plan salt, creating one if missing."""
    saved = db.get_saved_plan(schema_name, table_name)
    if saved and saved.get("salt"):
        return saved.get("salt")
    salt_val, _, _ = db.ensure_plan_security_metadata(schema_name, table_name)
    return salt_val

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
                is_sensitive = _is_sensitive_row(row)

                if is_sensitive and strategy == "keep":
                    invalid_selections.append(
                        f"❌ Sensitive column `{col_name}` cannot use strategy `keep`."
                    )
                    continue

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
    plan_salt = _resolve_plan_salt(db, schema_name, table_name)

    # --- 6b. Pre-save type validation using transformed sample ---
    try:
        raw_sample = db.read_table(table_name, schema_name, where=safe_where, limit=10)
        if not raw_sample.empty:
            anon_sample = db.apply_anonymization_rules(raw_sample, clean_plan, salt=plan_salt)
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
                        if not series.map(
                            lambda v: isinstance(v, (Decimal, numbers.Number)) and not isinstance(v, bool)
                        ).all():
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
    saved_now = db.get_saved_plan(schema_name, table_name)
    if saved_now and saved_now.get("source_list_mismatch"):
        st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")

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
            if next_table_plan.get("source_list_mismatch"):
                st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
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
    review_progress_slot = st.empty()
    review_status_slot = st.empty()
    # Always initialize early so downstream button disabled state is safe.
    violation_found = False
    selected_schema = st.session_state.get('selected_schema', 'public')
    if 'active_plan_by_table' not in st.session_state:
        st.session_state['active_plan_by_table'] = {}
    if 'current_plan_data' not in st.session_state:
        st.session_state['current_plan_data'] = {}
    if 'active_table_index' not in st.session_state:
        st.session_state['active_table_index'] = 0
    if 'current_table_index' not in st.session_state:
        st.session_state['current_table_index'] = st.session_state['active_table_index']

    def _set_active_table_by_index(index, ordered_tables, schema_name):
        """Single source of truth navigation setter based on execution order index."""
        if not ordered_tables:
            return

        safe_index = max(0, min(index, len(ordered_tables) - 1))
        next_table = ordered_tables[safe_index]
        st.session_state['active_table_index'] = safe_index
        st.session_state['current_table_index'] = safe_index
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
                if saved_data.get("source_list_mismatch"):
                    st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
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

    def _go_to_table_by_index(target_index, ordered_tables, schema_name):
        current_table_info = st.session_state.get('selected_table_info')
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
        current_idx_local = st.session_state.get('current_table_index', st.session_state.get('active_table_index', 0))
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
                    st.session_state['current_table_index'] = idx
                    _go_to_table_by_index(idx, ordered_tables, schema_name)

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

    all_tables_list = st.session_state.get('all_tables_list', [])
    completed_tables = st.session_state.get('completed_tables', set())

    with st.expander("1) ⚙️ Anonymization Strategy & Design", expanded=True):
        st.caption(f"Schema: `{selected_schema}`")
        force_overwrite_manual = st.checkbox(
            "Force Overwrite Manual Changes",
            value=False,
            key="force_overwrite_manual_changes"
        )
        st.caption("AI will update only checked tables. Manual-edited rows remain protected unless overwrite is enabled.")

        ai_scope_tables = st.session_state.get('all_tables_list', []) or st.session_state.get('selected_tables', [])
        st.session_state["ai_scope_tables_current"] = list(ai_scope_tables)

        def _on_select_all_ai_change():
            desired = bool(st.session_state.get("select_all_ai", False))
            for _table_name in st.session_state.get("ai_scope_tables_current", []):
                st.session_state[f"ai_include_{_table_name}"] = desired

        def _on_individual_ai_change():
            table_names = st.session_state.get("ai_scope_tables_current", [])
            all_checked = all(bool(st.session_state.get(f"ai_include_{t_name}", False)) for t_name in table_names)
            st.session_state["select_all_ai"] = all_checked

        if ai_scope_tables:
            with st.container():
                st.markdown("**Include in AI Scan**")
                include_keys = [f"ai_include_{t_name}" for t_name in ai_scope_tables]
                all_checked_now = all(bool(st.session_state.get(k, True)) for k in include_keys)
                if "select_all_ai" not in st.session_state:
                    st.session_state["select_all_ai"] = all_checked_now
                st.checkbox(
                    "✅ Select/Deselect All",
                    key="select_all_ai",
                    on_change=_on_select_all_ai_change
                )
                include_cols = st.columns(4)
                for i, t_name in enumerate(ai_scope_tables):
                    include_key = f"ai_include_{t_name}"
                    if include_key not in st.session_state:
                        st.session_state[include_key] = True
                    with include_cols[i % len(include_cols)]:
                        st.checkbox(t_name, key=include_key, on_change=_on_individual_ai_change)

        if st.button(
            "🤖 Suggest with AI (Unified Scan)",
            type="primary",
            disabled=not st.session_state.get('planning_initialized', False),
            key="explicit_unified_ai_scan_btn"
        ):
            selected_tables_set = set(st.session_state.get('selected_tables', []) or [])
            candidate_tables = st.session_state.get('all_tables_list', []) or st.session_state.get('selected_tables', [])
            tables_to_scan = [
                t for t in candidate_tables
                if t in selected_tables_set and st.session_state.get(f"ai_include_{t}", True)
            ]
            if not tables_to_scan:
                st.warning("No tables selected for AI scan. Enable 'Include in AI Scan' for at least one table.")
            else:
                with st.status("Running unified AI scan...", expanded=True) as scan_status:
                    scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                    scan_status.write("Submitting unified request...")
                    all_results = analyze_tables_parallel(
                        db, tables_to_scan, schema=selected_schema,
                        allow_sampling=bool(st.session_state.get("bulk_allow_sample", True)),
                        sample_limit=int(st.session_state.get("bulk_sample_rows", 5))
                    )
                    scan_status.write("Collecting results...")
                    existing_results = st.session_state.get('multi_ai_analysis', {})
                    merged_results = dict(existing_results) if isinstance(existing_results, dict) else {}
                    if "last_ai_plan_by_table" not in st.session_state:
                        st.session_state["last_ai_plan_by_table"] = {}

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
                        st.session_state["last_ai_plan_by_table"][t_name] = ai_entry.get("plan", [])
                        st.session_state['active_plan_by_table'][_table_plan_cache_key(selected_schema, t_name)] = merged_plan
                        if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
                            st.session_state['active_plan'] = {}
                        st.session_state['active_plan'][t_name] = {"mappings": merged_plan}

                    st.session_state['multi_ai_analysis'] = merged_results
                    st.session_state['plan_active'] = True
                    scan_status.update(label="Unified AI scan completed", state="complete")
                    st.rerun()

        selected_multi_tables = st.multiselect(
            "Tables for parallel scan",
            options=available_tables,
            default=initial_multiselect_default,
            key="planner_multiselect"
        )
        st.session_state['selected_tables'] = selected_multi_tables

        c1, c2 = st.columns([1, 2])
        with c1:
            bulk_allow_sampling = st.checkbox("Enable sampling", value=True, key="bulk_allow_sample")
        with c2:
            bulk_sample_rows = st.number_input(
                "Sample rows",
                min_value=1,
                max_value=5000,
                value=int(st.session_state.get("bulk_sample_rows", 5)),
                step=1,
                help="Tip: focus this field, then use keyboard Up/Down arrows for precise adjustments.",
                key="bulk_sample_rows"
            ) if bulk_allow_sampling else 0

        if st.button("🪄 Parallel AI Scan", disabled=not selected_multi_tables, type="secondary"):
            with st.status("Running parallel scan...", expanded=True) as scan_status:
                tables_to_scan = st.session_state.get('planner_multiselect', selected_multi_tables)
                scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                scan_status.write("Submitting scan tasks...")
                all_results = analyze_tables_parallel(
                    db, tables_to_scan, schema=selected_schema,
                    allow_sampling=bulk_allow_sampling, sample_limit=bulk_sample_rows
                )
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

        if 'selected_table_info' not in st.session_state and st.session_state.get('multi_ai_analysis'):
            multi_results = st.session_state['multi_ai_analysis']
            execution_order = st.session_state.get('all_tables_list', [])
            if not execution_order:
                fallback_selected = st.session_state.get('selected_tables', [])
                if fallback_selected:
                    execution_order = db.get_execution_order(fallback_selected, selected_schema)
                    st.session_state['all_tables_list'] = execution_order
            fallback_table = _pick_first_table_by_execution_order(multi_results.keys(), execution_order)
            if not fallback_table:
                first_result_key = next(iter(multi_results.keys()), None)
                fallback_table = str(first_result_key).split('.')[-1] if first_result_key else None
            if fallback_table:
                if fallback_table in execution_order:
                    _set_active_table_by_index(execution_order.index(fallback_table), execution_order, selected_schema)
                else:
                    _set_active_table_by_index(0, execution_order, selected_schema)

        all_tables_list = st.session_state.get('all_tables_list', [])
        completed_tables = st.session_state.get('completed_tables', set())
        _render_execution_roadmap(all_tables_list, completed_tables, selected_schema)

        selectable_tables = all_tables_list or st.session_state.get('selected_tables', [])
        if selectable_tables:
            max_idx = len(selectable_tables) - 1
            current_idx = st.session_state.get('current_table_index', st.session_state.get('active_table_index', 0))
            clamped_idx = max(0, min(current_idx, max_idx))
            if clamped_idx != current_idx:
                st.session_state['active_table_index'] = clamped_idx
                st.session_state['current_table_index'] = clamped_idx
            selected_active_table = st.selectbox("Planning table", selectable_tables, index=clamped_idx, label_visibility="collapsed")
            selected_idx = selectable_tables.index(selected_active_table)
            if selected_idx != st.session_state.get('current_table_index', st.session_state.get('active_table_index', 0)):
                _go_to_table_by_index(selected_idx, selectable_tables, selected_schema)

        st.info("ℹ️ ID/PK/FK columns are protected. If AI proposes `mask`, it is automatically forced to `hash`.")
        if 'selected_table_info' in st.session_state:
            table_info = st.session_state['selected_table_info']
            table_name = table_info[0] if isinstance(table_info, tuple) else table_info
            schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
            current_saved_plan_meta = db.get_saved_plan(schema_name, table_name)
            if current_saved_plan_meta and current_saved_plan_meta.get("source_list_mismatch"):
                st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
            editor_key = f"plan_editor_{table_name}"
            table_cache_key = _table_plan_cache_key(schema_name, table_name)
            cached_active_plan = _get_persisted_plan_for_table(schema_name, table_name)

            multi_results = st.session_state.get('multi_ai_analysis', {})
            if 'ai_analysis' not in st.session_state or st.session_state.get('last_rendered_table') != table_name:
                if cached_active_plan:
                    st.session_state['ai_analysis'] = cached_active_plan
                    st.session_state['last_rendered_table'] = table_name
                    st.session_state['plan_active'] = True
                else:
                    found_res = _find_multi_result(multi_results, table_name, schema_name)
                    found_plan = found_res.get('plan') if isinstance(found_res, dict) else getattr(found_res, 'plan', None) if found_res else None
                    found_audit = found_res.get('audit', []) if isinstance(found_res, dict) else getattr(found_res, 'audit', []) if found_res else []
                    found_error = found_res.get('error') if isinstance(found_res, dict) else None
                    if found_plan:
                        st.session_state['ai_analysis'] = found_plan
                        st.session_state['last_ai_audit'] = found_audit or []
                        st.session_state['last_rendered_table'] = table_name
                        st.session_state['plan_active'] = True
                    else:
                        if found_error:
                            violation_found = True
                            st.error(f"❌ Parallel scan failed for `{table_name}`: {found_error}")
                        saved_data = db.get_saved_plan(schema_name, table_name)
                        if saved_data:
                            if saved_data.get("source_list_mismatch"):
                                st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
                            st.session_state['ai_analysis'] = saved_data['plan']
                            st.session_state['plan_snapshot'] = saved_data['plan']
                            st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                            st.session_state['plan_origin'] = 'saved'
                            st.session_state['last_rendered_table'] = table_name
                            st.session_state['plan_active'] = True
                            st.session_state['active_plan_by_table'][table_cache_key] = saved_data['plan']
                            if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
                                st.session_state['active_plan'] = {}
                            st.session_state['active_plan'][table_name] = {"mappings": saved_data['plan']}
                        else:
                            if 'ai_analysis' in st.session_state:
                                del st.session_state['ai_analysis']
                            # Manual mode default when AI wasn't explicitly executed.
                            manual_plan = _default_manual_plan(db, schema_name, table_name)
                            st.session_state['ai_analysis'] = manual_plan
                            st.session_state['active_plan_by_table'][table_cache_key] = manual_plan
                            if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
                                st.session_state['active_plan'] = {}
                            st.session_state['active_plan'][table_name] = {"mappings": manual_plan}
                            st.session_state['last_rendered_table'] = table_name
                            st.session_state['plan_active'] = False

            handle_navigation_history(table_name, schema_name)
            current_table_col_details = db.get_column_details(table_name, schema_name)
            current_table_fks = [fk[1] for fk in all_fks if fk[0] == table_name]
            if f"pk_{table_name}" not in st.session_state:
                st.session_state[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)
            real_pks = st.session_state[f"pk_{table_name}"]

            left_col, right_col = st.columns([1, 2.2], gap="large")
            with left_col:
                render_planner_action_buttons(db, table_name, schema_name)
                if st.button("🗑️ Clear AI Suggestions", width="stretch", key=f"clear_ai_{schema_name}_{table_name}"):
                    cleared_plan = _default_manual_plan(db, schema_name, table_name)
                    st.session_state['ai_analysis'] = cleared_plan
                    st.session_state['current_plan'] = cleared_plan
                    st.session_state['active_plan_by_table'][table_cache_key] = cleared_plan
                    if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
                        st.session_state['active_plan'] = {}
                    st.session_state['active_plan'][table_name] = {"mappings": cleared_plan}
                    if "manual_overrides_by_table" in st.session_state:
                        st.session_state["manual_overrides_by_table"][table_name] = set()
                    st.rerun()
            with right_col:
                if 'ai_analysis' in st.session_state and st.session_state['ai_analysis']:
                    plan_df = pd.DataFrame(st.session_state['ai_analysis'])
                    if plan_df.empty:
                        violation_found = True
                        st.warning("⚠️ Anonymization plan is empty. Run scan again.")
                        return
                    available_cols = plan_df.columns.tolist()
                    col_key = next((c for c in available_cols if c.lower() == 'column'), None)
                    if col_key is None:
                        violation_found = True
                        st.error(f"❌ Data structure error. Found columns: {available_cols}")
                        return

                    def get_col_status(col):
                        if col in real_pks: return "🔑 PK (Locked)"
                        if col in current_table_fks: return "🔗 FK (Dependent)"
                        return "✅ Normal"

                    plan_df['status'] = plan_df[col_key].apply(get_col_status)
                    plan_df["is_sensitive"] = plan_df.apply(
                        lambda r: bool(r.get("is_sensitive", r.get("is_pii", False))),
                        axis=1
                    )
                    plan_df['guard'] = plan_df[col_key].apply(
                        lambda c: "ℹ️ Mask disabled for ID safety" if any(token in str(c).lower() for token in ["id", "pk", "fk"]) else ""
                    )
                    locked_mask = plan_df['status'].str.contains("Locked|Dependent", na=False)
                    id_name_mask = plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
                    if 'strategy' in plan_df.columns:
                        plan_df.loc[(locked_mask | id_name_mask) & (plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'
                    editor_left, editor_center, editor_right = st.columns([0.2, 4, 0.2], gap="small")
                    with editor_center:
                        edited_plan_df = st.data_editor(
                            plan_df,
                            column_config={
                                "status": st.column_config.TextColumn("Status", disabled=True),
                                "is_sensitive": st.column_config.CheckboxColumn("Sensitive", disabled=True),
                                "guard": st.column_config.TextColumn("Constraint", disabled=True),
                                col_key: st.column_config.TextColumn("Column", disabled=True),
                                "strategy": st.column_config.SelectboxColumn("Strategy", options=["keep", "hash", "mask", "null", "faker_name"], required=True),
                            },
                            hide_index=True, key=editor_key, width="stretch"
                        )
                    if 'strategy' in edited_plan_df.columns:
                        edited_id_mask = edited_plan_df[col_key].astype(str).str.contains(r"id|pk|fk", case=False, regex=True)
                        edited_plan_df.loc[edited_id_mask & (edited_plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'
                        if "is_sensitive" in edited_plan_df.columns:
                            sensitive_keep_mask = edited_plan_df["is_sensitive"] & (edited_plan_df["strategy"] == "keep")
                            if sensitive_keep_mask.any():
                                violation_found = True
                                blocked_cols = edited_plan_df.loc[sensitive_keep_mask, col_key].astype(str).tolist()
                                st.error(
                                    "Sensitive columns cannot use `keep`. Update strategies for: "
                                    f"{', '.join(blocked_cols)}."
                                )
                    st.session_state['current_plan'] = edited_plan_df.to_dict('records')
                    _track_manual_overrides_for_table(table_name, st.session_state['current_plan'])
                    st.session_state['active_plan_by_table'][table_cache_key] = st.session_state['current_plan']
                    if 'active_plan' not in st.session_state or not isinstance(st.session_state['active_plan'], dict):
                        st.session_state['active_plan'] = {}
                    st.session_state['active_plan'][table_name] = {"mappings": st.session_state['current_plan']}
                else:
                    st.info("💡 Select a table and run analysis to view the anonymization plan.")

            where_key_table = f"where_clause_{table_name}"
            current_plan_rows = st.session_state.get('current_plan') or st.session_state.get('ai_analysis') or []
            current_where_val = st.session_state.get(where_key_table, "")
            saved_plan_rows = current_saved_plan_meta.get("plan", []) if current_saved_plan_meta else []
            saved_where_val = current_saved_plan_meta.get("where", "") if current_saved_plan_meta else ""
            is_dirty = (
                _normalized_plan_rows(current_plan_rows) != _normalized_plan_rows(saved_plan_rows)
                or str(current_where_val or "").strip() != str(saved_where_val or "").strip()
            )
            _set_current_plan_data(
                schema_name=schema_name,
                table_name=table_name,
                plan_rows=current_plan_rows,
                where_clause=current_where_val,
                dirty=is_dirty,
            )
            if is_dirty:
                st.warning("⚠️ You have unsaved changes for this table.")
        else:
            st.info("👋 Select a table to start.")

        if st.session_state.get('multi_ai_analysis'):
            scan_results = st.session_state.get('multi_ai_analysis', {})
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

    with st.expander("3) 🔍 Filter, Consistency & Preview", expanded=False):
        if 'selected_table_info' in st.session_state:
            table_info = st.session_state['selected_table_info']
            table_name = table_info[0] if isinstance(table_info, tuple) else table_info
            schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
            where_key = f"where_clause_{table_name}"
            current_table_col_details = db.get_column_details(table_name, schema_name)
            table_columns = list(current_table_col_details.keys())

            is_locked = st.session_state.get('global_lock_check', False)
            g_val = st.session_state.get('global_integrity_val', '1')
            preview_limit_col, preview_refresh_col = st.columns([3, 1], gap="small", vertical_alignment="bottom")
            with preview_limit_col:
                row_limit = st.number_input(
                    "Row preview limit",
                    min_value=1,
                    max_value=1000,
                    value=int(st.session_state.get(f"row_limit_{table_name}", 100)),
                    step=1,
                    help="Tip: focus this field, then use keyboard Up/Down arrows for precise adjustments.",
                    key=f"row_limit_{table_name}"
                )
            with preview_refresh_col:
                refresh_preview = st.button("👁️ Refresh Preview", width="stretch", key=f"refresh_preview_{table_name}")
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
                deep_sync_col, deep_sync_help_col = st.columns([12, 1], gap="small")
                with deep_sync_col:
                    use_suggestion = st.checkbox("Use deep integrity sync", value=True, key=f"suggest_chk_{table_name}")
                with deep_sync_help_col:
                    st.markdown(
                        '<div style="padding-top: 0.35rem; font-size: 1.05rem;" title="Performs recursive updates across related tables to maintain referential integrity.">⭐</div>',
                        unsafe_allow_html=True
                    )
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

            st.divider()
            st.markdown("#### Live Data Preview")

            live_preview_df = _get_live_preview_once(
                db=db,
                schema_name=schema_name,
                table_name=table_name,
                where_clause=st.session_state.get(where_key, ""),
                row_limit=int(row_limit),
                force_refresh=bool(refresh_preview),
            )
            st.dataframe(live_preview_df, height=200, width="stretch")

            ordered_tables = all_tables_list or selectable_tables
            current_idx = st.session_state.get('current_table_index', st.session_state.get('active_table_index', 0))
            current_idx = max(0, min(current_idx, len(ordered_tables) - 1)) if ordered_tables else -1
        else:
            st.info("Select a table in Planning to configure filters.")

    st.markdown("---")
    st.markdown("### Global Actions")
    plans_initialized = bool(
        st.session_state.get('plan_active')
        or st.session_state.get('active_plan_by_table')
        or st.session_state.get('active_plan')
    )
    if 'selected_table_info' in st.session_state and plans_initialized:
        table_info = st.session_state['selected_table_info']
        table_name = table_info[0] if isinstance(table_info, tuple) else table_info
        schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema
        ordered_tables = st.session_state.get('all_tables_list', []) or st.session_state.get('selected_tables', [])
        current_idx = st.session_state.get('current_table_index', st.session_state.get('active_table_index', 0))
        current_idx = max(0, min(current_idx, len(ordered_tables) - 1)) if ordered_tables else -1
        next_t = get_next_table_in_chain(table_name, ordered_tables, st.session_state.get('completed_tables', set()))
        confirm_label = "💾 Save and Next" if next_t else "🏁 Finalize & Close Project"
        where_key = f"where_clause_{table_name}"

        action_cols = st.columns([1, 1, 1], gap="small")
        with action_cols[0]:
            if st.button("⬅️ Previous", width="stretch", disabled=(current_idx <= 0), key=f"global_prev_{schema_name}_{table_name}"):
                _go_to_table_by_index(current_idx - 1, ordered_tables, schema_name)
        with action_cols[1]:
            if st.button("Next ➡️", width="stretch", disabled=not (ordered_tables and current_idx < len(ordered_tables) - 1), key=f"global_next_{schema_name}_{table_name}"):
                _go_to_table_by_index(current_idx + 1, ordered_tables, schema_name)
        with action_cols[2]:
            current_table_state = st.session_state.get("current_plan_data", {}).get(f"{schema_name}.{table_name}", {})
            current_table_dirty = bool(current_table_state.get("dirty"))
            save_label = "💾 Save and Next" if next_t else "💾 Save Table Configuration"
            if st.button(save_label, type="primary", width="stretch", disabled=violation_found, key=f"global_conf_{table_name}"):
                current_plan = st.session_state.get('current_plan')
                if not current_plan:
                    current_plan = _get_persisted_plan_for_table(schema_name, table_name)
                if current_plan is None:
                    current_plan = st.session_state.get('ai_analysis')

                if current_plan is None:
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

                if ordered_tables and current_idx < len(ordered_tables) - 1:
                    _set_active_table_by_index(current_idx + 1, ordered_tables, schema_name)
                    st.success(f"Plan saved. Moving to {ordered_tables[st.session_state['active_table_index']]}...")
                else:
                    st.success("✅ Table configuration saved.")
                st.session_state["current_plan_data"][f"{schema_name}.{table_name}"]["dirty"] = False
                st.rerun()
            st.caption("Finalize: Saves the final state, clears the active session, and prepares the app for a new project.")
    else:
        st.caption("Global actions become available after a plan is initialized.")

    st.markdown("### Review")
    unsaved_tables = _get_unsaved_tables()
    ordered_tables_for_execution = st.session_state.get('all_tables_list', []) or st.session_state.get('selected_tables', [])
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
        disabled=(len(unsaved_tables) == 0)
    ):
        failed_tables = []
        for table_key in unsaved_tables:
            table_state = st.session_state.get("current_plan_data", {}).get(table_key, {})
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
                where_condition=str(where_clause or "").strip()
            )
            if save_ok:
                st.session_state["current_plan_data"][table_key]["dirty"] = False
            else:
                failed_tables.append(table_name)
        if failed_tables:
            st.error(f"Failed saving: {', '.join(failed_tables)}")
        else:
            st.toast("✅ All table configurations are now synchronized with the plan.")
        st.rerun()

    write_mode_ui = st.selectbox(
        "Write Mode",
        options=["Overwrite (Truncate)", "Append"],
        index=0,
        key="execution_write_mode",
        help="Overwrite truncates target tables before insert. Append keeps existing rows and adds new ones.",
    )
    overwrite_clear_mode = "truncate_cascade"
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
        else ("session_replication_role = replica" if overwrite_clear_mode == "session_replica" else "TRUNCATE ... CASCADE")
    )
    db_name = st.session_state.get("active_plan_db_name", "None")
    data_domain = st.session_state.get("data_domain", "General")
    st.info(
        f"Plan: {db_name} | Domain: {data_domain} | Write Mode: {summary_mode} | Clear Strategy: {summary_strategy}"
    )

    if unsaved_tables:
        st.warning("⚠️ Cannot finalize. You have unsaved changes in your table configurations.")

    if st.button(
        "🏁 Finalize & Close Project",
        type="primary",
        width="stretch",
        key="finalize_close_project_btn",
        disabled=bool(unsaved_tables)
    ):
        _render_finalize_confirmation_dialog()
    st.caption(
        "🏁 Finalize: Saves the final plan metadata, closes the active database connections, and resets the "
        "application state for a new session. Ensure all tables are executed before finalizing."
    )
    execute_disabled = bool(unsaved_tables) or bool(sensitive_keep_violations) or (len(ordered_tables_for_execution) == 0)
    st.markdown("---")
    if st.button(
        "🚀 Execute Anonymization Pipeline",
        type="primary",
        width="stretch",
        key="global_run_all_tables_bottom",
        disabled=execute_disabled,
    ):
        mode = "overwrite" if write_mode_ui.startswith("Overwrite") else "append"
        with st.spinner("Running execution-order batch anonymization..."):
            run_all_anonymization(
                db=db,
                schema_name=selected_schema,
                execution_order=ordered_tables_for_execution,
                progress_slot=review_progress_slot.progress(0.0),
                status_slot=review_status_slot,
                write_mode=mode,
                overwrite_clear_mode=overwrite_clear_mode,
            )

def render_comparison_tab(db):
    st.subheader("🔍 Side-by-Side Comparison")

    if 'current_plan' in st.session_state and 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_plan = st.session_state['current_plan']
        current_salt = _resolve_plan_salt(db, schema_name, table_name)
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
        # Audit log fetch (safe on first-run metadata bootstrapping)
        log_df = db.get_audit_logs(limit=50)
        if log_df.empty:
            st.info("No audit logs found yet.")
        else:
            st.dataframe(log_df, width="stretch")


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
                    logger.info(f"✅ [DB_MANAGER] DDL sync: converting {table_name}.{col} from {current_type} to VARCHAR(255)")

                    alter_query = text(f"""
                        ALTER TABLE "{target_schema}"."{table_name}"
                        ALTER COLUMN "{col}" TYPE VARCHAR(255)
                        USING "{col}"::VARCHAR
                    """)
                    conn.execute(alter_query)
                    conn.commit()
                    logger.info(f"✅ [DB_MANAGER] DDL aligned: {table_name}.{col} converted to VARCHAR")

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
        logger.error(f"❌ [DB_MANAGER] Error fetching foreign keys: {e}")
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