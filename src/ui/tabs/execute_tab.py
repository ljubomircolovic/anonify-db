# -*- coding: utf-8 -*-
"""Batch anonymization execution dialogs and orchestration."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import streamlit as st

from src.ui.tabs.planner.planner_logic import get_clean_plan
from src.ui.tabs.planner.planner_secrets import build_consistency_seed_maps

logger = logging.getLogger(__name__)


@st.dialog("Confirm Execution")
def render_execute_confirmation_dialog(
    db: Any,
    schema_name: str,
    execution_order: list[str],
    progress_slot: Any,
    status_slot: Any,
    write_mode: str,
    overwrite_clear_mode: str,
    truncate_before_migration: bool,
    execute_disabled: bool,
) -> None:
    """Confirm destructive batch run; on success calls :func:`run_all_anonymization`."""
    target_db_name = st.session_state.get("pending_execute_target_db_name", "unknown")
    target_host = st.session_state.get("pending_execute_target_host", "unknown-host")
    st.warning(f"You are about to transform data in {target_db_name}. Proceed?")
    st.caption(f"Targeting: {target_db_name} on {target_host}")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel", width="stretch", key="execute_confirm_cancel_btn"):
            st.rerun()
    with c2:
        if st.button("Proceed", type="primary", width="stretch", key="execute_confirm_ok_btn"):
            if execute_disabled:
                st.error("Execution is currently blocked by validation checks.")
                st.stop()
            with st.spinner("Running execution-order batch anonymization..."):
                ok = run_all_anonymization(
                    db=db,
                    schema_name=schema_name,
                    execution_order=execution_order,
                    progress_slot=progress_slot.progress(0.0),
                    status_slot=status_slot,
                    write_mode=write_mode,
                    overwrite_clear_mode=overwrite_clear_mode,
                    truncate_before_migration=truncate_before_migration,
                )
            st.session_state["execution_completed"] = bool(ok)
            if ok:
                st.session_state["execution_last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state["execution_last_run_db_name"] = target_db_name
                st.session_state["execution_last_run_host"] = target_host
            st.rerun()


@st.dialog("Finalize Project")
def render_finalize_confirmation_dialog(finalize_callback) -> None:
    """Ask for confirmation before clearing session via ``finalize_callback``."""
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
            finalize_callback()


def run_all_anonymization(
    db: Any,
    schema_name: str,
    execution_order: list[str],
    progress_slot: Any = None,
    status_slot: Any = None,
    write_mode: str = "overwrite",
    overwrite_clear_mode: str = "truncate_cascade",
    truncate_before_migration: bool = True,
) -> bool:
    """Execute anonymization for ``execution_order`` tables into the ``anon`` schema."""
    if not execution_order:
        st.error("No execution order found. Run AI scan first.")
        return False
    if hasattr(db, "reset_structural_sync_counters"):
        db.reset_structural_sync_counters()

    progress = progress_slot if progress_slot is not None else st.progress(0)
    status = status_slot if status_slot is not None else st.empty()
    progress.progress(0.0)
    total = len(execution_order)
    plan_id = (
        str((st.session_state.get("plan_metadata", {}) or {}).get("plan_db_name", "")).strip()
        or str(st.session_state.get("active_plan_db_name", "")).strip()
    )
    global_hash_seed = plan_id or "default_plan_salt"
    logger.info("[Consistency Check] Using Plan ID %s as global hashing seed.", global_hash_seed)
    st.session_state["consistency_check_seed"] = global_hash_seed
    status.markdown(f"🧩 **Consistency Check:** Plan ID `{global_hash_seed}` is active as global hashing seed.")
    db.log_index_distribution_preflight(schema_name, execution_order)
    consistency_seed_maps = build_consistency_seed_maps(
        db,
        schema_name,
        execution_order,
        global_hash_seed,
    )
    constraints_mode = "session_replica" if bool(truncate_before_migration) else None

    if bool(truncate_before_migration) and str(write_mode).lower() == "overwrite":
        status.info("Applying overwrite mode: truncating target tables before insert...")
        try:
            db.truncate_anon_tables("anon", execution_order, clear_mode="session_replica")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed truncating target tables: {exc}")
            status.error("Stopped before execution due to truncate failure.")
            return False
    elif str(write_mode).lower() == "overwrite":
        status.info("Overwrite mode selected with truncate disabled by user.")

    for idx, table_name in enumerate(execution_order, start=1):
        pct = int((idx / total) * 100)
        status.info(f"Processing {table_name}... {pct}%")

        saved_data = db.get_saved_plan(schema_name, table_name)
        if not saved_data or not saved_data.get("plan"):
            st.error(f"Missing saved strategy for `{table_name}`. Save all plans first.")
            status.error(f"Stopped at `{table_name}`. Downstream tables were skipped for RI safety.")
            return False

        where_clause = str(saved_data.get("where", "") or "").strip()
        clean_plan = get_clean_plan(saved_data.get("plan", []))
        table_plan_salt = global_hash_seed

        table_ready, table_create_msg = db.create_anonymized_table(
            source_schema=schema_name,
            table_name=table_name,
            target_db=db.target_db_url.split("/")[-1],
        )
        if not table_ready:
            st.error(f"Failed preparing target table `{table_name}`: {table_create_msg}")
            status.error(f"Stopped at `{table_name}`. Downstream tables were skipped for RI safety.")
            return False

        full_data = db.read_table(table_name, schema_name, where=where_clause)
        final_df = db.apply_anonymization_rules(
            full_data,
            clean_plan,
            salt=table_plan_salt,
            consistency_seed_map=consistency_seed_maps.get(table_name, {}),
        )
        save_ok = db.save_anonymized_table(
            final_df,
            table_name,
            target_schema="anon",
            source_schema=schema_name,
            preserve_native_columns=[
                i.get("column")
                for i in clean_plan
                if isinstance(i, dict) and str(i.get("strategy", "keep")).lower() == "keep"
            ],
            disable_constraints_mode=constraints_mode,
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
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed restoring foreign keys/indexes: {exc}")
        status.error("Execution finished with structural sync errors.")
        return False

    status.success("✅ Run & Save All completed for execution order.")
    return True
