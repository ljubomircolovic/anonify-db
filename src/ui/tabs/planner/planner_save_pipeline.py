# -*- coding: utf-8 -*-
"""Strict plan validation, persistence, and table-to-table navigation after save."""

from __future__ import annotations

import numbers
import time
from decimal import Decimal
from typing import Any

import streamlit as st

from src.ui.tabs.planner.planner_logic import get_clean_plan
from src.ui.tabs.planner.planner_navigation import get_next_table_in_chain
from src.ui.tabs.planner.planner_validation import _is_sensitive_row
from src.ui.tabs.planner.planner_secrets import resolve_plan_salt as _resolve_plan_salt

def save_and_move_to_next(
    db: Any,
    table_name: str,
    schema_name: str,
    plan_data: list[dict[str, Any]],
    where_clause: str = "",
    advance: bool = True,
) -> bool:
    """
    Performs strict DDL validation, PII checks, and RI synchronization,
    saves the plan, and moves navigation to the next table.
    """
    st.toast(f"⏳ Saving plan for {table_name}...", icon="💾")

    COMPATIBILITY = {
        "numeric": ["keep", "hash", "mapping", "noise", "null"],
        "text": ["keep", "hash", "mask", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "pii": ["keep", "hash", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "date": ["keep", "date_shift", "null"],
        "boolean": ["keep", "null"],
    }

    TYPE_GROUPS = {
        "int": "numeric",
        "bigint": "numeric",
        "numeric": "numeric",
        "double": "numeric",
        "date": "date",
        "timestamp": "date",
        "time": "date",
        "bool": "boolean",
    }

    try:
        col_details = db.get_column_details(table_name, schema_name)
        indexed_columns = db.get_indexed_columns(schema_name, table_name)
        table_pk_columns = set(db.get_primary_keys(schema_name, table_name))
        invalid_selections: list[str] = []

        all_relations = db.get_all_foreign_keys(schema_name)
        all_saved_plans = db.get_all_saved_plans(schema_name)

        for row in plan_data:
            col_name = row.get("column", "")
            strategy = row.get("strategy", "keep").lower()

            if col_name in col_details:
                col_info = col_details[col_name]
                sql_type = col_info["type"].lower()
                is_nullable = col_info["nullable"]
                is_sensitive = _is_sensitive_row(row)

                if is_sensitive and strategy == "keep":
                    invalid_selections.append(
                        f"❌ Sensitive column `{col_name}` cannot use strategy `keep`."
                    )
                    continue

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
                if col_name in indexed_columns and strategy in {
                    "mask",
                    "faker_name",
                    "faker_email",
                    "faker_phone",
                    "mapping",
                }:
                    if any(
                        token in sql_type
                        for token in [
                            "bigint",
                            "integer",
                            "smallint",
                            "int",
                            "numeric",
                            "decimal",
                            "double",
                            "real",
                            "boolean",
                            "date",
                            "time",
                            "timestamp",
                        ]
                    ):
                        invalid_selections.append(
                            f"❌ Indexed column `{col_name}` ({sql_type}) cannot use `{strategy}` "
                            "due to index type compatibility."
                        )
                        continue

                if strategy == "null" and is_nullable == "NO":
                    invalid_selections.append(f"❌ Column `{col_name}` is **NOT NULL**.")
                    continue

                is_numeric_id_type = any(
                    token in sql_type for token in ["bigint", "integer", "smallint", "int"]
                )
                if is_numeric_id_type:
                    if col_name in table_pk_columns and strategy != "keep":
                        invalid_selections.append(
                            f"❌ RI Conflict: `{col_name}` must be 'keep' to match referenced keys "
                            f"(e.g., `{table_name}.{col_name}`)."
                        )
                        continue

                for rel in all_relations:
                    if rel[0] == table_name and rel[1] == col_name:
                        parent_table, parent_col = rel[2], rel[3]
                        if is_numeric_id_type and strategy != "keep":
                            invalid_selections.append(
                                f"❌ RI Conflict: `{col_name}` must be 'keep' to match referenced keys "
                                f"(e.g., `{parent_table}.{parent_col}`)."
                            )
                            continue
                        p_plan = all_saved_plans.get(parent_table)
                        if p_plan:
                            p_strat = next(
                                (p["strategy"] for p in p_plan if p["column"] == parent_col), "keep"
                            ).lower()
                            if strategy != p_strat:
                                invalid_selections.append(
                                    f"❌ RI Conflict: `{col_name}` must be `{p_strat}` to match referenced keys "
                                    f"(e.g., `{parent_table}.{parent_col}`)."
                                )

                    elif rel[2] == table_name and rel[3] == col_name:
                        child_table, child_col = rel[0], rel[1]
                        c_plan = all_saved_plans.get(child_table)
                        if c_plan:
                            c_strat = next(
                                (c["strategy"] for c in c_plan if c["column"] == child_col), "keep"
                            ).lower()
                            if strategy != c_strat:
                                invalid_selections.append(
                                    f"❌ RI Conflict: Child rows in `{child_table}` already use `{c_strat}`."
                                )

        if invalid_selections:
            st.error("🛑 **Integrity Violation** - Plan was not saved.")
            for err in invalid_selections:
                st.write(err)
            return False

    except Exception as e:
        st.error(f"System validation error: {e}")
        return False

    clean_plan = get_clean_plan(plan_data)
    safe_where = str(where_clause or "").strip()
    plan_salt = _resolve_plan_salt(db, schema_name, table_name)

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
                                f"❌ Type mismatch on `{col_name}`: anonymized values are not valid integers "
                                f"for `{col_type}`."
                            )
                            return False
                elif any(t in col_type for t in ["numeric", "double", "real", "decimal"]):
                    series = anon_sample[col_name].dropna()
                    if not series.empty:
                        if not series.map(
                            lambda v: isinstance(v, (Decimal, numbers.Number)) and not isinstance(v, bool)
                        ).all():
                            st.error(
                                f"❌ Type mismatch on `{col_name}`: anonymized values are not valid numbers "
                                f"for `{col_type}`."
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
        where_condition=safe_where,
    )

    if not save_success:
        st.error(f"❌ Critical error: Plan for `{table_name}` was not saved to the database.")
        return False
    saved_now = db.get_saved_plan(schema_name, table_name)
    if saved_now and saved_now.get("source_list_mismatch"):
        st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")

    if "completed_tables" not in st.session_state:
        st.session_state["completed_tables"] = set()
    st.session_state["completed_tables"].add(table_name)

    if "selected_tables" in st.session_state and table_name not in st.session_state["selected_tables"]:
        st.session_state["selected_tables"].append(table_name)

    if not advance:
        st.success(f"✅ Plan saved for {table_name}.")
        return True

    all_tables = st.session_state.get("all_tables_list", [])

    next_table = get_next_table_in_chain(table_name, all_tables, st.session_state["completed_tables"])

    if next_table:
        next_table_plan = db.get_saved_plan(schema_name, next_table)
        if next_table_plan:
            st.session_state["plan_active"] = True
            st.session_state["ai_analysis"] = next_table_plan["plan"]
            st.session_state["plan_snapshot"] = next_table_plan["plan"]
            st.session_state[f"where_clause_{next_table}"] = next_table_plan["where"]
            if next_table_plan.get("source_list_mismatch"):
                st.warning("⚠️ Source list version mismatch: Anonymization consistency may be affected.")
            st.session_state["plan_origin"] = "saved"
        else:
            st.session_state["plan_active"] = False

        st.session_state["selected_table_info"] = (next_table, schema_name)

        keys_to_reset = ["ai_analysis", "current_plan", "last_rendered_table", "plan_snapshot", "plan_active"]
        for key in keys_to_reset:
            if key in st.session_state:
                del st.session_state[key]

        st.success(f"✅ Saved! Moving to {next_table}...")
        time.sleep(0.5)
    else:
        st.success("🎯 All tables finalized! Ready for Batch execution.")
        st.session_state["plan_active"] = False
        time.sleep(1)
    return True
