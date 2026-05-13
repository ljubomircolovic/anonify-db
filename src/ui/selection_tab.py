# -*- coding: utf-8 -*-
"""
Mirror Query tab: run SQL on the source database, then preview anonymization
in memory using the active / saved plan — no physical anonymized DB required.
"""
from __future__ import annotations

import re
from typing import List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitAPIException
from sqlalchemy import text as sql_text


def _quote_ident(ident: str) -> str:
    safe = str(ident).replace('"', '""')
    return f'"{safe}"'


def _is_safe_readonly_sql(sql: str) -> bool:
    s = str(sql or "").strip()
    if not s:
        return False
    if ";" in s.rstrip(";"):
        return False
    head = s.lstrip().lower()
    return head.startswith("select") or head.startswith("with")


def _table_plan_cache_key(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def _mirror_session_or_saved_plan(db, schema_name: str, table_name: str) -> List[dict]:
    """Planner order: in-session mappings first, then persisted DB plan."""
    active_plan = st.session_state.get("active_plan", {}) or {}
    block = active_plan.get(table_name) if isinstance(active_plan, dict) else None
    if isinstance(block, dict) and isinstance(block.get("mappings"), list):
        return list(block["mappings"])
    skey = _table_plan_cache_key(schema_name, table_name)
    block = active_plan.get(skey) if isinstance(active_plan, dict) else None
    if isinstance(block, dict) and isinstance(block.get("mappings"), list):
        return list(block["mappings"])
    legacy = (st.session_state.get("active_plan_by_table") or {}).get(skey)
    if isinstance(legacy, list):
        return list(legacy)
    saved = db.get_saved_plan(schema_name, table_name)
    if saved and isinstance(saved.get("plan"), list):
        return list(saved["plan"])
    return []


def _merged_plan_for_columns(
    db,
    schema_name: str,
    table_names: List[str],
    df_columns: List[str],
) -> List[dict]:
    """
    Build one plan list covering result columns: first mapping wins per column name
    (tables scanned in session / execution order).
    """
    colset = set(df_columns)
    seen: Set[str] = set()
    merged: List[dict] = []
    for table in table_names or []:
        for row in _mirror_session_or_saved_plan(db, schema_name, table):
            if not isinstance(row, dict):
                continue
            col = row.get("column")
            if not col or col not in colset or col in seen:
                continue
            seen.add(col)
            merged.append(row)
    return merged


def _mirror_effective_salt(db, schema_name: str, table_names: List[str]) -> str:
    for t in table_names or []:
        saved = db.get_saved_plan(schema_name, t)
        if saved and saved.get("salt"):
            return str(saved["salt"])
    if table_names:
        try:
            salt, _, _ = db.ensure_plan_security_metadata(schema_name, table_names[0])
            return str(salt)
        except Exception:
            pass
    return str(getattr(db, "runtime_salt", None) or "default_plan_salt")


def _run_sql_on_source(db, sql: str, search_path: List[str]) -> Tuple[pd.DataFrame, Optional[str]]:
    engine = db.source_engine
    path_clause = ", ".join(_quote_ident(s) for s in search_path if s)
    stmt = sql_text(str(sql))
    try:
        with engine.begin() as conn:
            if path_clause:
                conn.execute(sql_text(f"SET LOCAL search_path TO {path_clause}, public"))
            df = pd.read_sql(stmt, conn)
        return df, None
    except Exception as exc:
        return pd.DataFrame(), f"Query failed (source): {exc}"


def render_selection_tab(db) -> None:
    st.markdown("### 🆚 Raw vs. Anonymized Selection")
    st.caption(
        "Run **read-only** SQL against the **source** database. The right column shows the same "
        "rows after applying your anonymization rules **in memory** (no export target required)."
    )
    st.info(
        "Note: Preview generated in-memory based on current Anonymization Plan "
        "(Mappings tab / saved plans). Column names match the query result; only values are transformed."
    )

    if not st.session_state.get("source_confirmed"):
        st.warning("Confirm the source in **Tab 1 (Source)** before using mirror queries.")
        return
    if not bool(st.session_state.get("active_plan_db_key")):
        st.warning("Activate a **plan database** in the sidebar so plans and salts resolve correctly.")
        return

    source_schema = str(st.session_state.get("selected_schema") or "public")
    table_names: List[str] = list(
        st.session_state.get("all_tables_list")
        or st.session_state.get("selected_tables")
        or []
    )
    if not table_names:
        st.info("No tables discovered for the current source schema. Initialize the session from the Source tab.")
        return

    sql_widget_key = "mirror_sql_text_widget"
    st.session_state.pop("mirror_sql_buffer", None)

    if sql_widget_key not in st.session_state:
        t0 = table_names[0]
        try:
            st.session_state[sql_widget_key] = (
                f"SELECT * FROM {_quote_ident(source_schema)}.{_quote_ident(t0)} LIMIT 50"
            )
        except StreamlitAPIException:
            st.session_state[sql_widget_key] = "SELECT 1 LIMIT 1"

    st.markdown("**Shared SQL**")
    st.text_area(
        "SQL query",
        height=220,
        key=sql_widget_key,
        label_visibility="collapsed",
    )

    col_btn, col_hint = st.columns([1, 3])
    with col_btn:
        run = st.button("Run Mirror Query", type="primary", key="mirror_run_btn")
    with col_hint:
        st.caption("Only read-only `SELECT` / `WITH` queries are executed on the **source** connection.")

    if not run:
        return

    try:
        sql_raw = str(st.session_state.get(sql_widget_key, "") or "").strip()
    except (StreamlitAPIException, KeyError, TypeError):
        sql_raw = ""
    if not sql_raw:
        st.warning("Enter a SQL query.")
        return
    if not _is_safe_readonly_sql(sql_raw):
        st.error("Only a single SELECT or WITH statement is allowed (no semicolons, no writes).")
        return

    try:
        st.session_state["mirror_last_mode"] = "in_memory_preview"
    except StreamlitAPIException:
        pass

    with st.spinner("Running on source and applying plan in memory…"):
        df_raw, err_raw = _run_sql_on_source(
            db,
            sql_raw,
            search_path=[source_schema, "public"],
        )

    if err_raw:
        st.error(err_raw)
        return

    if df_raw.empty:
        st.warning("Query returned no rows.")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 📂 Raw Source Output")
            st.info("No data.")
        with c2:
            st.markdown("#### 🛡️ Anonymized Target Output")
            st.info("No data.")
        return

    merged_plan = _merged_plan_for_columns(db, source_schema, table_names, list(df_raw.columns))
    plan_salt = _mirror_effective_salt(db, source_schema, table_names)

    if not merged_plan:
        st.warning(
            "No plan rows matched the result columns (check **Mappings** or save plans for relevant tables). "
            "Showing raw data on both sides."
        )
        df_anon = df_raw.copy()
    else:
        try:
            df_anon = db.apply_anonymization_rules(df_raw, merged_plan, salt=plan_salt)
        except Exception as exc:
            st.error(f"In-memory anonymization failed: {exc}")
            df_anon = df_raw.copy()

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📂 Raw Source Output")
        st.caption(f"Source engine · `search_path`: `{source_schema}`")
        st.dataframe(df_raw, width="stretch", height=400)
    with c2:
        st.markdown("#### 🛡️ Anonymized Target Output")
        st.caption("In-memory preview · `db.apply_anonymization_rules`")
        st.dataframe(df_anon, width="stretch", height=400)

    st.success(f"**{len(df_raw)}** row(s); same shape as source, values transformed per plan.")

    with st.expander("SQL executed (source)", expanded=False):
        st.code(sql_raw, language="sql")
    if merged_plan:
        with st.expander("Plan rows applied to this result (by column)", expanded=False):
            st.json(merged_plan)
