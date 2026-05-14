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

from src.logic import query_mirror
from src.ui.source import source_utils as su


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


_MIRROR_PREVIEW_ROW_CAP = 10


def _table_plan_cache_key(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def _mirror_session_or_saved_plan(db, schema_name: str, table_name: str) -> List[dict]:
    """Plan rows for mirror preview: merge in-session intent over persisted plan.

    Resolution order for **session** rows (unsaved / active intent):
    1. ``current_plan_data[schema.table]`` — live editor buffer from the Mappings tab
    2. ``active_plan`` by bare table name, then by ``schema.table`` cache key
    3. Legacy ``active_plan_by_table[schema.table]``

    **Saved** rows come from ``db.get_saved_plan`` (plan DB / persisted rules).

    When both session and saved rows exist, rows are **merged by column name**:
    saved definitions provide the baseline; session rows overlay the same column
    so strategy / sensitivity flags reflect unsaved edits.
    """
    skey = _table_plan_cache_key(schema_name, table_name)

    session_rows: List[dict] | None = None
    cpd = st.session_state.get("current_plan_data") or {}
    if isinstance(cpd, dict):
        block = cpd.get(skey)
        if isinstance(block, dict) and isinstance(block.get("plan"), list) and block["plan"]:
            session_rows = [dict(r) for r in block["plan"] if isinstance(r, dict)]

    if session_rows is None:
        active_plan = st.session_state.get("active_plan") or {}
        if isinstance(active_plan, dict):
            block = active_plan.get(table_name)
            if isinstance(block, dict) and isinstance(block.get("mappings"), list) and block["mappings"]:
                session_rows = [dict(r) for r in block["mappings"] if isinstance(r, dict)]
            else:
                block = active_plan.get(skey)
                if isinstance(block, dict) and isinstance(block.get("mappings"), list) and block["mappings"]:
                    session_rows = [dict(r) for r in block["mappings"] if isinstance(r, dict)]

    if session_rows is None:
        legacy = (st.session_state.get("active_plan_by_table") or {}).get(skey)
        if isinstance(legacy, list) and legacy:
            session_rows = [dict(r) for r in legacy if isinstance(r, dict)]

    saved = db.get_saved_plan(schema_name, table_name)
    saved_rows: List[dict] = []
    if saved and isinstance(saved.get("plan"), list):
        saved_rows = [dict(r) for r in saved["plan"] if isinstance(r, dict)]

    if not session_rows:
        return saved_rows
    if not saved_rows:
        return list(session_rows)

    by_col: dict[str, dict] = {}
    order: List[str] = []
    for r in saved_rows:
        col = str(r.get("column", "")).strip()
        if not col:
            continue
        by_col[col] = dict(r)
        order.append(col)
    for r in session_rows:
        col = str(r.get("column", "")).strip()
        if not col:
            continue
        if col in by_col:
            merged = {**by_col[col], **r}
            by_col[col] = merged
        else:
            by_col[col] = dict(r)
            order.append(col)
    return [by_col[c] for c in order]


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


def _sync_destination_mode_session() -> str:
    """Set ``st.session_state.destination_mode`` to ``memory`` or ``database`` for this tab."""
    raw = su.get_plan_destination_mode(st.session_state)
    mode = "database" if raw == "database" else "memory"
    st.session_state["destination_mode"] = mode
    return mode


def _on_mirror_table_pick() -> None:
    """When the comparison table pick changes, refresh the default SQL template."""
    t = str(st.session_state.get("mirror_comparison_table_pick") or "").strip()
    if not t:
        return
    sch = str(st.session_state.get("selected_schema") or "public")
    try:
        st.session_state["mirror_sql_text_widget"] = (
            f"SELECT * FROM {_quote_ident(sch)}.{_quote_ident(t)} LIMIT {_MIRROR_PREVIEW_ROW_CAP}"
        )
        st.session_state["mirror_display_sql"] = ""
    except StreamlitAPIException:
        pass


def _on_mirror_sql_edit() -> None:
    """Original SQL edits invalidate the last mirrored preview until Run is clicked again."""
    try:
        st.session_state["mirror_display_sql"] = ""
    except StreamlitAPIException:
        pass


def _dedupe_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure unique column names so Arrow / ``st.dataframe`` accepts the frame (e.g. JOINs).

    Original SQL is unchanged; only display-side column labels are adjusted.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            "_".join(str(p) for p in tup if str(p) != "") or f"col_{i}"
            for i, tup in enumerate(out.columns)
        ]
    names = list(out.columns)
    if len(names) == len(set(names)):
        return out
    used: Set[str] = set()
    new_names: List[str] = []
    for c in names:
        base = str(c)
        candidate = base
        k = 0
        while candidate in used:
            k += 1
            candidate = f"{base}_{k}"
        used.add(candidate)
        new_names.append(candidate)
    out.columns = new_names
    return out


def _run_sql_on_source(
    db, sql: str, search_path: List[str], row_limit: int = _MIRROR_PREVIEW_ROW_CAP
) -> Tuple[pd.DataFrame, Optional[str]]:
    engine = db.source_engine
    path_clause = ", ".join(_quote_ident(s) for s in search_path if s)
    inner = str(sql).strip()
    limited = f"SELECT * FROM (\n{inner}\n) AS _mirror_sub LIMIT {int(row_limit)}"
    stmt = sql_text(limited)
    try:
        with engine.begin() as conn:
            if path_clause:
                conn.execute(sql_text(f"SET LOCAL search_path TO {path_clause}, public"))
            df = pd.read_sql(stmt, conn)
        return df, None
    except Exception as exc:
        return pd.DataFrame(), f"Query failed (source): {exc}"


def render_selection_tab(db) -> None:
    _sync_destination_mode_session()
    dm = str(st.session_state.get("destination_mode") or "memory")

    if dm == "database":
        st.success(
            "🗄️ **Destination: Physical Database (Persistence Mode)**  \n"
            "*Changes will be persisted to the target schema.*"
        )
    else:
        st.info(
            "📍 **Destination: In-Memory (Preview Mode)**  \n"
            "*Changes are temporary and visible in real-time.*"
        )

    source_schema = str(st.session_state.get("selected_schema") or "public")
    table_names: List[str] = list(
        st.session_state.get("all_tables_list")
        or st.session_state.get("selected_tables")
        or []
    )

    tbl_pick_key = "mirror_comparison_table_pick"
    if table_names:
        if tbl_pick_key not in st.session_state or st.session_state.get(tbl_pick_key) not in table_names:
            st.session_state[tbl_pick_key] = table_names[0]
        st.selectbox(
            "Table",
            options=table_names,
            key=tbl_pick_key,
            disabled=not (
                bool(st.session_state.get("source_confirmed"))
                and bool(st.session_state.get("active_plan_db_key"))
            ),
            on_change=_on_mirror_table_pick,
            help="Pick a table to seed the SQL template (read-only queries on the source DB).",
        )
    else:
        st.caption("No tables loaded yet.")

    st.markdown("### 🆚 Source vs. Anonymized")
    st.caption(
        "Edit **Original SQL** on the left (read-only execution on the **source**). "
        "**Mirrored SQL** on the right is built only after you click **Run Mirror Query**; "
        "result previews apply the active plan in memory (row fetch capped below)."
    )

    if not st.session_state.get("source_confirmed"):
        st.warning("Confirm the source in **Tab 1 (Source)** before using mirror queries.")
        return
    if not bool(st.session_state.get("active_plan_db_key")):
        st.warning("Activate a **plan database** in the sidebar so plans and salts resolve correctly.")
        return

    if not table_names:
        st.info("No tables discovered for the current source schema. Initialize the session from the Source tab.")
        return

    st.session_state.setdefault("mirror_display_sql", "")

    sql_widget_key = "mirror_sql_text_widget"

    if sql_widget_key not in st.session_state:
        t0 = str(st.session_state.get(tbl_pick_key) or table_names[0])
        if t0 not in table_names:
            t0 = table_names[0]
        try:
            st.session_state[sql_widget_key] = (
                f"SELECT * FROM {_quote_ident(source_schema)}.{_quote_ident(t0)} LIMIT {_MIRROR_PREVIEW_ROW_CAP}"
            )
        except StreamlitAPIException:
            st.session_state[sql_widget_key] = f"SELECT 1 LIMIT {_MIRROR_PREVIEW_ROW_CAP}"

    try:
        sql_live = str(st.session_state.get(sql_widget_key, "") or "").strip()
    except (StreamlitAPIException, KeyError, TypeError):
        sql_live = ""

    mirror_ok = False
    df_raw = pd.DataFrame()
    df_anon = pd.DataFrame()
    merged_plan: List[dict] = []

    run = st.button(
        "Run Mirror Query",
        type="primary",
        key="mirror_run_btn",
        use_container_width=True,
    )

    if run:
        if not sql_live:
            st.warning("Enter a SQL query.")
            st.session_state["mirror_display_sql"] = ""
        elif not _is_safe_readonly_sql(sql_live):
            st.error("Only a single SELECT or WITH statement is allowed (no semicolons, no writes).")
            st.session_state["mirror_display_sql"] = ""
        else:
            with st.spinner("Running on source and applying plan in memory…"):
                df_try, err_raw = _run_sql_on_source(
                    db,
                    sql_live,
                    search_path=[source_schema, "public"],
                )
            if err_raw:
                st.error(err_raw)
                st.session_state["mirror_display_sql"] = ""
            else:
                df_try = _dedupe_dataframe_columns(df_try)
                if df_try.empty:
                    st.warning("Query returned no rows.")
                    mirrored_display, any_mirror_transform = query_mirror.build_mirrored_sql(
                        sql_live,
                        source_schema=source_schema,
                        destination_mode=dm,
                        session=st.session_state,
                        db=db,
                        plan_salt=_mirror_effective_salt(db, source_schema, table_names),
                        df_preview_source=df_try,
                        df_preview_anon=df_try,
                    )
                    st.session_state["mirror_display_sql"] = mirrored_display
                    st.session_state["mirror_last_transform_applied"] = bool(any_mirror_transform)
                    mirror_ok = True
                    df_raw = df_try
                    df_anon = df_try
                else:
                    merged_plan = _merged_plan_for_columns(
                        db, source_schema, table_names, list(df_try.columns)
                    )
                    plan_salt = _mirror_effective_salt(db, source_schema, table_names)
                    if not merged_plan:
                        st.warning(
                            "No plan rows matched the result columns (check **Mappings** or save plans for relevant tables). "
                            "Showing raw data on both sides."
                        )
                        df_anon_try = df_try.copy()
                    else:
                        try:
                            df_anon_try = db.apply_anonymization_rules(df_try, merged_plan, salt=plan_salt)
                        except Exception as exc:
                            st.error(f"In-memory anonymization failed: {exc}")
                            df_anon_try = df_try.copy()
                    df_anon_try = _dedupe_dataframe_columns(df_anon_try)
                    mirrored_display, any_mirror_transform = query_mirror.build_mirrored_sql(
                        sql_live,
                        source_schema=source_schema,
                        destination_mode=dm,
                        session=st.session_state,
                        db=db,
                        plan_salt=plan_salt,
                        df_preview_source=df_try,
                        df_preview_anon=df_anon_try,
                    )
                    st.session_state["mirror_display_sql"] = mirrored_display
                    st.session_state["mirror_last_transform_applied"] = bool(any_mirror_transform)
                    mirror_ok = True
                    df_raw = df_try
                    df_anon = df_anon_try

    try:
        col_sql_l, col_sql_r = st.columns([1, 1], gap="small")
    except TypeError:
        col_sql_l, col_sql_r = st.columns([1, 1])

    with col_sql_l:
        st.markdown("##### 📄 Original SQL")
        st.text_area(
            "SQL query",
            height=220,
            key=sql_widget_key,
            label_visibility="collapsed",
            help="Executed on the source database (read-only).",
            on_change=_on_mirror_sql_edit,
        )

    with col_sql_r:
        st.markdown("##### 🛡️ Mirrored SQL")
        try:
            mirrored_body = str(st.session_state.get("mirror_display_sql", "") or "")
        except (StreamlitAPIException, KeyError, TypeError):
            mirrored_body = ""
        st.code(mirrored_body, language="sql")
        if run and mirror_ok and not st.session_state.get("mirror_last_transform_applied", True):
            st.caption("📍 Running against: [Anonymized Target Mode]")

    st.caption("Only read-only `SELECT` / `WITH` queries are executed on the **source** connection.")

    if not run:
        return

    if not mirror_ok:
        return

    try:
        sql_raw = str(st.session_state.get(sql_widget_key, "") or "").strip()
    except (StreamlitAPIException, KeyError, TypeError):
        sql_raw = ""

    try:
        st.session_state["mirror_last_mode"] = "in_memory_preview"
    except StreamlitAPIException:
        pass

    if df_raw.empty:
        st.warning("Query returned no rows.")
        st.markdown("#### 📂 Raw Source Output")
        st.info("No data.")
        st.divider()
        st.markdown("#### 🛡️ Anonymized Target Output")
        st.info("No data.")
        return

    n_total = len(df_raw)
    preview_n = n_total
    df_raw_view = df_raw
    df_anon_view = df_anon

    st.caption(
        f"Preview: **{preview_n}** row(s) shown (queries are executed with a hard cap of "
        f"**{_MIRROR_PREVIEW_ROW_CAP}** rows)."
    )

    st.markdown("#### 📂 Raw Source Output")
    st.caption(f"Source engine · `search_path`: `{source_schema}`")
    st.dataframe(df_raw_view, use_container_width=True, height=400)

    st.divider()

    st.markdown("#### 🛡️ Anonymized Target Output")
    st.caption("In-memory preview · `db.apply_anonymization_rules`")
    st.dataframe(df_anon_view, use_container_width=True, height=400)

    st.success(f"**{n_total}** row(s); same shape as source, values transformed per plan.")

    with st.expander("SQL executed (source)", expanded=False):
        st.code(sql_raw, language="sql")
    if merged_plan:
        with st.expander("Plan rows applied to this result (by column)", expanded=False):
            st.json(merged_plan)
