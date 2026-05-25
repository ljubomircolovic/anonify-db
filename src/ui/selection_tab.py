# -*- coding: utf-8 -*-
"""
Mirror Query tab: run SQL on the source database, then preview anonymization
in memory using the active / saved plan — no physical anonymized DB required.
"""
from __future__ import annotations

from html import escape
import re
from typing import List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitAPIException
from sqlalchemy import text as sql_text

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - enforced via requirements.txt
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]

from src.logic import query_mirror
from src.logic.audit import (
    fetch_session_sql_audit_logs,
    log_sql_execution,
    resolve_anonymized_target_database,
    resolve_target_database_name,
    strip_mirror_sql_header,
)
from src.logic.security import SECURITY_POLICY_ERROR, validate_safe_select_query
from src.ui.source import source_utils as su


def _quote_ident(ident: str) -> str:
    safe = str(ident).replace('"', '""')
    return f'"{safe}"'


def _render_sql_box_title(icon: str, title: str, database_name: str) -> None:
    db_label = escape(str(database_name or "unknown-db"))
    st.markdown(
        (
            "<div style='display:flex; align-items:baseline; gap:0.5rem; margin-bottom:0.35rem;'>"
            f"<span style='font-weight:600;'>{icon} {escape(title)}</span>"
            f"<span style='color:#6b7280; font-size:0.85rem;'>[{db_label}]</span>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


_MIRROR_PREVIEW_ROW_CAP = 10
_MASKED_TARGET_VIEW_NAME = "masked_target_view"
_VALIDATION_DEFAULT_QUERY = f"SELECT * FROM {_MASKED_TARGET_VIEW_NAME} LIMIT 50;"
_MIRROR_PREVIEW_STATE_KEYS = (
    "mirror_display_sql",
    "mirror_last_transform_applied",
    "mirror_last_mode",
    "mirror_last_preview_ready",
    "mirror_last_raw_df",
    "mirror_last_anon_df",
    "mirror_last_sql_raw",
    "mirror_last_merged_plan",
    "mirror_last_active_table",
    "mirror_last_source_schema",
    "mirror_last_validation_df",
    "mirror_last_validation_join_cols",
    "mirror_last_validation_error",
    "mirror_validation_exec_result_df",
    "mirror_validation_exec_rewritten_sql",
    "mirror_validation_exec_error",
    "bottom_anonymized_query_result_df",
    "bottom_anonymized_query_sql",
    "bottom_anonymized_query_error",
    "anonymized_query_result_df",
    "anonymized_query_result_sql",
    "anonymized_query_result_error",
    "anonymized_query_result_ran",
)


def _clear_mirror_preview_state(*, reset_validation_query: bool = False) -> None:
    for key in _MIRROR_PREVIEW_STATE_KEYS:
        try:
            st.session_state.pop(key, None)
        except StreamlitAPIException:
            pass
    if reset_validation_query:
        try:
            st.session_state["mirror_validation_sql_widget"] = _VALIDATION_DEFAULT_QUERY
        except StreamlitAPIException:
            pass


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
        _clear_mirror_preview_state(reset_validation_query=True)
        st.session_state["mirror_sql_text_widget"] = (
            f"SELECT * FROM {_quote_ident(sch)}.{_quote_ident(t)} LIMIT {_MIRROR_PREVIEW_ROW_CAP}"
        )
    except StreamlitAPIException:
        pass


def _on_mirror_sql_edit() -> None:
    """Original SQL edits invalidate the last mirrored preview until Run is clicked again."""
    _clear_mirror_preview_state(reset_validation_query=False)


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


def _resolve_validation_join_columns(
    db,
    schema_name: str,
    table_name: str,
    df_raw: pd.DataFrame,
    df_anon: pd.DataFrame,
) -> List[str]:
    raw_cols = {str(c) for c in df_raw.columns}
    anon_cols = {str(c) for c in df_anon.columns}
    common_cols = raw_cols & anon_cols
    if not common_cols:
        return []

    real_pks = [str(c) for c in (db.get_primary_keys(schema_name, table_name) or []) if str(c)]
    join_cols = [col for col in real_pks if col in common_cols]
    if join_cols:
        return join_cols

    identity_candidates = [
        col
        for col in df_raw.columns
        if str(col) in common_cols
        and str(col).lower() in {"id", f"{table_name}_id"}
    ]
    if len(identity_candidates) == 1:
        return [str(identity_candidates[0])]
    return []


def _build_side_by_side_validation_view(
    db,
    schema_name: str,
    table_name: str,
    df_raw: pd.DataFrame,
    df_anon: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    if df_raw is None or df_anon is None or df_raw.empty or df_anon.empty:
        return pd.DataFrame(), [], "Run a preview with rows to build the validation view."

    raw_view = _dedupe_dataframe_columns(df_raw)
    anon_view = _dedupe_dataframe_columns(df_anon)
    join_cols = _resolve_validation_join_columns(
        db,
        schema_name,
        table_name,
        raw_view,
        anon_view,
    )
    if not join_cols:
        return (
            pd.DataFrame(),
            [],
            "The active table primary key (or identity column) is not present in the current result set, so the side-by-side join cannot be built.",
        )

    try:
        import duckdb
    except ImportError:
        return (
            pd.DataFrame(),
            join_cols,
            "Install the `duckdb` package to enable the side-by-side validation view.",
        )

    conn = duckdb.connect(database=":memory:")
    try:
        conn.register("source_preview", raw_view)
        conn.register("anon_preview", anon_view)

        select_raw = [f"s.{_quote_ident(str(col))}" for col in raw_view.columns]
        select_anon = [
            f"a.{_quote_ident(str(col))} AS {_quote_ident(f'anon_{col}')}"
            for col in anon_view.columns
        ]
        join_predicates = [
            f"s.{_quote_ident(col)} = a.{_quote_ident(col)}"
            for col in join_cols
        ]
        order_expr = ", ".join(f"s.{_quote_ident(col)}" for col in join_cols)
        query = (
            "SELECT "
            + ", ".join(select_raw + select_anon)
            + " FROM source_preview AS s"
            + " LEFT JOIN anon_preview AS a ON "
            + " AND ".join(join_predicates)
        )
        if order_expr:
            query += f" ORDER BY {order_expr}"

        return conn.execute(query).df(), join_cols, None
    except Exception as exc:  # noqa: BLE001
        return pd.DataFrame(), join_cols, f"Could not build side-by-side validation view: {exc}"
    finally:
        conn.close()


def _flatten_and_terms(node):
    if exp is None or node is None:
        return []
    if isinstance(node, exp.And):
        return _flatten_and_terms(node.this) + _flatten_and_terms(node.expression)
    return [node]


def _source_columns_referenced_in_expression(
    node,
    validation_columns: Optional[Set[str]] = None,
) -> List[str]:
    if exp is None or node is None:
        return []
    seen: Set[str] = set()
    ordered: List[str] = []
    for col in node.find_all(exp.Column):
        name = str(getattr(col, "name", "") or "").strip().strip('"')
        if not name or name.startswith("anon_"):
            continue
        if validation_columns and (
            name not in validation_columns or f"anon_{name}" not in validation_columns
        ):
            continue
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _validation_entity_view_name(table_name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", str(table_name or "").strip()).strip("_").lower()
    return f"validation_view_{safe or 'active'}"


def _is_identity_like_column(column_name: str, join_columns: List[str]) -> bool:
    name = str(column_name or "").strip().lower()
    if not name:
        return False
    if str(column_name) in set(join_columns or []):
        return True
    return (
        name == "id"
        or name.endswith("_id")
        or name.startswith("id_")
        or "_pk" in name
        or "_fk" in name
    )


def _rewrite_term_source_columns_to_anon(
    term,
    validation_columns: Optional[Set[str]] = None,
):
    if exp is None or term is None:
        return term
    updated = term.copy()
    for col in list(updated.find_all(exp.Column)):
        name = str(getattr(col, "name", "") or "").strip().strip('"')
        if not name or name.startswith("anon_"):
            continue
        if validation_columns and (
            name not in validation_columns or f"anon_{name}" not in validation_columns
        ):
            continue
        col.replace(exp.column(f"anon_{name}"))
    return updated


def _dequalify_expression_columns(node):
    if exp is None or node is None:
        return node
    updated = node.copy()
    for col in list(updated.find_all(exp.Column)):
        name = str(getattr(col, "name", "") or "").strip().strip('"')
        if not name:
            continue
        col.replace(exp.column(name))
    return updated


def _rewrite_validation_view_sql(
    sql_text: str,
    validation_columns: Optional[List[str]] = None,
    *,
    table_name: str,
    join_columns: List[str],
) -> Tuple[str, Optional[str]]:
    text_sql = str(sql_text or "").strip()
    if not text_sql:
        return "", "Enter an anonymized SQL query."
    if not validate_safe_select_query(text_sql):
        return "", SECURITY_POLICY_ERROR
    if sqlglot is None or exp is None:
        return "", "sqlglot is required to rewrite validation-view queries."

    try:
        parsed = sqlglot.parse_one(text_sql, read="duckdb")
    except Exception as exc:  # noqa: BLE001
        return "", f"Could not parse anonymized SQL: {exc}"

    if not isinstance(parsed, exp.Select):
        return "", f"Only direct SELECT statements against `{_MASKED_TARGET_VIEW_NAME}` are supported."

    validation_colset = {str(col) for col in (validation_columns or [])}
    outer_view_name = _MASKED_TARGET_VIEW_NAME

    where = parsed.args.get("where")
    rewritten_where_sql = ""
    if where is not None and where.this is not None:
        dequalified_where = _dequalify_expression_columns(where.this)
        if _source_columns_referenced_in_expression(dequalified_where, validation_colset):
            original_where_sql = dequalified_where.sql(dialect="duckdb", pretty=False)
            rewritten_terms: List[str] = []
            for term in _flatten_and_terms(dequalified_where):
                term_columns = _source_columns_referenced_in_expression(term, validation_colset)
                if not term_columns:
                    rewritten_terms.append(term.sql(dialect="duckdb", pretty=False))
                    continue

                if (
                    isinstance(term, exp.EQ)
                    and isinstance(term.this, exp.Column)
                    and isinstance(term.expression, exp.Literal)
                ):
                    column_name = str(getattr(term.this, "name", "") or "").strip().strip('"')
                    if column_name and column_name in term_columns:
                        anon_column_name = f"anon_{column_name}"
                        literal_sql = term.expression.sql(dialect="duckdb", pretty=False)
                        if term.expression.is_string:
                            rewritten_terms.append(
                                f"{_quote_ident(anon_column_name)} = ("
                                f"SELECT {_quote_ident(anon_column_name)} "
                                f"FROM {_quote_ident(outer_view_name)} "
                                f"WHERE {original_where_sql}"
                                ")"
                            )
                        elif _is_identity_like_column(column_name, join_columns):
                            rewritten_terms.append(f"{_quote_ident(anon_column_name)} = {literal_sql}")
                        else:
                            rewritten_terms.append(f"{_quote_ident(anon_column_name)} = {literal_sql}")
                        continue

                if (
                    isinstance(term, exp.EQ)
                    and isinstance(term.expression, exp.Column)
                    and isinstance(term.this, exp.Literal)
                ):
                    column_name = str(getattr(term.expression, "name", "") or "").strip().strip('"')
                    if column_name and column_name in term_columns:
                        anon_column_name = f"anon_{column_name}"
                        literal_sql = term.this.sql(dialect="duckdb", pretty=False)
                        if term.this.is_string:
                            rewritten_terms.append(
                                f"{_quote_ident(anon_column_name)} = ("
                                f"SELECT {_quote_ident(anon_column_name)} "
                                f"FROM {_quote_ident(outer_view_name)} "
                                f"WHERE {original_where_sql}"
                                ")"
                            )
                        elif _is_identity_like_column(column_name, join_columns):
                            rewritten_terms.append(f"{_quote_ident(anon_column_name)} = {literal_sql}")
                        else:
                            rewritten_terms.append(f"{_quote_ident(anon_column_name)} = {literal_sql}")
                        continue

                rewritten_terms.append(
                    _rewrite_term_source_columns_to_anon(term, validation_colset).sql(
                        dialect="duckdb",
                        pretty=False,
                    )
                )

            rewritten_where_sql = " AND ".join(rewritten_terms).strip()

    rewritten_sql_parts: List[str] = ["SELECT *", f"FROM {outer_view_name}"]
    if rewritten_where_sql:
        rewritten_sql_parts.append(f"WHERE {rewritten_where_sql}")

    order = parsed.args.get("order")
    if order is not None:
        rewritten_order = _rewrite_term_source_columns_to_anon(
            _dequalify_expression_columns(order),
            validation_colset,
        )
        rewritten_sql_parts.append(rewritten_order.sql(dialect="duckdb", pretty=False))

    limit = parsed.args.get("limit")
    if limit is not None:
        rewritten_sql_parts.append(limit.sql(dialect="duckdb", pretty=False))

    offset = parsed.args.get("offset")
    if offset is not None:
        rewritten_sql_parts.append(offset.sql(dialect="duckdb", pretty=False))

    rewritten_sql = " ".join(part for part in rewritten_sql_parts if str(part).strip())
    if not validate_safe_select_query(rewritten_sql):
        return "", SECURITY_POLICY_ERROR
    return rewritten_sql, None


def _execute_validation_view_sql(
    db,
    rewritten_sql: str,
    validation_df: pd.DataFrame,
    *,
    table_name: str,
    source_schema: str,
    destination_mode: str,
) -> Tuple[pd.DataFrame, str, Optional[str]]:
    if validation_df is None or validation_df.empty:
        return pd.DataFrame(), "", "Build the validation view first before running anonymized SQL."
    final_sql = str(rewritten_sql or "").strip()
    if not final_sql:
        return pd.DataFrame(), "", "Generate anonymized SQL from the Original SQL input first."
    if not validate_safe_select_query(final_sql):
        return pd.DataFrame(), final_sql, SECURITY_POLICY_ERROR

    try:
        import duckdb
    except ImportError:
        return (
            pd.DataFrame(),
            final_sql,
            f"Install the `duckdb` package to execute anonymized SQL against `{_MASKED_TARGET_VIEW_NAME}`.",
        )

    conn = duckdb.connect(database=":memory:")
    try:
        deduped_validation_df = _dedupe_dataframe_columns(validation_df)
        staging_view_name = f"{_MASKED_TARGET_VIEW_NAME}__df"
        conn.register(staging_view_name, deduped_validation_df)
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW {_quote_ident(_MASKED_TARGET_VIEW_NAME)} AS "
            f"SELECT * FROM {_quote_ident(staging_view_name)}"
        )
        result_df = conn.execute(final_sql).df()
        try:
            conn.commit()
        except Exception:
            pass
    except Exception as exc:  # noqa: BLE001
        return pd.DataFrame(), final_sql, f"Anonymized SQL failed: {exc}"
    finally:
        conn.close()

    log_sql_execution(
        final_sql,
        "ANONYMIZED",
        resolve_anonymized_target_database(
            db,
            st.session_state,
            destination_mode=destination_mode,
            source_schema=source_schema,
        ),
        db=db,
        session_state=st.session_state,
    )
    return result_df, final_sql, None


def _clear_legacy_validation_exec_state() -> None:
    for key in (
        "mirror_validation_exec_result_df",
        "mirror_validation_exec_rewritten_sql",
        "mirror_validation_exec_error",
    ):
        try:
            st.session_state.pop(key, None)
        except StreamlitAPIException:
            pass


def _set_anonymized_query_result_state(
    sql_text: str,
    result_df: Optional[pd.DataFrame],
    error: Optional[str],
    *,
    ran: bool = True,
) -> None:
    normalized_sql = str(sql_text or "").strip()
    normalized_error = str(error or "").strip()
    safe_df = result_df if isinstance(result_df, pd.DataFrame) else pd.DataFrame()
    if isinstance(safe_df, pd.DataFrame) and not safe_df.empty:
        safe_df = _dedupe_dataframe_columns(safe_df)
    try:
        st.session_state["anonymized_query_result_sql"] = normalized_sql
        st.session_state["anonymized_query_result_error"] = normalized_error
        st.session_state["anonymized_query_result_df"] = safe_df
        st.session_state["anonymized_query_result_ran"] = bool(ran)
        # Keep the existing bottom keys in sync for compatibility with current readers.
        st.session_state["bottom_anonymized_query_sql"] = normalized_sql
        st.session_state["bottom_anonymized_query_error"] = normalized_error
        st.session_state["bottom_anonymized_query_result_df"] = safe_df
    except StreamlitAPIException:
        pass


def _execute_bottom_anonymized_sql(
    db,
    query_sql: str,
    validation_df: pd.DataFrame,
    *,
    validation_error: Optional[str],
    table_name: str,
    source_schema: str,
    destination_mode: str,
) -> Tuple[pd.DataFrame, str, Optional[str]]:
    raw_sql = str(query_sql or "").strip()
    if validation_error:
        return pd.DataFrame(), raw_sql, f"Anonymized SQL failed: {validation_error}"
    result_df, executed_sql, exec_error = _execute_validation_view_sql(
        db,
        raw_sql,
        validation_df,
        table_name=table_name,
        source_schema=source_schema,
        destination_mode=destination_mode,
    )
    if isinstance(result_df, pd.DataFrame) and not result_df.empty:
        result_df = _dedupe_dataframe_columns(result_df)
    return result_df, executed_sql, exec_error


def _render_anonymized_query_result_grid() -> None:
    st.markdown("#### 🧪 Bottom Query Result")
    st.caption("Rows returned by the SQL entered in the bottom `🔒 Anonymized SQL` editor.")

    if not bool(st.session_state.get("anonymized_query_result_ran")):
        st.info("Execute anonymized SQL to render rows here.")
        return

    query_error = str(st.session_state.get("anonymized_query_result_error", "") or "").strip()
    query_result = st.session_state.get("anonymized_query_result_df")
    if query_error:
        st.error(query_error)
        return

    if isinstance(query_result, pd.DataFrame):
        if query_result.empty:
            st.info("Query returned no rows.")
        else:
            st.dataframe(query_result, width="stretch", height=320, hide_index=True)
        return

    st.info("No data.")


def _sync_anonymized_rewritten_sql(
    sql_text: str,
    *,
    table_name: str,
    join_columns: List[str],
    validation_columns: Optional[List[str]] = None,
    force: bool = False,
) -> Optional[str]:
    source_sql = str(sql_text or "").strip()
    if not source_sql:
        try:
            st.session_state["anonymized_rewritten_sql"] = ""
            st.session_state["anonymized_rewritten_sql_source"] = ""
        except StreamlitAPIException:
            pass
        return None

    current_source = str(st.session_state.get("anonymized_rewritten_sql_source", "") or "")
    current_sql = str(st.session_state.get("anonymized_rewritten_sql", "") or "")
    if not force and current_source == source_sql and current_sql:
        return None

    rewritten_sql, rewrite_error = _rewrite_validation_view_sql(
        source_sql,
        validation_columns,
        table_name=table_name,
        join_columns=join_columns,
    )
    try:
        st.session_state["anonymized_rewritten_sql"] = rewritten_sql if not rewrite_error else ""
        st.session_state["anonymized_rewritten_sql_source"] = source_sql if not rewrite_error else ""
    except StreamlitAPIException:
        pass
    return rewrite_error


def _run_sql_on_source(
    db, sql: str, search_path: List[str], row_limit: int = _MIRROR_PREVIEW_ROW_CAP
) -> Tuple[pd.DataFrame, Optional[str]]:
    if not validate_safe_select_query(sql):
        return pd.DataFrame(), SECURITY_POLICY_ERROR

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
        log_sql_execution(
            limited,
            "ORIGINAL",
            resolve_target_database_name(db, st.session_state),
            db=db,
            session_state=st.session_state,
        )
        return df, None
    except Exception as exc:
        return pd.DataFrame(), f"Query failed (source): {exc}"


def _log_anonymized_mirror_sql(
    db,
    mirrored_sql: str,
    *,
    source_schema: str,
    destination_mode: str,
) -> None:
    body = strip_mirror_sql_header(mirrored_sql)
    if not body:
        return
    log_sql_execution(
        body,
        "ANONYMIZED",
        resolve_anonymized_target_database(
            db,
            st.session_state,
            destination_mode=destination_mode,
            source_schema=source_schema,
        ),
        db=db,
        session_state=st.session_state,
    )


def _render_session_audit_log(db) -> None:
    with st.expander("📜 Session Audit Log", expanded=False):
        st.caption("SQL executed in this browser session (newest first).")
        audit_df = fetch_session_sql_audit_logs(db, limit=30)
        if audit_df.empty:
            st.info("No SQL executions logged for this session yet.")
            return

        st.dataframe(
            audit_df[["Time", "User", "Type", "Target DB"]],
            width="stretch",
            hide_index=True,
        )
        st.markdown("##### Query text")
        for _, row in audit_df.iterrows():
            st.markdown(
                f"**{row['Time']}** · `{row['User']}` · **{row['Type']}** · `{row['Target DB']}`"
            )
            st.code(str(row["SQL Query"]), language="sql")


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

    if not st.session_state.get("source_confirmed"):
        st.warning("Confirm the source in **Tab 1 (Source)** before using mirror queries.")
        _render_session_audit_log(db)
        return
    if not bool(st.session_state.get("active_plan_db_key")):
        st.warning("Activate a **plan database** in the sidebar so plans and salts resolve correctly.")
        _render_session_audit_log(db)
        return

    if not table_names:
        st.info("No tables discovered for the current source schema. Initialize the session from the Source tab.")
        _render_session_audit_log(db)
        return

    active_table_name = str(st.session_state.get(tbl_pick_key) or table_names[0])
    st.session_state.setdefault("mirror_display_sql", "")
    st.session_state.setdefault("anonymized_rewritten_sql", "")
    st.session_state.setdefault("bottom_anonymized_query_sql", "")
    st.session_state.setdefault("bottom_anonymized_query_error", "")
    st.session_state.setdefault("bottom_anonymized_query_result_df", pd.DataFrame())
    st.session_state.setdefault("anonymized_query_result_sql", "")
    st.session_state.setdefault("anonymized_query_result_error", "")
    st.session_state.setdefault("anonymized_query_result_df", pd.DataFrame())
    st.session_state.setdefault("anonymized_query_result_ran", False)

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
    try:
        auto_join_columns = list(db.get_primary_keys(source_schema, active_table_name) or [])
    except Exception:
        auto_join_columns = []
    _sync_anonymized_rewritten_sql(
        sql_live,
        table_name=active_table_name,
        join_columns=auto_join_columns,
    )

    mirror_ok = False
    df_raw = pd.DataFrame()
    df_anon = pd.DataFrame()
    merged_plan: List[dict] = []
    sql_raw_display = sql_live

    preview_ready = bool(st.session_state.get("mirror_last_preview_ready"))
    preview_table = str(st.session_state.get("mirror_last_active_table") or "")
    preview_schema = str(st.session_state.get("mirror_last_source_schema") or "")
    if preview_ready and preview_table == active_table_name and preview_schema == source_schema:
        stored_raw = st.session_state.get("mirror_last_raw_df")
        stored_anon = st.session_state.get("mirror_last_anon_df")
        if isinstance(stored_raw, pd.DataFrame) and isinstance(stored_anon, pd.DataFrame):
            df_raw = stored_raw
            df_anon = stored_anon
            merged_plan = list(st.session_state.get("mirror_last_merged_plan") or [])
            sql_raw_display = str(st.session_state.get("mirror_last_sql_raw") or sql_live).strip()
            mirror_ok = True

    source_db_label = resolve_target_database_name(db, st.session_state)
    anonymized_db_label = resolve_anonymized_target_database(
        db,
        st.session_state,
        destination_mode=dm,
        source_schema=source_schema,
    )

    _render_sql_box_title("📄", "Original SQL", source_db_label)
    st.text_area(
        "Original SQL Query",
        height=220,
        key=sql_widget_key,
        label_visibility="collapsed",
        help="Executed on the source database (read-only).",
        on_change=_on_mirror_sql_edit,
    )
    run = st.button(
        "Run Original SQL",
        type="primary",
        key="mirror_run_btn",
        width="stretch",
    )

    if run:
        _clear_mirror_preview_state(reset_validation_query=False)
        if not sql_live:
            st.warning("Enter a SQL query.")
            st.session_state["mirror_display_sql"] = ""
        elif not validate_safe_select_query(sql_live):
            st.error(SECURITY_POLICY_ERROR)
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
                    _log_anonymized_mirror_sql(
                        db,
                        mirrored_display,
                        source_schema=source_schema,
                        destination_mode=dm,
                    )
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
                    _log_anonymized_mirror_sql(
                        db,
                        mirrored_display,
                        source_schema=source_schema,
                        destination_mode=dm,
                    )
                    mirror_ok = True
                    df_raw = df_try
                    df_anon = df_anon_try

                if mirror_ok:
                    try:
                        st.session_state["mirror_last_mode"] = "in_memory_preview"
                        st.session_state["mirror_last_preview_ready"] = True
                        st.session_state["mirror_last_raw_df"] = df_raw
                        st.session_state["mirror_last_anon_df"] = df_anon
                        st.session_state["mirror_last_sql_raw"] = sql_live
                        st.session_state["mirror_last_merged_plan"] = list(merged_plan)
                        st.session_state["mirror_last_active_table"] = active_table_name
                        st.session_state["mirror_last_source_schema"] = source_schema
                    except StreamlitAPIException:
                        pass

    if not mirror_ok:
        _render_session_audit_log(db)
        return

    if df_raw.empty:
        st.warning("Query returned no rows.")
        st.markdown("#### 📂 Raw Source Output")
        st.info("No data.")
        st.divider()
        st.markdown("#### 🛡️ Anonymized Target Output")
        st.info("No data.")
        _render_session_audit_log(db)
        return

    n_total = len(df_raw)
    preview_n = n_total
    df_raw_view = df_raw
    df_anon_view = df_anon
    anonymized_output_df = df_anon_view
    anonymized_output_caption = "In-memory preview · `db.apply_anonymization_rules`"

    validation_df, validation_join_cols, validation_error = _build_side_by_side_validation_view(
        db,
        source_schema,
        active_table_name,
        df_raw_view,
        df_anon_view,
    )
    try:
        st.session_state["mirror_last_validation_df"] = validation_df
        st.session_state["mirror_last_validation_join_cols"] = list(validation_join_cols)
        st.session_state["mirror_last_validation_error"] = validation_error
    except StreamlitAPIException:
        pass

    st.caption(
        f"Preview: **{preview_n}** row(s) shown (queries are executed with a hard cap of "
        f"**{_MIRROR_PREVIEW_ROW_CAP}** rows)."
    )

    st.markdown("#### 📂 Raw Source Output")
    st.caption(f"Source engine · `search_path`: `{source_schema}`")
    st.dataframe(df_raw_view, width="stretch", height=400)

    anon_output_slot = st.container()

    st.success(f"**{n_total}** row(s); same shape as source, values transformed per plan.")

    with st.expander("SQL executed (source)", expanded=False):
        st.code(sql_raw_display, language="sql")
    if merged_plan:
        with st.expander("Plan rows applied to this result (by column)", expanded=False):
            st.json(merged_plan)

    _render_session_audit_log(db)

    validation_slot = st.container()
    validation_result_slot = st.container()

    bottom_sql_section = st.container()
    with bottom_sql_section:
        st.divider()
        _render_sql_box_title("🔒", "Anonymized SQL", anonymized_db_label)
        with st.form("mirror_bottom_anonymized_sql_form", clear_on_submit=False):
            anonymized_sql_live = st.text_area(
                "Anonymized SQL Query",
                height=220,
                key="anonymized_rewritten_sql",
                label_visibility="collapsed",
                help="Auto-generated from the source query and fully editable before execution.",
            )
            run_anonymized_sql = st.form_submit_button(
                "Execute Anonymized SQL",
                key="mirror_run_validation_sql_btn",
                width="stretch",
                disabled=not mirror_ok,
            )

        anonymized_sql_live = str(anonymized_sql_live or "").strip()
        if run_anonymized_sql:
            try:
                result_df, rewritten_sql, exec_error = _execute_bottom_anonymized_sql(
                    db,
                    anonymized_sql_live,
                    validation_df,
                    validation_error=validation_error,
                    table_name=active_table_name,
                    source_schema=source_schema,
                    destination_mode=dm,
                )
            except Exception as exc:  # noqa: BLE001
                result_df = pd.DataFrame()
                rewritten_sql = anonymized_sql_live
                exec_error = f"Anonymized SQL failed: {exc}"

            _clear_legacy_validation_exec_state()
            try:
                st.session_state["anonymized_rewritten_sql"] = rewritten_sql
            except StreamlitAPIException:
                pass
            _set_anonymized_query_result_state(rewritten_sql or anonymized_sql_live, result_df, exec_error)

        _render_anonymized_query_result_grid()

    stored_exec_sql = str(st.session_state.get("mirror_validation_exec_rewritten_sql", "") or "").strip()
    stored_exec_error = str(st.session_state.get("mirror_validation_exec_error", "") or "").strip()
    stored_exec_result = st.session_state.get("mirror_validation_exec_result_df")

    if not stored_exec_error and isinstance(stored_exec_result, pd.DataFrame):
        anonymized_output_df = stored_exec_result
        anonymized_output_caption = "DuckDB validation view result from `🔒 Anonymized SQL`"

    with anon_output_slot:
        st.divider()
        st.markdown("#### 🛡️ Anonymized Target Output")
        st.caption(anonymized_output_caption)
        if stored_exec_error:
            st.error(stored_exec_error)
        else:
            st.dataframe(anonymized_output_df, width="stretch", height=400)

    with validation_slot:
        st.divider()
        st.markdown("### 📊 Side-by-Side Data Validation View")
        if validation_error:
            st.info(validation_error)
        else:
            st.dataframe(validation_df, width="stretch", height=400)

    with validation_result_slot:
        if stored_exec_sql:
            if stored_exec_error:
                st.error(stored_exec_error)
            elif isinstance(stored_exec_result, pd.DataFrame):
                st.markdown("#### 🧪 Anonymized SQL Result")
                if stored_exec_result.empty:
                    st.info("Query returned no rows.")
                else:
                    st.dataframe(stored_exec_result, width="stretch", height=400)
