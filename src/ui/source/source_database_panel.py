# -*- coding: utf-8 -*-
"""Database connection form, schema/table pickers, and preview for the Source tab."""

from __future__ import annotations

import logging
import os
from typing import Any, MutableMapping
from urllib.parse import parse_qs, urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import text as sqla_text

from src.logic.app_state import AppState
from src.logic.source_connection import resolve_postgresql_source_url
from src.ui.source import source_utils as su

logger = logging.getLogger(__name__)


def _normalize_jdbc_url(url: str) -> str:
    u = str(url or "").strip()
    if u.startswith("postgres://"):
        return "postgresql://" + u[len("postgres://") :]
    return u


def _db_connection_field_labels(m: MutableMapping[str, Any]) -> tuple[str, str, str, str]:
    """Build Host / Port / Database / Schema widget labels from the resolved URL or session."""
    raw = (resolve_postgresql_source_url(m) or "").strip()
    session_schema = str(m.get("selected_schema") or m.get("source_schema") or "").strip()

    if raw:
        parsed = urlparse(_normalize_jdbc_url(raw))
        host = (parsed.hostname or "").strip() or "—"
        port = str(parsed.port or "").strip()
        if not port:
            eng = str(m.get("db_source_type", "PostgreSQL") or "PostgreSQL")
            port = su.default_port_for_engine(eng)
        path_part = (parsed.path or "").lstrip("/")
        dbn = path_part.split("?")[0].strip() if path_part else ""
        if not dbn:
            dbn = "—"
        q = parse_qs(parsed.query)
        qs_schema = (
            (q.get("schema") or q.get("currentSchema") or q.get("search_path") or [""])[0] or ""
        ).strip()
        schema_disp = qs_schema or session_schema or "public"
    else:
        host = str(m.get("conn_host", "") or "").strip() or "—"
        port = str(m.get("conn_port", "") or "").strip() or su.default_port_for_engine(
            str(m.get("db_source_type", "PostgreSQL"))
        )
        dbn = str(m.get("conn_database_name", "") or "").strip() or "—"
        schema_disp = session_schema or "public"

    return (
        f"Host ({host})",
        f"Port ({port})",
        f"Database ({dbn})",
        f"Schema ({schema_disp})",
    )

__all__ = [
    "pick_default_schema",
    "render_db_engine_subselector",
    "render_db_source_section",
]


def pick_default_schema(schemas: list[str]) -> str:
    """Pick a sensible default schema (``SOURCE_SCHEMA`` env > ecommerce > first non-system)."""
    env_schema = str(os.getenv("SOURCE_SCHEMA", "")).strip()
    if env_schema and env_schema in schemas:
        return env_schema
    if "ecommerce" in schemas:
        return "ecommerce"
    for s in schemas:
        if s and s not in su.SYSTEM_SCHEMAS:
            return s
    return "public"


def fetch_pg_metadata(db: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute a read-only SQL statement through ``db.engine``; return rows as dicts."""
    try:
        engine = getattr(db, "engine", None)
        if engine is None:
            return []
        with engine.connect() as conn:
            result = conn.execute(sqla_text(sql), params)
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Metadata query failed: %s", exc)
        return []


def render_technical_metadata(db: Any, schema: str, table: str) -> None:
    """Render PK, FK, indexes, constraints, triggers, and views for ``schema.table``."""
    cols = st.columns(2)
    with cols[0]:
        st.caption("Primary Keys")
        try:
            pks = db.get_primary_keys(schema, table) or []
        except Exception as exc:  # noqa: BLE001
            pks = []
            st.warning(f"PK fetch failed: {exc}")
        if pks:
            st.code("\n".join(map(str, pks)), language="text")
        else:
            st.caption("None")
    with cols[1]:
        st.caption("Foreign Keys")
        try:
            fks_raw = db.get_all_foreign_keys(schema) or []
        except Exception as exc:  # noqa: BLE001
            fks_raw = []
            st.warning(f"FK fetch failed: {exc}")
        table_fks = [fk for fk in fks_raw if isinstance(fk, (tuple, list)) and table in fk]
        if table_fks:
            st.dataframe(pd.DataFrame(table_fks), width="stretch", hide_index=True)
        else:
            st.caption("None")

    idx_rows = fetch_pg_metadata(
        db,
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = :schema AND tablename = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Indexes")
    if idx_rows:
        st.dataframe(pd.DataFrame(idx_rows), width="stretch", hide_index=True)
    else:
        st.caption("None")

    cons_rows = fetch_pg_metadata(
        db,
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints "
        "WHERE table_schema = :schema AND table_name = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Constraints")
    if cons_rows:
        st.dataframe(pd.DataFrame(cons_rows), width="stretch", hide_index=True)
    else:
        st.caption("None")

    trig_rows = fetch_pg_metadata(
        db,
        "SELECT DISTINCT trigger_name, event_manipulation, action_timing "
        "FROM information_schema.triggers "
        "WHERE event_object_schema = :schema AND event_object_table = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Triggers")
    if trig_rows:
        st.dataframe(pd.DataFrame(trig_rows), width="stretch", hide_index=True)
    else:
        st.caption("None")

    view_rows = fetch_pg_metadata(
        db,
        "SELECT table_name FROM information_schema.views WHERE table_schema = :schema",
        {"schema": schema},
    )
    st.caption(f"Views in `{schema}`")
    if view_rows:
        st.dataframe(pd.DataFrame(view_rows), width="stretch", hide_index=True)
    else:
        st.caption("None")


def render_db_engine_subselector(locked: bool) -> None:
    """Engine pick when Database is active; drives default port on change."""
    cur = st.session_state.get("db_source_type", "PostgreSQL")
    try:
        idx = su.DB_ENGINES.index(cur)
    except ValueError:
        idx = 0
    st.selectbox(
        " ",
        options=su.DB_ENGINES,
        index=idx,
        key="db_source_type",
        disabled=locked,
        label_visibility="collapsed",
        on_change=su.on_db_engine_change,
        help="Database engine. Changing this sets Port to the usual default for that engine.",
    )


def render_db_source_section(db: Any, app: AppState, locked: bool = False) -> None:
    """Render the Database Source expander (connection, schema/tables, preview)."""
    m = app.mapping
    with st.expander("Database Source", expanded=True):
        st.caption(
            ".env values seed defaults (`SOURCE_DB_URL` or `DATABASE_URL`); edits live in session until "
            "you click **Confirm Source**, which persists them to `.env`."
        )

        c1, c2 = st.columns(2)
        with c1:
            st.text_input("Name", key="db_source_name", placeholder="primary_postgres", disabled=locked)
        with c2:
            st.text_input("Alias", key="db_source_alias", placeholder="prod-pg", disabled=locked)

        st.text_input(
            "Connection String",
            key="db_source_conn_string",
            placeholder="postgresql://user:pass@host:5432/dbname",
            disabled=locked,
        )

        cu, cpw = st.columns(2)
        with cu:
            st.text_input("User", key="conn_user", disabled=locked)
        with cpw:
            st.text_input("Password", type="password", key="conn_password", disabled=locked)

        hds_disabled = bool(m.get("source_locked", False))

        host_l, port_l, db_l, schema_l = _db_connection_field_labels(m)
        ch, cport = st.columns(2)
        with ch:
            st.text_input(host_l, key="conn_host", disabled=hds_disabled)
        with cport:
            st.text_input(port_l, key="conn_port", disabled=locked)

        st.text_input(db_l, key="conn_database_name", disabled=hds_disabled)

        try:
            schemas = db.get_all_schemas() or []
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not list schemas: {exc}")
            schemas = []

        if schemas:
            default_schema = pick_default_schema(schemas)
            current_schema = m.get("selected_schema") or default_schema
            if current_schema not in schemas:
                current_schema = default_schema
            schema_choice = st.selectbox(
                schema_l,
                options=schemas,
                index=schemas.index(current_schema),
                key="db_source_schema_select",
                disabled=hds_disabled,
            )
            m["selected_schema"] = schema_choice
            m["source_schema"] = schema_choice
        else:
            schema_choice = st.text_input(
                schema_l,
                key="db_source_schema_text",
                placeholder="public",
                disabled=hds_disabled,
            )
            if schema_choice:
                m["selected_schema"] = schema_choice
                m["source_schema"] = schema_choice

        su.sync_db_config_dict_from_session(m)

        if m.get("db_source_type", "PostgreSQL") != "PostgreSQL":
            st.info(
                "Only PostgreSQL is currently wired to Mappings / Export. Other engines "
                "are configurable here but require a future backend extension to be activated."
            )

        active_schema = str(m.get("selected_schema", "") or "")
        if not active_schema:
            return

        try:
            available_tables = db.get_tables_in_schema(active_schema) or []
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not list tables: {exc}")
            available_tables = []

        st.markdown(f"#### Tables in `{active_schema}`")
        previously_selected = [t for t in m.get("db_source_selected_tables", []) if t in available_tables]
        default_tables = previously_selected or available_tables
        selected_tables = st.multiselect(
            "Select tables to include",
            options=available_tables,
            default=default_tables,
            key="db_source_selected_tables",
            disabled=locked,
        )

        if selected_tables:
            active_idx = 0
            stored_active = m.get("db_source_active_table")
            if stored_active in selected_tables:
                active_idx = selected_tables.index(stored_active)
            active_table = st.selectbox(
                "Active table",
                options=selected_tables,
                index=active_idx,
                key="db_source_active_table",
                disabled=locked,
            )

            try:
                cols_avail = db.get_columns(active_table, active_schema) or []
            except Exception as exc:  # noqa: BLE001
                cols_avail = []
                st.warning(f"Could not list columns: {exc}")
            st.multiselect(
                "SELECT — columns",
                options=cols_avail,
                default=cols_avail,
                key=f"db_source_cols__{active_schema}__{active_table}",
                disabled=locked,
            )
            st.text_input(
                "WHERE clause (preview only)",
                key=f"db_source_where__{active_schema}__{active_table}",
                placeholder="email IS NOT NULL AND created_at > '2024-01-01'",
                disabled=locked,
            )

            with st.expander("Technical Metadata", expanded=False):
                render_technical_metadata(db, active_schema, active_table)

            st.divider()
            if st.button("Preview 20 rows", key="db_source_preview_btn", disabled=locked):
                where_clause = str(
                    m.get(f"db_source_where__{active_schema}__{active_table}", "")
                ).strip() or None
                try:
                    preview_df = db.read_table(
                        active_table,
                        active_schema,
                        where=where_clause,
                        limit=20,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Preview failed: {exc}")
                    su.log_source_event(
                        m,
                        "database",
                        "preview_failed",
                        table=active_table,
                        schema=active_schema,
                        error=str(exc),
                    )
                else:
                    selected_cols = m.get(
                        f"db_source_cols__{active_schema}__{active_table}",
                        list(getattr(preview_df, "columns", [])),
                    )
                    if selected_cols and isinstance(preview_df, pd.DataFrame):
                        keep = [c for c in selected_cols if c in preview_df.columns]
                        if keep:
                            preview_df = preview_df[keep]
                    st.dataframe(preview_df, width="stretch")
                    su.log_source_event(
                        m,
                        "database",
                        "preview",
                        table=active_table,
                        schema=active_schema,
                        rows=int(getattr(preview_df, "shape", (0, 0))[0]),
                        where=where_clause or "",
                    )
