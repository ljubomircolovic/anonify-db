# -*- coding: utf-8 -*-
"""Source tab UI: Domain, File, Database, API, Source Log sections.

This module replaces the legacy `render_data_source_section` and orchestrates
five independent input groups. Only the Database section is wired end-to-end
to the existing Mappings -> Comparison -> Export pipeline; File and API
sections are functional scaffolds that store config + provide a 20-row
preview in their own session-state slices.

Connection details are read-only with respect to `.env`: values from
`os.getenv("DATABASE_URL")` seed the defaults; user edits live in
`st.session_state` only and never write back.
"""
from __future__ import annotations

import csv
import datetime
import io
import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import text as sqla_text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DOMAIN_OPTIONS = [
    "Customer Data",
    "Financial Records",
    "E-commerce",
    "Healthcare",
    "Custom",
    "Other",
]

_FILE_TYPES = ["CSV", "JSON", "XML", "TXT"]
_ENCODINGS = ["utf-8", "utf-8-sig", "utf-16", "latin-1", "cp1252", "ascii"]
_QUOTING_OPTIONS: dict[str, int] = {
    "QUOTE_MINIMAL": csv.QUOTE_MINIMAL,
    "QUOTE_ALL": csv.QUOTE_ALL,
    "QUOTE_NONNUMERIC": csv.QUOTE_NONNUMERIC,
    "QUOTE_NONE": csv.QUOTE_NONE,
}
_DB_TYPES = ["PostgreSQL", "MySQL", "SQL Server", "Oracle"]
_HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
_SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
_LOG_MAX_ENTRIES = 200

# Master source-selector options. Exactly one is active at any time; switching
# clears the inactive sources' previews and metadata, and persists the choice
# back to `.env` (key `SOURCE_TYPE`) for cross-session memory.
_SOURCE_TYPES = ["Database", "File", "API"]


def _normalize_source_type(raw: str | None) -> str:
    """Map any case/format variant (e.g. `DATABASE`, `database`) to a canonical option."""
    if not raw:
        return _SOURCE_TYPES[0]
    candidate = str(raw).strip().lower()
    for opt in _SOURCE_TYPES:
        if opt.lower() == candidate:
            return opt
    return _SOURCE_TYPES[0]


def _persist_source_type_env(source_type: str) -> None:
    """Best-effort persist `SOURCE_TYPE=<value>` back to `.env`.

    Silent on failure: a missing `.env` file or write error must not break the
    UI. The value is uppercased on write so it reads naturally as an env var
    (`SOURCE_TYPE=DATABASE`).
    """
    try:
        from dotenv import set_key, find_dotenv

        env_path = find_dotenv(usecwd=True)
        if not env_path:
            return
        set_key(env_path, "SOURCE_TYPE", source_type.upper(), quote_mode="never")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not persist SOURCE_TYPE to .env: %s", exc)


def _clear_inactive_source_state(active_type: str) -> None:
    """Drop previews/loaded artefacts for sources other than `active_type`.

    Only ephemeral preview/metadata keys are cleared. Configuration inputs
    (host/port/file path/API URL) survive so the user does not lose their
    typed values when they toggle back.
    """
    ss = st.session_state
    inactive_keys: dict[str, list[str]] = {
        "File": [
            "file_source_df",
            "file_source_size",
            "file_source_column_map",
            "file_source_column_map_editor",
        ],
        "API": [
            "api_source_last_response",
        ],
        "Database": [
            # DB previews aren't persisted; nothing to clear when DB becomes inactive.
        ],
    }
    for source, keys in inactive_keys.items():
        if source == active_type:
            continue
        for k in keys:
            ss.pop(k, None)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def render_source_tab(db: Any) -> None:
    """Render the Source tab with exclusive source-type selection.

    Layout (top -> bottom):
        1. Domain Selection           — applies to whichever source is active
        2. Master Source Selector     — Database / File / API (radio, horizontal)
        3. Active source section      — exactly one of [DB, File, API]
        4. Source Log                 — chronological audit of source events

    Switching the master selector clears the inactive sources' previews and
    metadata (configuration fields are preserved) and persists the new choice
    to `.env` via `SOURCE_TYPE`.
    """
    _init_source_state()
    _render_domain_section()
    _render_master_source_selector()

    active = _normalize_source_type(st.session_state.get("source_type"))
    if active == "Database":
        _render_db_source_section(db)
    elif active == "File":
        _render_file_source_section()
    elif active == "API":
        _render_api_source_section()

    _render_source_log_section()


def _on_source_type_change() -> None:
    """Streamlit on_change callback for the master source-type radio."""
    new_type = _normalize_source_type(st.session_state.get("source_type"))
    st.session_state["source_type"] = new_type  # ensure canonical case
    _clear_inactive_source_state(new_type)
    _persist_source_type_env(new_type)
    _log_source_event("system", "source_type_changed", to=new_type)


def _render_master_source_selector() -> None:
    """Top-level radio that determines which source section is rendered."""
    current = _normalize_source_type(st.session_state.get("source_type"))
    st.session_state["source_type"] = current  # normalize before widget reads it

    st.markdown("#### Active Data Source")
    st.radio(
        "Select Active Data Source",
        options=_SOURCE_TYPES,
        horizontal=True,
        key="source_type",
        label_visibility="collapsed",
        on_change=_on_source_type_change,
        help=(
            "Only the selected source's configuration is rendered. Switching clears "
            "the previous preview/metadata. The choice is persisted to .env as "
            "`SOURCE_TYPE` so the next session opens on the same source."
        ),
    )
    st.caption(f"Editing **{current}** source configuration.")


# ---------------------------------------------------------------------------
# State init + log helper
# ---------------------------------------------------------------------------
def _init_source_state() -> None:
    """Seed all default session-state keys this tab owns."""
    ss = st.session_state
    ss.setdefault("source_event_log", [])
    ss.setdefault("source_domain", _DOMAIN_OPTIONS[0])

    # Master source-type selector. SOURCE_TYPE env var wins on first render,
    # then session_state owns the value (and writes back to .env on changes).
    ss.setdefault(
        "source_type",
        _normalize_source_type(os.getenv("SOURCE_TYPE")),
    )

    # File source
    ss.setdefault("file_source_name", "")
    ss.setdefault("file_source_path", "")
    ss.setdefault("file_source_type", _FILE_TYPES[0])
    ss.setdefault("file_source_encoding", "utf-8")
    ss.setdefault("file_source_delimiter", ",")
    ss.setdefault("file_source_quotechar", '"')
    ss.setdefault("file_source_quoting", "QUOTE_MINIMAL")
    ss.setdefault("file_source_escapechar", "")
    ss.setdefault("file_source_doublequote", True)
    ss.setdefault("file_source_has_header", True)
    ss.setdefault("file_source_row_limit", 100)
    ss.setdefault("file_source_where", "")
    ss.setdefault("file_source_df", None)
    ss.setdefault("file_source_size", None)

    # API source
    ss.setdefault("api_source_url", "")
    ss.setdefault("api_source_method", "GET")
    ss.setdefault("api_source_api_key", "")
    ss.setdefault("api_source_secret", "")
    ss.setdefault("api_source_body", "")
    ss.setdefault("api_source_last_response", None)


def _log_source_event(kind: str, event: str, **details: Any) -> None:
    """Append a bounded entry to the in-memory Source event log."""
    log: list = st.session_state.setdefault("source_event_log", [])
    log.append(
        {
            "ts": datetime.datetime.now().isoformat(timespec="seconds"),
            "source": kind,
            "event": event,
            "details": json.dumps(details, default=str) if details else "",
        }
    )
    if len(log) > _LOG_MAX_ENTRIES:
        del log[: len(log) - _LOG_MAX_ENTRIES]


def _format_size(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "—"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 ** 2:
        return f"{num_bytes / 1024:.1f} KB"
    if num_bytes < 1024 ** 3:
        return f"{num_bytes / (1024 ** 2):.2f} MB"
    return f"{num_bytes / (1024 ** 3):.2f} GB"


# ---------------------------------------------------------------------------
# Section 1 — Domain Selection
# ---------------------------------------------------------------------------
def _render_domain_section() -> None:
    st.markdown("#### Data Domain")
    current = st.session_state.get("source_domain", _DOMAIN_OPTIONS[0])
    try:
        default_idx = _DOMAIN_OPTIONS.index(current)
    except ValueError:
        default_idx = 0
    domain = st.selectbox(
        "Data Domain",
        options=_DOMAIN_OPTIONS,
        index=default_idx,
        key="source_domain",
        help="Classify the source so downstream AI rules can apply domain-specific heuristics.",
    )
    # Mirror to keys consumed by the rest of the app.
    st.session_state["data_source_domain_type"] = domain
    st.session_state["data_domain"] = domain
    plan_md = st.session_state.setdefault("plan_metadata", {})
    if isinstance(plan_md, dict):
        plan_md["data_domain"] = domain


# ---------------------------------------------------------------------------
# Section 2 — File Source
# ---------------------------------------------------------------------------
def _load_file_dataframe(
    *,
    raw_bytes: bytes,
    file_type: str,
    encoding: str,
    delimiter: str,
    quotechar: str,
    quoting: int,
    escapechar: str | None,
    doublequote: bool,
    has_header: bool,
) -> pd.DataFrame:
    """Parse uploaded/path bytes into a DataFrame based on type."""
    if file_type in {"CSV", "TXT"}:
        text_buf = io.StringIO(raw_bytes.decode(encoding, errors="replace"))
        return pd.read_csv(
            text_buf,
            sep=delimiter or ",",
            quotechar=quotechar or '"',
            quoting=quoting,
            escapechar=(escapechar or None),
            doublequote=bool(doublequote),
            header=0 if has_header else None,
            encoding=encoding,
            engine="python",
        )
    if file_type == "JSON":
        return pd.read_json(io.StringIO(raw_bytes.decode(encoding, errors="replace")))
    if file_type == "XML":
        return pd.read_xml(io.BytesIO(raw_bytes))
    raise ValueError(f"Unsupported file type: {file_type}")


def _render_file_source_section() -> None:
    with st.expander("File Source (CSV / JSON / XML / TXT)", expanded=True):
        st.caption(
            "Define a file source. The file is parsed into a Pandas DataFrame for preview only — "
            "it is not yet routed through Mappings / Export."
        )

        c1, c2, c3 = st.columns([1.5, 1, 1])
        with c1:
            st.text_input("Name", key="file_source_name", placeholder="customers_csv")
        with c2:
            st.selectbox("Type", options=_FILE_TYPES, key="file_source_type")
        with c3:
            st.selectbox("Encoding", options=_ENCODINGS, key="file_source_encoding")

        st.text_input(
            "Path",
            key="file_source_path",
            placeholder="/absolute/path/to/file.csv (or use the upload below)",
        )
        uploaded = st.file_uploader(
            "Upload file",
            type=["csv", "json", "xml", "txt"],
            key="file_source_uploader",
        )

        c4, c5, c6, c7 = st.columns(4)
        with c4:
            st.text_input("Delimiter", key="file_source_delimiter", max_chars=4)
        with c5:
            st.text_input("Quotechar", key="file_source_quotechar", max_chars=4)
        with c6:
            st.selectbox(
                "Quoting",
                options=list(_QUOTING_OPTIONS.keys()),
                key="file_source_quoting",
            )
        with c7:
            st.text_input("Escapechar", key="file_source_escapechar", max_chars=4)

        c8, c9 = st.columns(2)
        with c8:
            st.checkbox("Doublequote", key="file_source_doublequote")
        with c9:
            st.checkbox("First row is header", key="file_source_has_header")

        if st.button("Load File", key="file_source_load_btn"):
            raw_bytes: bytes | None = None
            size_bytes: int | None = None
            origin: str = ""
            if uploaded is not None:
                raw_bytes = uploaded.getvalue()
                size_bytes = len(raw_bytes)
                origin = f"upload:{uploaded.name}"
            else:
                path = str(st.session_state.get("file_source_path", "")).strip()
                if path and os.path.isfile(path):
                    try:
                        with open(path, "rb") as fh:
                            raw_bytes = fh.read()
                        size_bytes = os.path.getsize(path)
                        origin = f"path:{path}"
                    except OSError as exc:
                        st.error(f"Could not read file: {exc}")
                else:
                    st.error("Provide a valid path or upload a file.")

            if raw_bytes is not None:
                try:
                    df = _load_file_dataframe(
                        raw_bytes=raw_bytes,
                        file_type=str(st.session_state.get("file_source_type", "CSV")),
                        encoding=str(st.session_state.get("file_source_encoding", "utf-8")),
                        delimiter=str(st.session_state.get("file_source_delimiter", ",")),
                        quotechar=str(st.session_state.get("file_source_quotechar", '"')),
                        quoting=_QUOTING_OPTIONS.get(
                            str(st.session_state.get("file_source_quoting", "QUOTE_MINIMAL")),
                            csv.QUOTE_MINIMAL,
                        ),
                        escapechar=str(st.session_state.get("file_source_escapechar", "")) or None,
                        doublequote=bool(st.session_state.get("file_source_doublequote", True)),
                        has_header=bool(st.session_state.get("file_source_has_header", True)),
                    )
                except Exception as exc:  # noqa: BLE001 — surface any parser error to UI
                    st.error(f"Failed to parse file: {exc}")
                    _log_source_event(
                        "file", "load_failed", origin=origin, error=str(exc)
                    )
                else:
                    st.session_state["file_source_df"] = df
                    st.session_state["file_source_size"] = size_bytes
                    st.session_state["file_source_column_map"] = pd.DataFrame(
                        {"source": list(df.columns), "target": list(df.columns)}
                    )
                    _log_source_event(
                        "file",
                        "load",
                        origin=origin,
                        rows=int(df.shape[0]),
                        cols=int(df.shape[1]),
                        bytes=size_bytes,
                    )
                    st.success(
                        f"Loaded {df.shape[0]} rows × {df.shape[1]} columns from {origin}."
                    )

        df = st.session_state.get("file_source_df")
        if isinstance(df, pd.DataFrame):
            m1, m2, m3 = st.columns(3)
            m1.metric("Columns", int(df.shape[1]))
            m2.metric("Rows", int(df.shape[0]))
            m3.metric("Size", _format_size(st.session_state.get("file_source_size")))

            st.markdown("**Column selection**")
            selected_cols = st.multiselect(
                "Columns to keep",
                options=list(df.columns),
                default=list(df.columns),
                key="file_source_selected_columns",
            )

            st.markdown("**Row filter** (pandas `query` syntax) + Limit")
            qcol, lcol = st.columns([3, 1])
            with qcol:
                where_expr = st.text_input(
                    "WHERE (pandas query)",
                    key="file_source_where",
                    placeholder="age > 30 and country == 'DE'",
                )
            with lcol:
                row_limit = st.number_input(
                    "LIMIT",
                    min_value=1,
                    max_value=1_000_000,
                    value=int(st.session_state.get("file_source_row_limit", 100)),
                    step=10,
                    key="file_source_row_limit",
                )

            st.markdown("**Column Mapping** (Source → Target)")
            mapping_df = st.session_state.get(
                "file_source_column_map",
                pd.DataFrame({"source": list(df.columns), "target": list(df.columns)}),
            )
            edited_map = st.data_editor(
                mapping_df,
                num_rows="fixed",
                use_container_width=True,
                disabled=["source"],
                key="file_source_column_map_editor",
            )
            st.session_state["file_source_column_map"] = edited_map

            try:
                preview = df.copy()
                if selected_cols:
                    preview = preview[selected_cols]
                if where_expr.strip():
                    preview = preview.query(where_expr)
                preview = preview.head(int(row_limit)).head(20)

                rename_map = {
                    str(row["source"]): str(row["target"])
                    for _, row in edited_map.iterrows()
                    if str(row["source"]) in preview.columns
                    and str(row["target"]).strip()
                    and str(row["target"]) != str(row["source"])
                }
                if rename_map:
                    preview = preview.rename(columns=rename_map)

                st.markdown("**Preview** (first 20 records after filter/mapping)")
                st.dataframe(preview, use_container_width=True)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Preview failed: {exc}")


# ---------------------------------------------------------------------------
# Section 3 — Database Source
# ---------------------------------------------------------------------------
def _parse_env_database_url() -> dict[str, str]:
    """Parse DATABASE_URL into connection components for read-only defaults."""
    raw = str(os.getenv("DATABASE_URL", "")).strip()
    if not raw:
        return {}
    try:
        parsed = urlparse(raw)
    except Exception:
        return {}
    return {
        "conn_string": raw,
        "host": parsed.hostname or "",
        "port": str(parsed.port or ""),
        "database_name": (parsed.path.lstrip("/") if parsed.path else "") or "",
        "user": parsed.username or "",
        "password": parsed.password or "",
    }


def _pick_default_schema(schemas: list[str]) -> str:
    """Pick a sensible default source schema (SOURCE_SCHEMA env > ecommerce > first user schema)."""
    env_schema = str(os.getenv("SOURCE_SCHEMA", "")).strip()
    if env_schema and env_schema in schemas:
        return env_schema
    if "ecommerce" in schemas:
        return "ecommerce"
    for s in schemas:
        if s and s not in _SYSTEM_SCHEMAS:
            return s
    return "public"


def _fetch_pg_metadata(db: Any, sql: str, params: dict[str, Any]) -> list[dict]:
    """Execute a read-only SQL statement through `db.engine`, return rows-as-dicts."""
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


def _render_technical_metadata(db: Any, schema: str, table: str) -> None:
    """Render PK · FK · Indexes · Constraints · Triggers · Views for `<schema>.<table>`."""
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
        table_fks = [
            fk for fk in fks_raw if isinstance(fk, (tuple, list)) and table in fk
        ]
        if table_fks:
            st.dataframe(pd.DataFrame(table_fks), use_container_width=True, hide_index=True)
        else:
            st.caption("None")

    idx_rows = _fetch_pg_metadata(
        db,
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = :schema AND tablename = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Indexes")
    if idx_rows:
        st.dataframe(pd.DataFrame(idx_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("None")

    cons_rows = _fetch_pg_metadata(
        db,
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints "
        "WHERE table_schema = :schema AND table_name = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Constraints")
    if cons_rows:
        st.dataframe(pd.DataFrame(cons_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("None")

    trig_rows = _fetch_pg_metadata(
        db,
        "SELECT DISTINCT trigger_name, event_manipulation, action_timing "
        "FROM information_schema.triggers "
        "WHERE event_object_schema = :schema AND event_object_table = :table",
        {"schema": schema, "table": table},
    )
    st.caption("Triggers")
    if trig_rows:
        st.dataframe(pd.DataFrame(trig_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("None")

    view_rows = _fetch_pg_metadata(
        db,
        "SELECT table_name FROM information_schema.views WHERE table_schema = :schema",
        {"schema": schema},
    )
    st.caption(f"Views in `{schema}`")
    if view_rows:
        st.dataframe(pd.DataFrame(view_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("None")


def _render_db_source_section(db: Any) -> None:
    with st.expander("Database Source", expanded=True):
        st.caption(
            ".env values are shown as defaults; edits live in this session only. "
            "The actual DB connection used for scans is the one loaded from `.env` at app startup."
        )

        env_defaults = _parse_env_database_url()

        # Seed connection-field defaults from .env on first render only.
        for ss_key, env_key in [
            ("db_source_conn_string", "conn_string"),
            ("conn_host", "host"),
            ("conn_port", "port"),
            ("conn_database_name", "database_name"),
            ("conn_user", "user"),
        ]:
            st.session_state.setdefault(ss_key, env_defaults.get(env_key, ""))

        c1, c2 = st.columns(2)
        with c1:
            st.text_input("Name", key="db_source_name", placeholder="primary_postgres")
        with c2:
            st.text_input("Alias", key="db_source_alias", placeholder="prod-pg")

        st.text_input(
            "Connection String",
            key="db_source_conn_string",
            placeholder="postgresql://user:pass@host:5432/dbname",
        )

        c3, c4 = st.columns([1, 1])
        with c3:
            st.selectbox("Type", options=_DB_TYPES, key="db_source_type")
        with c4:
            st.text_input("User", key="conn_user")

        c5, c6 = st.columns(2)
        with c5:
            st.text_input("Password", type="password", key="conn_password")
        with c6:
            st.text_input("Host", key="conn_host")

        c7, c8 = st.columns([1, 1.5])
        with c7:
            st.text_input("Port", key="conn_port")
        with c8:
            st.text_input("Database Name", key="conn_database_name")

        # ---- Schema selector ----
        try:
            schemas = db.get_all_schemas() or []
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not list schemas: {exc}")
            schemas = []

        if schemas:
            default_schema = _pick_default_schema(schemas)
            current_schema = st.session_state.get("selected_schema") or default_schema
            if current_schema not in schemas:
                current_schema = default_schema
            schema_choice = st.selectbox(
                "Schema",
                options=schemas,
                index=schemas.index(current_schema),
                key="db_source_schema_select",
            )
            st.session_state["selected_schema"] = schema_choice
            st.session_state["source_schema"] = schema_choice
        else:
            schema_choice = st.text_input(
                "Schema",
                key="db_source_schema_text",
                placeholder="public",
            )
            if schema_choice:
                st.session_state["selected_schema"] = schema_choice
                st.session_state["source_schema"] = schema_choice

        # Sync db_config so the spinner block and connection dashboard pick up edits.
        st.session_state["db_config"] = {
            "database_type": st.session_state.get("db_source_type", "PostgreSQL"),
            "data_domain": st.session_state.get("source_domain", _DOMAIN_OPTIONS[0]),
            "connection": {
                "host": st.session_state.get("conn_host", ""),
                "port": st.session_state.get("conn_port", ""),
                "database_name": st.session_state.get("conn_database_name", ""),
                "user": st.session_state.get("conn_user", ""),
                "password": st.session_state.get("conn_password", ""),
            },
        }
        st.session_state["data_source_database_type"] = st.session_state.get(
            "db_source_type", "PostgreSQL"
        )

        if st.session_state.get("db_source_type", "PostgreSQL") != "PostgreSQL":
            st.info(
                "Only PostgreSQL is currently wired to Mappings / Export. Other engines "
                "are configurable here but require a future backend extension to be activated."
            )

        # ---- Test connection ----
        tc1, tc2 = st.columns([1, 4])
        with tc1:
            if st.button("Test Connection", key="db_source_test_btn"):
                try:
                    ok, msg = db.test_connection()
                except Exception as exc:  # noqa: BLE001
                    ok, msg = False, str(exc)
                st.session_state["source_connected"] = bool(ok)
                _log_source_event(
                    "database", "test_connection", ok=bool(ok), msg=str(msg)
                )
                if ok:
                    st.success(msg or "Connection successful.")
                else:
                    st.error(msg or "Connection failed.")
        with tc2:
            connected = bool(st.session_state.get("source_connected"))
            st.markdown(
                f"**Status:** {'🟢 Connected' if connected else '🔴 Not yet tested'}"
            )

        # ---- Table discovery ----
        active_schema = str(st.session_state.get("selected_schema", "") or "")
        if not active_schema:
            return

        try:
            available_tables = db.get_tables_in_schema(active_schema) or []
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not list tables: {exc}")
            available_tables = []

        st.markdown(f"#### Tables in `{active_schema}`")
        previously_selected = [
            t for t in st.session_state.get("db_source_selected_tables", [])
            if t in available_tables
        ]
        default_tables = previously_selected or available_tables
        selected_tables = st.multiselect(
            "Select tables to include",
            options=available_tables,
            default=default_tables,
            key="db_source_selected_tables",
        )

        if selected_tables:
            active_idx = 0
            stored_active = st.session_state.get("db_source_active_table")
            if stored_active in selected_tables:
                active_idx = selected_tables.index(stored_active)
            active_table = st.selectbox(
                "Active table",
                options=selected_tables,
                index=active_idx,
                key="db_source_active_table",
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
            )
            st.text_input(
                "WHERE clause (preview only)",
                key=f"db_source_where__{active_schema}__{active_table}",
                placeholder="email IS NOT NULL AND created_at > '2024-01-01'",
            )

            with st.expander("Technical Metadata", expanded=False):
                _render_technical_metadata(db, active_schema, active_table)

            if st.button("Preview 20 rows", key="db_source_preview_btn"):
                where_clause = str(
                    st.session_state.get(
                        f"db_source_where__{active_schema}__{active_table}", ""
                    )
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
                    _log_source_event(
                        "database",
                        "preview_failed",
                        table=active_table,
                        schema=active_schema,
                        error=str(exc),
                    )
                else:
                    selected_cols = st.session_state.get(
                        f"db_source_cols__{active_schema}__{active_table}",
                        list(getattr(preview_df, "columns", [])),
                    )
                    if selected_cols and isinstance(preview_df, pd.DataFrame):
                        keep = [c for c in selected_cols if c in preview_df.columns]
                        if keep:
                            preview_df = preview_df[keep]
                    st.dataframe(preview_df, use_container_width=True)
                    _log_source_event(
                        "database",
                        "preview",
                        table=active_table,
                        schema=active_schema,
                        rows=int(getattr(preview_df, "shape", (0, 0))[0]),
                        where=where_clause or "",
                    )

        # ---- Initialize / Activate Source ----
        st.markdown("---")
        init_col1, init_col2 = st.columns([1, 3])
        with init_col1:
            if st.button(
                "🚀 Initialize Session",
                type="primary",
                key="db_source_initialize_btn",
                help="Test the source connection, index schema/tables, and wire them into the rest of the workflow.",
            ):
                st.session_state["trigger_session_initialize"] = True
                st.session_state["source_db_connection"] = dict(
                    st.session_state["db_config"]["connection"]
                )
                _log_source_event(
                    "database",
                    "init_triggered",
                    schema=active_schema,
                    tables=list(selected_tables) if selected_tables else [],
                )
        with init_col2:
            st.caption(
                "Initialize Session validates connectivity and indexes metadata. "
                "Plan selection is independent — configure either tab in any order."
            )


# ---------------------------------------------------------------------------
# Section 4 — API Source
# ---------------------------------------------------------------------------
def _detect_response_language(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "json" in ct:
        return "json"
    if "xml" in ct:
        return "xml"
    if "html" in ct:
        return "html"
    if "javascript" in ct:
        return "javascript"
    return "text"


def _render_api_source_section() -> None:
    with st.expander("API Source", expanded=True):
        st.caption(
            "Define an HTTP API as a data source. Provides response monitoring and a "
            "preview panel — not yet plumbed into Mappings / Export."
        )

        c1, c2 = st.columns([3, 1])
        with c1:
            st.text_input(
                "URL",
                key="api_source_url",
                placeholder="https://api.example.com/customers",
            )
        with c2:
            st.selectbox("Method", options=_HTTP_METHODS, key="api_source_method")

        c3, c4 = st.columns(2)
        with c3:
            st.text_input("API Key", type="password", key="api_source_api_key")
        with c4:
            st.text_input("Secret", type="password", key="api_source_secret")

        st.markdown("**Headers**")
        headers_state = st.session_state.setdefault(
            "api_source_headers",
            pd.DataFrame({"key": ["Content-Type"], "value": ["application/json"]}),
        )
        headers_df = st.data_editor(
            headers_state,
            num_rows="dynamic",
            use_container_width=True,
            key="api_source_headers_editor",
        )
        st.session_state["api_source_headers"] = headers_df

        st.text_area(
            "Body (JSON or raw)",
            key="api_source_body",
            height=120,
            placeholder='{"q": "select customers"}',
        )

        if st.button("Send Request", key="api_source_send_btn"):
            try:
                import requests  # type: ignore  # noqa: WPS433 — local import is intentional
            except ImportError:
                st.error(
                    "The `requests` library is not installed. "
                    "Add it to `requirements.txt` or run `pip install requests`."
                )
                _log_source_event("api", "send_failed", error="requests not installed")
            else:
                url = str(st.session_state.get("api_source_url", "")).strip()
                method = str(st.session_state.get("api_source_method", "GET")).upper()
                if not url:
                    st.error("URL is required.")
                else:
                    headers: dict[str, str] = {}
                    try:
                        for _, row in headers_df.iterrows():
                            k = str(row.get("key", "")).strip()
                            v = str(row.get("value", "")).strip()
                            if k:
                                headers[k] = v
                    except Exception:  # noqa: BLE001
                        pass
                    api_key = str(st.session_state.get("api_source_api_key", "")).strip()
                    if api_key and "Authorization" not in headers:
                        headers["Authorization"] = f"Bearer {api_key}"
                    body_raw = str(st.session_state.get("api_source_body", "")).strip()
                    body_payload: Any = None
                    if body_raw:
                        try:
                            body_payload = json.loads(body_raw)
                        except json.JSONDecodeError:
                            body_payload = body_raw

                    started = time.perf_counter()
                    try:
                        resp = requests.request(
                            method,
                            url,
                            headers=headers,
                            json=body_payload if isinstance(body_payload, (dict, list)) else None,
                            data=body_payload if isinstance(body_payload, str) else None,
                            timeout=30,
                        )
                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        body_bytes = resp.content or b""
                        try:
                            body_text = body_bytes.decode(
                                resp.encoding or "utf-8", errors="replace"
                            )
                        except Exception:  # noqa: BLE001
                            body_text = repr(body_bytes[:4096])
                        st.session_state["api_source_last_response"] = {
                            "status": int(resp.status_code),
                            "elapsed_ms": float(elapsed_ms),
                            "size": int(len(body_bytes)),
                            "headers": dict(resp.headers or {}),
                            "body": body_text,
                            "content_type": str(resp.headers.get("Content-Type", "")),
                        }
                        _log_source_event(
                            "api",
                            "request",
                            method=method,
                            url=url,
                            status=int(resp.status_code),
                            elapsed_ms=round(elapsed_ms, 1),
                            size=int(len(body_bytes)),
                        )
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Request failed: {exc}")
                        _log_source_event(
                            "api",
                            "request_failed",
                            method=method,
                            url=url,
                            error=str(exc),
                        )

        last = st.session_state.get("api_source_last_response")
        if last:
            st.markdown("**Response Monitor**")
            m1, m2, m3 = st.columns(3)
            m1.metric("Status", last["status"])
            m2.metric("Response time", f"{last['elapsed_ms']:.0f} ms")
            m3.metric("Size", _format_size(last["size"]))

            st.caption("Response headers")
            headers_dict = last.get("headers", {})
            if headers_dict:
                st.dataframe(
                    pd.DataFrame(
                        {
                            "header": list(headers_dict.keys()),
                            "value": list(headers_dict.values()),
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

            st.caption("Response body")
            lang = _detect_response_language(last.get("content_type", ""))
            body_preview = str(last.get("body", ""))
            if len(body_preview) > 4000:
                body_preview = body_preview[:4000] + "\n... (truncated)"
            st.code(body_preview, language=lang)


# ---------------------------------------------------------------------------
# Section 5 — Source Log
# ---------------------------------------------------------------------------
def _render_source_log_section() -> None:
    with st.expander("Source Log", expanded=False):
        log: list = list(st.session_state.get("source_event_log", []))
        st.caption(
            f"Most recent {len(log)} source events (bounded to {_LOG_MAX_ENTRIES})."
        )

        ctl1, ctl2 = st.columns([1, 1])
        with ctl1:
            if st.button("Clear log", key="source_log_clear_btn"):
                st.session_state["source_event_log"] = []
                st.rerun()
        with ctl2:
            if log:
                st.download_button(
                    "Download log (JSON)",
                    data=json.dumps(log, indent=2),
                    file_name="anonifydb_source_log.json",
                    mime="application/json",
                    key="source_log_download_btn",
                )

        if not log:
            st.info(
                "No source events yet. Loading a file, sending an API request, or "
                "testing the DB connection will populate this log."
            )
            return

        df_log = pd.DataFrame(log).iloc[::-1].reset_index(drop=True)
        st.dataframe(df_log, use_container_width=True, hide_index=True)
