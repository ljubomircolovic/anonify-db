# -*- coding: utf-8 -*-
"""Constants, logging, environment persistence, and session seeding for the Source tab."""

from __future__ import annotations

import csv
import datetime
import json
import logging
import os
from typing import Any, Mapping, MutableMapping

import streamlit as st

from src.logic.app_state import AppState
from src.logic.source_connection import (
    compose_postgresql_source_url,
    connection_test_and_init_disabled,
    seed_db_connection_fields_from_env,
    sync_db_config_from_session,
)
from src.logic.source_constants import DOMAIN_OPTIONS, SOURCE_TYPES

logger = logging.getLogger(__name__)

FILE_TYPES = ["CSV", "JSON", "XML", "TXT"]
ENCODINGS = ["utf-8", "utf-8-sig", "utf-16", "latin-1", "cp1252", "ascii"]
QUOTING_OPTIONS: dict[str, int] = {
    "QUOTE_MINIMAL": csv.QUOTE_MINIMAL,
    "QUOTE_ALL": csv.QUOTE_ALL,
    "QUOTE_NONNUMERIC": csv.QUOTE_NONNUMERIC,
    "QUOTE_NONE": csv.QUOTE_NONE,
}
DB_ENGINES = [
    "PostgreSQL",
    "MySQL",
    "SQL Server",
    "Oracle",
    "DB2",
    "Informix",
    "Sybase",
]
DEFAULT_PORT_BY_ENGINE: dict[str, str] = {
    "PostgreSQL": "5432",
    "MySQL": "3306",
    "SQL Server": "1433",
    "Oracle": "1521",
    "DB2": "50000",
    "Informix": "9088",
    "Sybase": "5000",
}
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
LOG_MAX_ENTRIES = 200

_SOURCE_CONFIRM_ENV = "SOURCE_CONFIRMED"
_SOURCE_DB_URL_ENV = "SOURCE_DB_URL"


def normalize_source_type(raw: str | None) -> str:
    """Return a canonical ``SOURCE_TYPES`` label for ``raw``."""
    if not raw:
        return SOURCE_TYPES[0]
    candidate = str(raw).strip().lower()
    for opt in SOURCE_TYPES:
        if opt.lower() == candidate:
            return opt
    return SOURCE_TYPES[0]


def compose_source_database_url(m: MutableMapping[str, Any]) -> str:
    """Build a PostgreSQL URL from the connection string or discrete fields."""
    return compose_postgresql_source_url(m)


def sync_db_config_dict_from_session(m: MutableMapping[str, Any]) -> None:
    """Mirror discrete connection fields into ``db_config`` for downstream consumers."""
    sync_db_config_from_session(m, default_domain=DOMAIN_OPTIONS[0])


def connection_test_and_init_disabled_for_store(locked: bool, m: MutableMapping[str, Any]) -> bool:
    """Whether Test Connection / Initialize Session should be disabled."""
    return connection_test_and_init_disabled(locked, m)


def get_plan_destination_mode(m: Mapping[str, Any]) -> str:
    """Return ``database`` when an active plan database exists, else ``in_memory``.

    A physical plan database is the persistence target for mappings and structural
    twin DDL; without it, previews operate in session / in-memory only.

    Aligns with the main app's ``is_initialized`` guard: ``active_plan_db_key``
    must belong to the currently selected metadata environment when that label
    is present in session.
    """
    if not bool(m.get("project_initialized", False)):
        return "in_memory"
    key = str(m.get("active_plan_db_key") or "").strip()
    if not key:
        return "in_memory"
    name = str(m.get("active_plan_db_name", "") or "").strip()
    if not name or name.lower() == "none":
        return "in_memory"
    env_label = str(m.get("selected_env_label") or "").strip()
    if env_label and ":" in key and not key.startswith(f"{env_label}:"):
        return "in_memory"
    return "database"


def default_port_for_engine(engine: str | None) -> str:
    """Return the conventional TCP port for ``engine``."""
    if not engine:
        return "5432"
    return DEFAULT_PORT_BY_ENGINE.get(str(engine).strip(), "5432")


def on_db_engine_change() -> None:
    """Streamlit callback: align Port when the engine pick changes."""
    eng = str(st.session_state.get("db_source_type") or "PostgreSQL")
    st.session_state["conn_port"] = default_port_for_engine(eng)


def sync_domain_from_session(m: MutableMapping[str, Any]) -> None:
    """Mirror ``source_domain`` into keys the rest of the app reads."""
    domain = str(m.get("source_domain", DOMAIN_OPTIONS[0]))
    m["data_source_domain_type"] = domain
    m["data_domain"] = domain
    plan_md = m.setdefault("plan_metadata", {})
    if isinstance(plan_md, dict):
        plan_md["data_domain"] = domain


def log_source_event(m: MutableMapping[str, Any], kind: str, event: str, **details: Any) -> None:
    """Append a bounded entry to the in-memory Source event log."""
    log: list = m.setdefault("source_event_log", [])
    log.append(
        {
            "ts": datetime.datetime.now().isoformat(timespec="seconds"),
            "source": kind,
            "event": event,
            "details": json.dumps(details, default=str) if details else "",
        }
    )
    if len(log) > LOG_MAX_ENTRIES:
        del log[: len(log) - LOG_MAX_ENTRIES]


def format_size(num_bytes: int | None) -> str:
    """Human-readable byte size for UI metrics."""
    if num_bytes is None:
        return "—"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024**2:
        return f"{num_bytes / 1024:.1f} KB"
    if num_bytes < 1024**3:
        return f"{num_bytes / (1024**2):.2f} MB"
    return f"{num_bytes / (1024**3):.2f} GB"


def clear_inactive_source_state(m: MutableMapping[str, Any], active_type: str) -> None:
    """Clear ephemeral preview keys for sources other than ``active_type``."""
    inactive_keys: dict[str, list[str]] = {
        "File": [
            "file_source_df",
            "file_source_size",
            "file_source_column_map",
            "file_source_column_map_editor",
        ],
        "API": ["api_source_last_response"],
        "Database": [],
    }
    for source, keys in inactive_keys.items():
        if source == active_type:
            continue
        for k in keys:
            m.pop(k, None)


def init_source_state(app: AppState) -> None:
    """Seed default session keys owned by the Source tab into ``app.mapping``."""
    m = app.mapping
    m.setdefault("source_event_log", [])
    m.setdefault("source_domain", DOMAIN_OPTIONS[0])
    m.setdefault("source_type", normalize_source_type(os.getenv("SOURCE_TYPE")))

    m.setdefault("file_source_name", "")
    m.setdefault("file_source_path", "")
    m.setdefault("file_source_type", FILE_TYPES[0])
    m.setdefault("file_source_encoding", "utf-8")
    m.setdefault("file_source_delimiter", ",")
    m.setdefault("file_source_quotechar", '"')
    m.setdefault("file_source_quoting", "QUOTE_MINIMAL")
    m.setdefault("file_source_escapechar", "")
    m.setdefault("file_source_doublequote", True)
    m.setdefault("file_source_has_header", True)
    m.setdefault("file_source_row_limit", 100)
    m.setdefault("file_source_where", "")
    m.setdefault("file_source_df", None)
    m.setdefault("file_source_size", None)

    m.setdefault("api_source_url", "")
    m.setdefault("api_source_method", "GET")
    m.setdefault("api_source_api_key", "")
    m.setdefault("api_source_secret", "")
    m.setdefault("api_source_body", "")
    m.setdefault("api_source_last_response", None)

    m.setdefault("db_source_type", "PostgreSQL")
    eng = str(m.get("db_source_type", "PostgreSQL"))
    if eng not in DB_ENGINES:
        m["db_source_type"] = "PostgreSQL"
        eng = "PostgreSQL"
    if not str(m.get("conn_port", "")).strip():
        m["conn_port"] = default_port_for_engine(eng)

    seed_db_connection_fields_from_env(m)
    env_schema = str(os.getenv("SOURCE_SCHEMA", "")).strip()
    if env_schema:
        m.setdefault("selected_schema", env_schema)
        m.setdefault("source_schema", env_schema)
    else:
        m.setdefault("selected_schema", "public")
        m.setdefault("source_schema", m.get("selected_schema", "public"))


def inject_source_control_bar_styles() -> None:
    """Inject compact CSS for the Source control bar."""
    st.markdown(
        """
<style>
.adb-src-control-left {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    min-width: 0;
    flex-wrap: nowrap;
}
.adb-src-heading {
    font-weight: 700;
    font-size: 1.02rem;
    color: #e2e8f0;
    letter-spacing: 0.03em;
    white-space: nowrap;
}
@media (prefers-color-scheme: light) {
    .adb-src-heading { color: #0f172a; }
}
.adb-src-pill {
    font-size: 0.68rem;
    font-weight: 700;
    padding: 0.14rem 0.5rem;
    border-radius: 999px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    white-space: nowrap;
}
.adb-src-pill--ok {
    color: #86efac;
    border: 1px solid rgba(34, 197, 94, 0.45);
    background: rgba(34, 197, 94, 0.14);
}
.adb-src-pill--draft {
    color: #fde68a;
    border: 1px solid rgba(245, 158, 11, 0.45);
    background: rgba(245, 158, 11, 0.14);
}
.adb-src-type {
    font-size: 0.78rem;
    color: #94a3b8;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 11rem;
}
@media (prefers-color-scheme: light) {
    .adb-src-type { color: #64748b; }
}
.adb-src-conn-status-line {
    margin: 0.15rem 0 0 0;
    padding: 0;
}
.adb-src-test-ok {
    font-size: 0.82rem;
    font-weight: 600;
    color: #16a34a;
    margin: 0;
    padding-top: 0.4rem;
    line-height: 1.3;
}
.adb-src-test-err {
    font-size: 0.82rem;
    font-weight: 600;
    color: #dc2626;
    margin: 0;
    padding-top: 0.4rem;
    line-height: 1.3;
}
.adb-src-test-muted {
    font-size: 0.75rem;
    color: #64748b;
    padding-top: 0.45rem;
    display: inline-block;
}
@media (prefers-color-scheme: dark) {
    .adb-src-test-muted { color: #94a3b8; }
}
.adb-src-hds-readonly-hint {
    font-size: 0.82rem;
    line-height: 1.35;
    color: #475569;
    margin: 0.1rem 0 0.55rem 0;
    padding: 0.4rem 0.6rem;
    border-radius: 8px;
    background: rgba(71, 85, 105, 0.12);
    border: 1px solid rgba(71, 85, 105, 0.28);
}
@media (prefers-color-scheme: dark) {
    .adb-src-hds-readonly-hint {
        color: #cbd5e1;
        background: rgba(148, 163, 184, 0.12);
        border-color: rgba(148, 163, 184, 0.28);
    }
}
</style>
        """,
        unsafe_allow_html=True,
    )


def persist_source_confirmation_to_env(m: MutableMapping[str, Any]) -> tuple[bool, str]:
    """Write confirmed Source settings to ``.env`` and mirror critical keys into ``os.environ``."""
    try:
        from dotenv import find_dotenv, set_key

        env_path = find_dotenv(usecwd=True)
        if not env_path:
            return False, "No .env file found (use python-dotenv search path)."
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)

    active = normalize_source_type(m.get("source_type"))
    try:
        set_key(env_path, "SOURCE_TYPE", active.upper(), quote_mode="never")
        os.environ["SOURCE_TYPE"] = active.upper()
        if active == "Database":
            url = compose_source_database_url(m).strip()
            if not url:
                return False, "Provide a connection string or host + database name before confirming."
            set_key(env_path, _SOURCE_DB_URL_ENV, url, quote_mode="never")
            os.environ[_SOURCE_DB_URL_ENV] = url
            schema = str(m.get("selected_schema") or m.get("source_schema") or "public").strip() or "public"
            set_key(env_path, "SOURCE_SCHEMA", schema, quote_mode="never")
            os.environ["SOURCE_SCHEMA"] = schema
            plan_md = m.setdefault("plan_metadata", {})
            if isinstance(plan_md, dict):
                plan_md["source_db_connection"] = url
        elif active == "File":
            path = str(m.get("file_source_path", "")).strip()
            set_key(env_path, "SOURCE_FILE_PATH", path, quote_mode="never")
            os.environ["SOURCE_FILE_PATH"] = path
        else:
            api_url = str(m.get("api_source_url", "")).strip()
            set_key(env_path, "SOURCE_API_URL", api_url, quote_mode="never")
            os.environ["SOURCE_API_URL"] = api_url

        set_key(env_path, _SOURCE_CONFIRM_ENV, "1", quote_mode="never")
        os.environ[_SOURCE_CONFIRM_ENV] = "1"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    return True, ""


def clear_source_confirmation_env() -> None:
    """Mark Source as editable again in ``.env`` for the next cold start."""
    try:
        from dotenv import find_dotenv, set_key

        env_path = find_dotenv(usecwd=True)
        if env_path:
            set_key(env_path, _SOURCE_CONFIRM_ENV, "0", quote_mode="never")
        os.environ[_SOURCE_CONFIRM_ENV] = "0"
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not clear %s in .env: %s", _SOURCE_CONFIRM_ENV, exc)


def persist_source_type_env(source_type: str) -> None:
    """Persist ``SOURCE_TYPE`` to ``.env`` (silent on failure)."""
    try:
        from dotenv import find_dotenv, set_key

        env_path = find_dotenv(usecwd=True)
        if not env_path:
            return
        set_key(env_path, "SOURCE_TYPE", source_type.upper(), quote_mode="never")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not persist SOURCE_TYPE to .env: %s", exc)
