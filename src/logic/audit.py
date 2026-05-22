# -*- coding: utf-8 -*-
"""SQL execution audit logging to the metadata database."""

from __future__ import annotations

import logging
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

SQL_AUDIT_TABLE = "metadata.sql_audit_logs"
_SQL_AUDIT_BOOTSTRAP = (
    "CREATE SCHEMA IF NOT EXISTS metadata",
    """
    CREATE TABLE IF NOT EXISTS metadata.sql_audit_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        username VARCHAR(255) NOT NULL DEFAULT 'anonymous_user',
        session_id VARCHAR(255) NOT NULL,
        query_type VARCHAR(32) NOT NULL,
        target_database VARCHAR(255) NOT NULL,
        sql_text TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sql_audit_logs_session_ts
        ON metadata.sql_audit_logs (session_id, timestamp DESC)
    """,
)

_VALID_QUERY_TYPES = frozenset({"ORIGINAL", "ANONYMIZED"})


def sanitize_target_database(target_db: str) -> str:
    """
    Normalize target labels before persist/display.

    Collapses accidental ``name:name`` duplicates; preserves ``in_memory:…`` prefixes.
    """
    value = str(target_db or "").strip()
    if not value:
        return "unknown-db"
    if ":" not in value:
        return value

    parts = [p.strip() for p in value.split(":") if p.strip()]
    if len(parts) == 2 and parts[0] == parts[1]:
        return parts[0]
    if len(parts) == 2 and parts[0] == "in_memory":
        return value
    return value


def _resolve_metadata_audit_engine(db: Any):
    """
    Engine for ``metadata.sql_audit_logs``.

    Uses the canonical metadata brain connection (``metadata_engine`` /
    ``source_engine``), not ``target_engine``, which switches to the active plan DB.
    """
    return (
        getattr(db, "metadata_engine", None)
        or getattr(db, "source_engine", None)
        or getattr(db, "engine", None)
    )


def _metadata_database_label(db: Any) -> str:
    url = str(
        getattr(db, "metadata_db_url", None)
        or getattr(db, "source_db_url", None)
        or ""
    ).strip()
    if url:
        try:
            path = urlparse(url).path.lstrip("/")
            if path:
                return path
        except Exception:  # noqa: BLE001
            pass
    return "unknown-db"


def _ensure_sql_audit_table(conn) -> None:
    for stmt in _SQL_AUDIT_BOOTSTRAP:
        conn.execute(text(stmt))


def _persist_audit_row(engine, insert_sql, params: dict) -> None:
    """Insert one audit row with an explicit commit (never raises)."""
    with engine.connect() as conn:
        try:
            _ensure_sql_audit_table(conn)
            conn.execute(insert_sql, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def get_streamlit_session_id() -> str:
    """Stable Streamlit browser session id for audit filtering."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        ctx = get_script_run_ctx()
        if ctx is not None and getattr(ctx, "session_id", None):
            return str(ctx.session_id)
    except Exception:  # noqa: BLE001
        pass
    return "unknown_session"


def get_audit_username(session_state: Optional[Mapping[str, Any]] = None) -> str:
    """Resolve username from Streamlit user context or session login."""
    try:
        import streamlit as st

        user = getattr(st, "user", None)
        if user is not None:
            name = str(getattr(user, "username", None) or getattr(user, "email", None) or "").strip()
            if name:
                return name
    except Exception:  # noqa: BLE001
        pass

    if session_state is not None:
        for key in ("user_name", "username", "audit_username"):
            val = str(session_state.get(key, "") or "").strip()
            if val:
                return val

    return "anonymous_user"


def resolve_target_database_name(
    db: Any,
    session_state: Optional[Mapping[str, Any]] = None,
) -> str:
    """Source / active connection database name from session or engine URL."""
    state = session_state or {}
    explicit = str(state.get("conn_database_name", "") or "").strip()
    if explicit:
        return explicit

    url = str(getattr(db, "source_db_url", "") or getattr(db, "db_url", "") or "").strip()
    if url:
        try:
            path = urlparse(url).path.lstrip("/")
            if path:
                return path
        except Exception:  # noqa: BLE001
            pass

    plan_name = str(state.get("active_plan_db_name", "") or "").strip()
    if plan_name and plan_name not in ("", "None"):
        return plan_name

    return "unknown-db"


def resolve_anonymized_target_database(
    db: Any,
    session_state: Mapping[str, Any],
    *,
    destination_mode: str,
    source_schema: str,
) -> str:
    """Label for anonymized / in-memory mirror execution target (single name, no duplication)."""
    from src.logic import query_mirror

    schema = query_mirror.resolve_anonymized_target_schema(
        session_state,
        destination_mode,
        source_schema,
        db,
    ).strip()
    if str(destination_mode or "").strip().lower() != "database":
        return sanitize_target_database(f"in_memory:{schema}" if schema else "in_memory:__mirror_preview__")

    db_name = query_mirror.resolve_export_target_identifier(session_state, db)
    if not db_name:
        db_name = resolve_target_database_name(db, session_state)

    if schema and db_name and schema == db_name:
        return sanitize_target_database(schema)
    if schema:
        return sanitize_target_database(schema)
    return sanitize_target_database(db_name or "unknown-db")


def log_sql_execution(
    sql_text: str,
    query_type: str,
    target_db: str,
    *,
    db: Any,
    username: Optional[str] = None,
    session_id: Optional[str] = None,
    session_state: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Persist one executed SQL audit row. Failures are logged and never raised.
    """
    text_sql = str(sql_text or "").strip()
    if not text_sql or db is None:
        return

    qtype = str(query_type or "").strip().upper()
    if qtype not in _VALID_QUERY_TYPES:
        logger.warning("log_sql_execution: invalid query_type=%r", query_type)
        return

    state = session_state if session_state is not None else {}
    user = str(username or get_audit_username(state)).strip() or "anonymous_user"
    sid = str(session_id or get_streamlit_session_id()).strip() or "unknown_session"
    target = sanitize_target_database(str(target_db or "unknown-db"))

    engine = _resolve_metadata_audit_engine(db)
    if engine is None:
        logger.debug("log_sql_execution: no metadata audit engine on db manager")
        return

    insert_sql = text(
        f"""
        INSERT INTO {SQL_AUDIT_TABLE}
            (username, session_id, query_type, target_database, sql_text)
        VALUES
            (:username, :session_id, :query_type, :target_database, :sql_text)
        """
    )
    params = {
        "username": user[:255],
        "session_id": sid[:255],
        "query_type": qtype[:32],
        "target_database": target[:255],
        "sql_text": text_sql,
    }

    meta_db = _metadata_database_label(db)
    try:
        _persist_audit_row(engine, insert_sql, params)
        logger.debug(
            "log_sql_execution: committed %s row to %s.%s",
            qtype,
            meta_db,
            SQL_AUDIT_TABLE,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "log_sql_execution failed (non-fatal) on %s.%s: %s",
            meta_db,
            SQL_AUDIT_TABLE,
            exc,
        )


def strip_mirror_sql_header(display_sql: str) -> str:
    """Drop leading ``--`` comment lines from mirrored SQL before audit persistence."""
    lines: list[str] = []
    for line in str(display_sql or "").splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines).strip()
    return body or str(display_sql or "").strip()


def fetch_session_sql_audit_logs(
    db: Any,
    *,
    session_id: Optional[str] = None,
    limit: int = 50,
) -> pd.DataFrame:
    """Recent audit rows for the current Streamlit session, newest first."""
    sid = str(session_id or get_streamlit_session_id()).strip()
    engine = _resolve_metadata_audit_engine(db)
    if engine is None or not sid:
        return pd.DataFrame(
            columns=["Time", "User", "Type", "Target DB", "SQL Query"]
        )

    query = text(
        f"""
        SELECT
            timestamp AS time,
            username AS user,
            query_type AS type,
            target_database AS target_db,
            sql_text AS sql_query
        FROM {SQL_AUDIT_TABLE}
        WHERE session_id = :session_id
        ORDER BY timestamp DESC
        LIMIT :lim
        """
    )

    try:
        with engine.connect() as conn:
            _ensure_sql_audit_table(conn)
            df = pd.read_sql(query, conn, params={"session_id": sid, "lim": int(limit)})
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "fetch_session_sql_audit_logs failed on %s.%s: %s",
            _metadata_database_label(db),
            SQL_AUDIT_TABLE,
            exc,
        )
        return pd.DataFrame(
            columns=["Time", "User", "Type", "Target DB", "SQL Query"]
        )

    if df.empty:
        return pd.DataFrame(
            columns=["Time", "User", "Type", "Target DB", "SQL Query"]
        )

    df = df.rename(
        columns={
            "time": "Time",
            "user": "User",
            "type": "Type",
            "target_db": "Target DB",
            "sql_query": "SQL Query",
        }
    )
    if "Time" in df.columns:
        df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce").dt.strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        )
    if "Target DB" in df.columns:
        df["Target DB"] = df["Target DB"].map(
            lambda v: sanitize_target_database(str(v))
        )
    return df[["Time", "User", "Type", "Target DB", "SQL Query"]]
