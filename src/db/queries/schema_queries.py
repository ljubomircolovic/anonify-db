# -*- coding: utf-8 -*-
"""Schema and table discovery against a PostgreSQL source engine."""

from __future__ import annotations

import logging
import re
from typing import Any, Mapping

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_FORBIDDEN_SCHEMA_FRAGMENTS: frozenset[str] = frozenset(
    {"information_schema", "pg_catalog", "metadata", "_anon_metadata"}
)


def quote_sql_identifier(identifier: str) -> str:
    """Return a double-quoted SQL identifier with internal quotes escaped."""
    safe = str(identifier).replace('"', '""')
    return f'"{safe}"'


def fetch_all_schemas(engine: Engine) -> list[str]:
    """Return user-facing schema names from ``engine``, excluding system schemas."""
    inspector = inspect(engine)
    all_schemas = inspector.get_schema_names()
    return [
        s
        for s in all_schemas
        if s not in _FORBIDDEN_SCHEMA_FRAGMENTS and "anon" not in s.lower()
    ]


def fetch_tables_in_schema(engine: Engine, schema: str = "public") -> list[str]:
    """Return base table names in ``schema`` excluding internal ``anon`` tables."""
    inspector = inspect(engine)
    all_tables = inspector.get_table_names(schema=schema)
    return [t for t in all_tables if "anon" not in t.lower()]


def fetch_column_names(engine: Engine, table_name: str, schema_name: str = "public") -> list[str]:
    """Return column names for ``schema_name.table_name`` via SQLAlchemy inspect."""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name, schema=schema_name)
        return [str(col["name"]) for col in columns]
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "❌ [schema_queries] Error fetching columns for %s.%s: %s",
            schema_name,
            table_name,
            exc,
        )
        return []


def read_table_to_dataframe(
    engine: Engine,
    *,
    table_name: str,
    schema_name: str = "public",
    where: str | None = None,
    limit: int = 100,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Run ``SELECT *`` (optional ``WHERE`` / ``LIMIT``) and return a DataFrame."""
    quoted_schema = quote_sql_identifier(schema_name)
    quoted_table = quote_sql_identifier(table_name)
    query_str = f"SELECT * FROM {quoted_schema}.{quoted_table}"

    if where and str(where).strip():
        clean_filter = re.sub(r"(?i)^where\s+", "", str(where).strip())
        query_str += f" WHERE {clean_filter}"

    if limit:
        query_str += f" LIMIT {int(limit)}"

    stmt = text(query_str)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {quoted_schema}, public;"))
            result = conn.execute(stmt, params or {})
            logger.info("✅ [schema_queries] Read preview from %s.%s", schema_name, table_name)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "❌ [schema_queries] Error reading table %s.%s: %s",
            schema_name,
            table_name,
            exc,
        )
        return pd.DataFrame()


def fetch_tables_excluding_internal(engine: Engine, schema_name: str = "public") -> list[str]:
    """List base tables in ``schema_name`` excluding known internal Anonify tables."""
    excluded_tables = ("anon_forced_mappings", "anonymization_logs", "audit_trail")
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        AND table_type = 'BASE TABLE'
        AND table_name NOT IN {excluded_tables}
        ORDER BY table_name;
    """
    try:
        df = pd.read_sql(query, engine)
        return df["table_name"].tolist()
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [schema_queries] Error listing tables: %s", exc)
        return []


def fetch_source_schema_catalog(engine: Engine, schema_name: str = "public") -> list[dict[str, str]]:
    """Return table names with PostgreSQL ``COMMENT ON`` text for ``schema_name``."""
    query = text(
        """
            SELECT
                relname AS table_name,
                obj_description(oid) AS description
            FROM pg_class
            WHERE relkind = 'r'
              AND relnamespace = (
                  SELECT oid
                  FROM pg_namespace
                  WHERE nspname = :schema_name
              )
            ORDER BY relname;
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"schema_name": schema_name})
            rows: list[dict[str, str]] = []
            for row in result:
                rows.append(
                    {
                        "table_name": row[0],
                        "description": row[1] if row[1] is not None else "No description available",
                    }
                )
            return rows
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [schema_queries] Error loading source schema catalog: %s", exc)
        return []
