# -*- coding: utf-8 -*-
"""Foreign keys, execution order, and richer column metadata for PostgreSQL."""

from __future__ import annotations

import logging
from typing import Any, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.db.queries.schema_queries import quote_sql_identifier

logger = logging.getLogger(__name__)


def fetch_foreign_key_relations_postgres(engine: Engine, schema_name: str = "public") -> pd.DataFrame:
    """Return FK rows as a DataFrame (table/column → foreign table/column)."""
    query = text(
        """
            SELECT
                tc.table_name AS table_name,
                kcu.column_name AS column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = :schema;
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"schema": schema_name})
            df_rel = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(
                "✅ [data_discovery] Dependency engine found %s relations in schema %s",
                len(df_rel),
                schema_name,
            )
            return df_rel
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [data_discovery] Error fetching Postgres relations: %s", exc)
        return pd.DataFrame()


def compute_execution_order(selected_tables: Sequence[str], relations: pd.DataFrame) -> list[str]:
    """Topologically order ``selected_tables`` using FK edges from ``relations``."""
    if not selected_tables:
        return []
    dependencies: dict[str, set[str]] = {str(t): set() for t in selected_tables}
    if relations.empty:
        return list(selected_tables)

    for _, row in relations.iterrows():
        tab = row["table_name"]
        parent = row["foreign_table_name"]
        if tab in dependencies and parent in selected_tables and tab != parent:
            dependencies[str(tab)].add(str(parent))

    ordered_tables: list[str] = []
    deps_mut = {k: set(v) for k, v in dependencies.items()}
    while deps_mut:
        ready_nodes = [t for t, deps in deps_mut.items() if not deps]
        if not ready_nodes:
            ordered_tables.extend(list(deps_mut.keys()))
            break
        for node in ready_nodes:
            ordered_tables.append(node)
            del deps_mut[node]
            for t in deps_mut:
                deps_mut[t].discard(node)
    return ordered_tables


def fetch_all_foreign_keys_tuples(engine: Engine, schema_name: str) -> list[tuple[str, str, str, str]]:
    """Return ``(source_table, source_column, target_table, target_column)`` tuples."""
    query = text(
        """
            SELECT
                kcu.table_name as source_table,
                kcu.column_name as source_column,
                rel_kcu.table_name as target_table,
                rel_kcu.column_name as target_column
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
              ON tco.constraint_name = kcu.constraint_name
            JOIN information_schema.referential_constraints rco
              ON tco.constraint_name = rco.constraint_name
            JOIN information_schema.key_column_usage rel_kcu
              ON rco.unique_constraint_name = rel_kcu.constraint_name
            WHERE tco.constraint_type = 'FOREIGN KEY'
              AND tco.table_schema = :s
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"s": schema_name})
            return [(row[0], row[1], row[2], row[3]) for row in result]
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [data_discovery] Error fetching foreign keys: %s", exc)
        return []


def fetch_primary_key_column_names(engine: Engine, schema: str, table: str) -> list[str]:
    """Return primary-key column names for ``schema.table``."""
    query = text(
        """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = :schema
            AND tc.table_name = :table;
        """
    )
    try:
        df = pd.read_sql(query, engine, params={"schema": schema, "table": table})
        logger.info("✅ [data_discovery] Batch %s: found %s PK columns", table, len(df))
        return df["column_name"].tolist()
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [data_discovery] Error fetching PKs for %s: %s", table, exc)
        return []


def table_exists(engine: Engine, table_name: str, schema_name: str) -> bool:
    """Return whether ``schema_name.table_name`` exists (case-insensitive match)."""
    query = text(
        """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :s
                AND table_name = :t
            )
        """
    )
    with engine.connect() as conn:
        return bool(
            conn.execute(
                query, {"s": schema_name.lower(), "t": table_name.lower()}
            ).scalar()
        )


def fetch_row_count(engine: Engine, table_name: str, schema_name: str) -> Any:
    """Return ``COUNT(*)`` for ``schema_name.table_name``."""
    query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    with engine.connect() as conn:
        return conn.execute(query).scalar()


def fetch_column_details(engine: Engine, table_name: str, schema_name: str) -> dict[str, dict[str, str]]:
    """Return column metadata keyed by column name."""
    query = text(
        """
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t
            ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"s": schema_name, "t": table_name})
        return {
            row[0]: {"type": row[1], "nullable": row[2]}
            for row in result
        }


def fetch_indexed_column_names(engine: Engine, schema_name: str, table_name: str) -> set[str]:
    """Return column names participating in valid indexes on the table."""
    idx_sql = text(
        """
            SELECT DISTINCT a.attname AS column_name
            FROM pg_class t
            JOIN pg_namespace ns ON ns.oid = t.relnamespace
            JOIN pg_index i ON i.indrelid = t.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
            WHERE ns.nspname = :schema_name
              AND t.relname = :table_name
              AND i.indisvalid = true
              AND a.attnum > 0
              AND NOT a.attisdropped
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                idx_sql, {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
        return {str(r[0]) for r in rows}
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "⚠️ [data_discovery] Failed to read indexed columns for %s.%s: %s",
            schema_name,
            table_name,
            exc,
        )
        return set()


def fetch_table_sample_as_str_records(
    engine: Engine, schema: str, table: str, limit: int = 5
) -> list[dict[str, str]]:
    """Load up to ``limit`` rows as stringified dict records (AI-safe JSON)."""
    quoted_schema = quote_sql_identifier(schema)
    quoted_table = quote_sql_identifier(table)
    query = f"SELECT * FROM {quoted_schema}.{quoted_table} LIMIT {int(limit)}"
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {quoted_schema}, public;"))
            df = pd.read_sql(text(query), conn)
            logger.info("✅ [data_discovery] Sample loaded from %s.%s", schema, table)
            return df.astype(str).to_dict(orient="records")
    except Exception as exc:  # noqa: BLE001
        logger.error("❌ [data_discovery] Error fetching sample for %s.%s: %s", schema, table, exc)
        return []
