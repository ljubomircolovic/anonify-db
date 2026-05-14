# -*- coding: utf-8 -*-
"""Schema/table DDL mirroring, structural sync, and FK maintenance helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from src.db.queries import data_discovery as dd_queries
from src.db.services.ddl_extensions import DdlExtensions

if TYPE_CHECKING:
    from src.database.db_manager import DBManager

logger = logging.getLogger(__name__)


class DdlManager:
    """Target DDL alignment, FK lifecycle, and anonymization skeleton prep."""

    __slots__ = ("_m", "_ext")

    def __init__(self, manager: "DBManager") -> None:
        self._m = manager
        self._ext = DdlExtensions(self)

    def ensure_target_table_mirror(
        self,
        active_conn: Any,
        source_schema: str,
        target_schema: str,
        table_name: str,
    ) -> None:
        """Ensures target table exists and mirrors source DDL types exactly."""
        active_conn.execute(
            text(f"SET search_path TO {self._m.quote_identifier(target_schema)}, public;")
        )
        exists_sql = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :target_schema
                  AND table_name = :table_name
            )
        """)
        exists = active_conn.execute(
            exists_sql,
            {"target_schema": target_schema, "table_name": table_name},
        ).scalar()
        quoted_target_schema = self._m.quote_identifier(target_schema)
        quoted_table_name = self._m.quote_identifier(table_name)
        source_rows = self._m._get_source_type_signatures(source_schema, table_name)
        source_types = {row[0]: row[1] for row in source_rows}
        created_now = False

        if not exists:
            active_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_target_schema}"))
            columns_sql = ", ".join(
                f"{self._m.quote_identifier(col_name)} {col_type}"
                for col_name, col_type in source_types.items()
            )
            active_conn.execute(
                text(f"CREATE TABLE {quoted_target_schema}.{quoted_table_name} ({columns_sql})")
            )
            logger.info(
                "✅ [DB_MANAGER] Created target table %s.%s from source schema %s",
                target_schema,
                table_name,
                source_schema,
            )
            created_now = True

        if not created_now:
            type_signature_sql = text("""
                SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS column_type
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = :schema_name
                  AND c.relname = :table_name
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum
            """)
            target_rows = active_conn.execute(
                type_signature_sql,
                {"schema_name": target_schema, "table_name": table_name},
            ).fetchall()
            target_types = {row[0]: row[1] for row in target_rows}

            for column_name, source_type in source_types.items():
                if column_name in target_types and target_types[column_name] != source_type:
                    quoted_col = self._m.quote_identifier(column_name)
                    active_conn.execute(
                        text(
                            f"ALTER TABLE {quoted_target_schema}.{quoted_table_name} "
                            f"ALTER COLUMN {quoted_col} TYPE {source_type} "
                            f"USING {quoted_col}::{source_type}"
                        )
                    )
                    logger.info(
                        "✅ [DB_MANAGER] Aligned type %s.%s.%s -> %s",
                        target_schema,
                        table_name,
                        column_name,
                        source_type,
                    )

        self.sync_pk_unique_constraints(active_conn, source_schema, target_schema, table_name)
        created_indexes = self.sync_non_constraint_indexes(
            active_conn, source_schema, target_schema, table_name
        )
        self._m._structural_sync_counters["indexes_recreated"] += int(created_indexes or 0)

    @staticmethod
    def rewrite_schema_references(sql_def: str, source_schema: str, target_schema: str) -> str:
        source_schema_q = f'"{source_schema}"'
        target_schema_q = f'"{target_schema}"'
        rewritten = str(sql_def)
        rewritten = rewritten.replace(f"{source_schema_q}.", f"{target_schema_q}.")
        rewritten = rewritten.replace(f"{source_schema}.", f"{target_schema}.")
        return rewritten

    def constraint_exists(
        self,
        active_conn: Any,
        schema_name: str,
        table_name: str,
        constraint_name: str,
    ) -> bool:
        exists_sql = text("""
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_schema = :schema_name
              AND table_name = :table_name
              AND constraint_name = :constraint_name
            LIMIT 1
        """)
        row = active_conn.execute(
            exists_sql,
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "constraint_name": constraint_name,
            },
        ).fetchone()
        return row is not None

    def index_exists(self, active_conn: Any, schema_name: str, index_name: str) -> bool:
        exists_sql = text("SELECT to_regclass(:idx_name)")
        reg_name = f'"{schema_name}"."{index_name}"'
        row = active_conn.execute(exists_sql, {"idx_name": reg_name}).fetchone()
        return bool(row and row[0])

    def get_indexed_columns(self, schema_name: str, table_name: str) -> set[str]:
        """Return column names participating in indexes for a table."""
        return dd_queries.fetch_indexed_column_names(
            self._m.source_engine, schema_name, table_name
        )

    def log_index_distribution_preflight(self, source_schema: str, ordered_tables: list[str]) -> None:
        """Log index-awareness preflight before migration execution."""
        if not ordered_tables:
            return
        idx_sql = text("""
            SELECT tablename, COUNT(*)::int AS idx_count
            FROM pg_indexes
            WHERE schemaname = :schema_name
              AND tablename = ANY(:tables)
            GROUP BY tablename
        """)
        try:
            with self._m.source_engine.connect() as conn:
                rows = conn.execute(
                    idx_sql,
                    {"schema_name": source_schema, "tables": list(ordered_tables)},
                ).fetchall()
            idx_map = {str(r[0]): int(r[1]) for r in rows}
            total = sum(idx_map.values())
            if total > 0:
                logger.info(
                    "Indexing patterns detected. Ensuring data distribution maintains index efficiency."
                )
                logger.info(
                    "✅ [DB_MANAGER] Index preflight: %s indexed structures across %s table(s).",
                    total,
                    len(idx_map),
                )
                for table_name in ordered_tables:
                    indexed_cols = self.get_indexed_columns(source_schema, table_name)
                    if not indexed_cols:
                        continue
                    saved_plan = self._m.get_saved_plan(source_schema, table_name) or {}
                    plan_rows = saved_plan.get("plan", []) if isinstance(saved_plan, dict) else []
                    for row in plan_rows:
                        if not isinstance(row, dict):
                            continue
                        col_name = str(row.get("column", "")).strip()
                        strategy = str(row.get("strategy", "keep")).lower().strip()
                        if not col_name or col_name not in indexed_cols:
                            continue
                        if strategy != "keep":
                            logger.warning(
                                "⚠️ Column %s is indexed. Ensuring anonymization preserves data distribution.",
                                col_name,
                            )
        except Exception as e:
            logger.warning("⚠️ [DB_MANAGER] Index preflight check failed: %s", e)

    def reset_structural_sync_counters(self) -> None:
        self._m._structural_sync_counters = {"indexes_recreated": 0, "fks_recreated": 0}

    def sync_pk_unique_constraints(
        self,
        active_conn: Any,
        source_schema: str,
        target_schema: str,
        table_name: str,
    ) -> None:
        source_constraints_sql = text("""
            SELECT con.conname, pg_get_constraintdef(con.oid) AS condef
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = :schema_name
              AND rel.relname = :table_name
              AND con.contype IN ('p', 'u')
            ORDER BY con.contype DESC, con.conname ASC
        """)
        with self._m.source_engine.connect() as source_conn:
            source_rows = source_conn.execute(
                source_constraints_sql,
                {"schema_name": source_schema, "table_name": table_name},
            ).fetchall()

        for row in source_rows:
            con_name, con_def = row[0], row[1]
            if self.constraint_exists(active_conn, target_schema, table_name, con_name):
                continue
            rewritten_def = self.rewrite_schema_references(con_def, source_schema, target_schema)
            alter_sql = text(
                f"ALTER TABLE {self._m.quote_identifier(target_schema)}.{self._m.quote_identifier(table_name)} "
                f"ADD CONSTRAINT {self._m.quote_identifier(con_name)} {rewritten_def}"
            )
            active_conn.execute(alter_sql)

    def sync_non_constraint_indexes(
        self,
        active_conn: Any,
        source_schema: str,
        target_schema: str,
        table_name: str,
    ) -> int:
        source_indexes_sql = text("""
            SELECT idx.indexname, idx.indexdef
            FROM pg_indexes idx
            LEFT JOIN pg_class cls ON cls.relname = idx.indexname
            LEFT JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace AND nsp.nspname = idx.schemaname
            LEFT JOIN pg_index pi ON pi.indexrelid = cls.oid
            LEFT JOIN pg_constraint con ON con.conindid = pi.indexrelid
            WHERE idx.schemaname = :schema_name
              AND idx.tablename = :table_name
              AND con.oid IS NULL
            ORDER BY idx.indexname ASC
        """)
        with self._m.source_engine.connect() as source_conn:
            source_rows = source_conn.execute(
                source_indexes_sql,
                {"schema_name": source_schema, "table_name": table_name},
            ).fetchall()

        created_indexes = 0
        for row in source_rows:
            index_name, index_def = row[0], row[1]
            if self.index_exists(active_conn, target_schema, index_name):
                continue
            rewritten_def = self.rewrite_schema_references(index_def, source_schema, target_schema)
            active_conn.execute(text(rewritten_def))
            created_indexes += 1
        return created_indexes

    def sync_foreign_keys_for_tables(self, source_schema, target_schema, ordered_tables):
        return self._ext.sync_foreign_keys_for_tables(source_schema, target_schema, ordered_tables)

    def check_fk_integrity(self, source_schema, target_schema, ordered_tables):
        return self._ext.check_fk_integrity(source_schema, target_schema, ordered_tables)

    def create_anonymized_table(self, source_schema, table_name, target_db, target_schema="anon"):
        return self._ext.create_anonymized_table(source_schema, table_name, target_db, target_schema)

    def prepare_anonymization_target(self, source_schema, target_schema, ordered_tables):
        return self._ext.prepare_anonymization_target(source_schema, target_schema, ordered_tables)

    def drop_fks_from_table(self, conn, schema, table):
        return self._ext.drop_fks_from_table(conn, schema, table)

    def restore_foreign_keys(self, source_schema, target_schema, tables):
        return self._ext.restore_foreign_keys(source_schema, target_schema, tables)

    def drop_target_schema(self, target_schema):
        return self._ext.drop_target_schema(target_schema)

    def align_db_types(self, target_schema, table_name, plan, conn=None):
        return self._ext.align_db_types(target_schema, table_name, plan, conn)

    def drop_all_fks_for_table(self, conn, schema, table):
        return self._ext.drop_all_fks_for_table(conn, schema, table)

    def rehook_foreign_keys(self, conn, commands):
        return self._ext.rehook_foreign_keys(conn, commands)

    def truncate_anon_tables(self, target_schema, ordered_tables, clear_mode="truncate_cascade"):
        return self._ext.truncate_anon_tables(target_schema, ordered_tables, clear_mode)

    def set_fk_constraints_temporarily_disabled(self, target_schema, ordered_tables, disabled=True):
        return self._ext.set_fk_constraints_temporarily_disabled(target_schema, ordered_tables, disabled)

