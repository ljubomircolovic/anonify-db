# -*- coding: utf-8 -*-
"""DDL operations that depend on structural sync (FK lifecycle, CTAS, batch prep)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

if TYPE_CHECKING:
    from src.db.services.ddl_manager import DdlManager

logger = logging.getLogger(__name__)


class DdlExtensions:
    """FK sync, integrity checks, anonymized table creation, truncate/disable triggers."""

    __slots__ = ("_ddl",)

    def __init__(self, ddl: "DdlManager") -> None:
        self._ddl = ddl

    @property
    def _m(self):
        return self._ddl._m

    def sync_foreign_keys_for_tables(
        self,
        source_schema: str,
        target_schema: str,
        ordered_tables: list[str],
    ) -> None:
        if not ordered_tables:
            return
        fk_sql = text("""
            SELECT con.conname, rel.relname AS table_name, pg_get_constraintdef(con.oid) AS condef
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = :schema_name
              AND rel.relname = :table_name
              AND con.contype = 'f'
            ORDER BY con.conname ASC
        """)
        recreated_fks = 0
        with self._m.target_engine.connect() as target_conn:
            with target_conn.begin():
                target_conn.execute(
                    text(f"SET search_path TO {self._m.quote_identifier(target_schema)}, public;")
                )
                for table_name in ordered_tables:
                    with self._m.source_engine.connect() as source_conn:
                        fk_rows = source_conn.execute(
                            fk_sql,
                            {"schema_name": source_schema, "table_name": table_name},
                        ).fetchall()
                    for fk_row in fk_rows:
                        con_name, child_table, con_def = fk_row[0], fk_row[1], fk_row[2]
                        if self._ddl.constraint_exists(target_conn, target_schema, child_table, con_name):
                            continue
                        rewritten_def = self._ddl.rewrite_schema_references(con_def, source_schema, target_schema)
                        try:
                            target_conn.execute(
                                text(
                                    f"ALTER TABLE {self._m.quote_identifier(target_schema)}.{self._m.quote_identifier(child_table)} "
                                    f"ADD CONSTRAINT {self._m.quote_identifier(con_name)} {rewritten_def}"
                                )
                            )
                            recreated_fks += 1
                        except Exception as e:
                            logger.warning(
                                "⚠️ [DB_MANAGER] FK sync warning for %s.%s: %s",
                                child_table,
                                con_name,
                                e,
                            )
        self._m._structural_sync_counters["fks_recreated"] += int(recreated_fks or 0)
        logger.info(
            "✅ [DB_MANAGER] Structural twin sync finished | Indexes recreated: %s | FKs recreated: %s",
            self._m._structural_sync_counters.get("indexes_recreated", 0),
            self._m._structural_sync_counters.get("fks_recreated", 0),
        )

    def check_fk_integrity(
        self,
        source_schema: str,
        target_schema: str,
        ordered_tables: list[str],
    ) -> list[dict[str, Any]]:
        """Return orphan FK violations in the target schema."""
        if not ordered_tables:
            return []
        fk_meta_sql = text("""
            SELECT
                tc.table_name AS child_table,
                kcu.column_name AS child_column,
                ccu.table_name AS parent_table,
                ccu.column_name AS parent_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = :schema_name
              AND tc.table_name = ANY(:tables)
            ORDER BY tc.table_name, kcu.column_name
        """)
        violations: list[dict[str, Any]] = []
        with self._m.source_engine.connect() as src_conn:
            fk_rows = src_conn.execute(
                fk_meta_sql,
                {"schema_name": source_schema, "tables": list(ordered_tables)},
            ).fetchall()

        with self._m.target_engine.connect() as tgt_conn:
            for row in fk_rows:
                child_table, child_column, parent_table, parent_column = row[0], row[1], row[2], row[3]
                orphan_sql = text(
                    f"""
                    SELECT COUNT(*)::bigint
                    FROM {self._m.quote_identifier(target_schema)}.{self._m.quote_identifier(child_table)} c
                    LEFT JOIN {self._m.quote_identifier(target_schema)}.{self._m.quote_identifier(parent_table)} p
                      ON c.{self._m.quote_identifier(child_column)} = p.{self._m.quote_identifier(parent_column)}
                    WHERE c.{self._m.quote_identifier(child_column)} IS NOT NULL
                      AND p.{self._m.quote_identifier(parent_column)} IS NULL
                    """
                )
                orphan_count = int(tgt_conn.execute(orphan_sql).scalar() or 0)
                if orphan_count > 0:
                    violations.append(
                        {
                            "child_table": child_table,
                            "child_column": child_column,
                            "parent_table": parent_table,
                            "parent_column": parent_column,
                            "orphan_count": orphan_count,
                        }
                    )
        return violations

    def create_anonymized_table(
        self,
        source_schema: str,
        table_name: str,
        target_db: str | None,
        target_schema: str = "anon",
    ) -> tuple[bool, str]:
        """Create target anonymized table via CTAS or mirror fallback."""
        source_schema = source_schema or "ecommerce"
        quoted_source_schema = self._m.quote_identifier(source_schema)
        quoted_target_schema = self._m.quote_identifier(target_schema)
        quoted_table = self._m.quote_identifier(table_name)
        target_db_name = target_db or (make_url(self._m.target_db_url).database or "target_db")
        source_db_name = make_url(self._m.source_db_url).database or "source_db"
        cross_database_mode = target_db_name != source_db_name

        create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {quoted_target_schema}")
        ctas_sql = text(
            f"CREATE TABLE {quoted_target_schema}.{quoted_table} AS "
            f"SELECT * FROM {quoted_source_schema}.{quoted_table} WITH NO DATA"
        )
        target_engine_for_op = self._m.target_engine
        temp_engine = None
        if target_db_name != (make_url(self._m.target_db_url).database or "target_db"):
            temp_engine = create_engine(
                self._m._conn.build_database_url(target_db_name),
                connect_args={"client_encoding": "utf8"},
                pool_size=10,
                max_overflow=20,
            )
            target_engine_for_op = temp_engine

        try:
            if not cross_database_mode:
                with target_engine_for_op.connect() as target_conn:
                    with target_conn.begin():
                        target_conn.execute(create_schema_sql)
                        target_conn.execute(text(f"SET search_path TO {quoted_target_schema}, public;"))
                        target_conn.execute(ctas_sql)
                        self._ddl.sync_pk_unique_constraints(
                            target_conn, source_schema, target_schema, table_name
                        )
                        created_indexes = self._ddl.sync_non_constraint_indexes(
                            target_conn,
                            source_schema,
                            target_schema,
                            table_name,
                        )
                        self._m._structural_sync_counters["indexes_recreated"] += int(created_indexes or 0)
                        logger.info(
                            "✅ [DB_MANAGER] Created %s.%s.%s from source schema %s using CTAS.",
                            target_db_name,
                            target_schema,
                            table_name,
                            source_schema,
                        )
                        return True, "created_via_ctas"
        except Exception:
            pass
        try:
            with target_engine_for_op.connect() as target_conn:
                with target_conn.begin():
                    target_conn.execute(text(f"SET search_path TO {quoted_target_schema}, public;"))
                    self._ddl.ensure_target_table_mirror(
                        target_conn,
                        source_schema=source_schema,
                        target_schema=target_schema,
                        table_name=table_name,
                    )
            logger.info(
                "✅ [DB_MANAGER] Table aligned via mirror: %s.%s.%s",
                target_db_name,
                target_schema,
                table_name,
            )
            return True, "created_via_mirror"
        except Exception as fallback_error:
            logger.error(
                "❌ [DB_MANAGER] Failed to create anonymized table %s.%s: %s",
                target_schema,
                table_name,
                fallback_error,
            )
            return False, str(fallback_error)
        finally:
            if temp_engine is not None:
                temp_engine.dispose()

    def prepare_anonymization_target(
        self,
        source_schema: str,
        target_schema: str,
        ordered_tables: list[str],
    ) -> None:
        """Phase 1: create schema and skeleton tables without FKs."""
        with self._m.target_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

            for table in ordered_tables:
                logger.info("✅ [DB_MANAGER] Creating skeleton for %s.%s", target_schema, table)
                conn.execute(text(f"DROP TABLE IF EXISTS {target_schema}.{table} CASCADE"))
                conn.execute(
                    text(f"""
                    CREATE TABLE {target_schema}.{table}
                    (LIKE {source_schema}.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)
                """)
                )
                self.drop_fks_from_table(conn, target_schema, table)

            conn.commit()

    def drop_fks_from_table(self, conn: Any, schema: str, table: str) -> None:
        """Remove FK constraints from a table before bulk load."""
        query = text(f"""
            SELECT conname
            FROM pg_constraint
            WHERE contype = 'f'
            AND conrelid = '{schema}.{table}'::regclass
        """)
        fks = conn.execute(query).fetchall()
        for fk in fks:
            conn.execute(text(f"ALTER TABLE {schema}.{table} DROP CONSTRAINT {fk[0]}"))

    def restore_foreign_keys(self, source_schema: str, target_schema: str, tables: list[str]) -> None:
        """Copy FK definitions from source schema onto target tables."""
        query = text("""
            SELECT
                conname,
                pg_get_constraintdef(oid) as def
            FROM pg_constraint
            WHERE contype = 'f'
            AND conrelid::regclass::text LIKE :schema_prefix
        """)

        with self._m.target_engine.connect() as conn:
            res = conn.execute(query, {"schema_prefix": f"{source_schema}.%"})
            for row in res:
                con_name = row[0]
                con_def = row[1]
                new_def = con_def.replace(f"{source_schema}.", f"{target_schema}.")
                table_query = text(
                    f"SELECT relname FROM pg_class c JOIN pg_constraint con ON con.conrelid = c.oid WHERE con.conname = '{con_name}'"
                )
                tab_name = conn.execute(table_query).fetchone()[0]

                if tab_name in tables:
                    try:
                        conn.execute(
                            text(
                                f'ALTER TABLE "{target_schema}"."{tab_name}" ADD CONSTRAINT "{con_name}" {new_def}'
                            )
                        )
                    except Exception as e:
                        logger.warning("⚠️ [DB_MANAGER] FK mismatch on %s: %s", con_name, e)
            conn.commit()

    def drop_target_schema(self, target_schema: str) -> None:
        with self._m.target_engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE'))
            conn.commit()

    def align_db_types(
        self,
        target_schema: str,
        table_name: str,
        plan: list[Any],
        conn: Any | None = None,
    ) -> list[Any]:
        """Legacy hook retained for batch compatibility."""
        return []

    def drop_all_fks_for_table(self, conn: Any, schema: str, table: str) -> list[str]:
        """Store FK definitions in pending_fks and drop them from the live table."""
        find_fks_sql = text("""
            SELECT
                conname,
                relname,
                pg_get_constraintdef(c.oid) as constraint_def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = :schema_name
            AND (
                t.relname = :table_name
                OR c.confrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = :table_name
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema_name)
                )
            )
            AND c.contype = 'f';
        """)

        results = conn.execute(
            find_fks_sql,
            {"schema_name": schema, "table_name": table},
        ).fetchall()

        rehook_commands: list[str] = []

        for conname, relname, condef in results:
            rehook_sql = f"ALTER TABLE {schema}.{relname} ADD CONSTRAINT {conname} {condef}"
            rehook_commands.append(rehook_sql)

            conn.execute(
                text("""
                INSERT INTO _anon_metadata.pending_fks (target_schema, table_name, constraint_name, rehook_sql)
                VALUES (:s, :t, :c, :sql)
            """),
                {
                    "s": schema,
                    "t": relname,
                    "c": conname,
                    "sql": rehook_sql,
                },
            )

            logger.info("✅ [DB_MANAGER] Stored FK %s for table %s", conname, relname)
            conn.execute(text(f'ALTER TABLE "{schema}"."{relname}" DROP CONSTRAINT IF EXISTS "{conname}"'))

        return rehook_commands

    def rehook_foreign_keys(self, conn: Any, commands: list[str]) -> None:
        logger.info("✅ [DB_MANAGER] Re-hooking %s foreign keys", len(commands))

        for cmd in commands:
            try:
                conn.execute(text(cmd))
                con_name = cmd.split('CONSTRAINT "')[1].split('"')[0]
                conn.execute(
                    text("DELETE FROM _anon_metadata.pending_fks WHERE constraint_name = :c"),
                    {"c": con_name},
                )

                logger.info("✅ [DB_MANAGER] Successfully restored foreign key: %s", con_name)
            except Exception as e:
                logger.warning("⚠️ [DB_MANAGER] Re-hook warning: %s", e)

    def truncate_anon_tables(
        self,
        target_schema: str,
        ordered_tables: list[str],
        clear_mode: str = "truncate_cascade",
    ) -> None:
        if not ordered_tables:
            return

        tables_to_clear = ", ".join([f'"{target_schema}"."{t}"' for t in ordered_tables])

        truncate_sql = text(f"TRUNCATE TABLE {tables_to_clear} RESTART IDENTITY CASCADE;")
        mode = str(clear_mode or "truncate_cascade").lower()

        with self._m.target_engine.connect() as conn:
            if mode == "session_replica":
                conn.execute(text("SET session_replication_role = 'replica';"))
                try:
                    logger.info(
                        "✅ [DB_MANAGER] Clearing target tables using session_replication_role replica: %s",
                        tables_to_clear,
                    )
                    for table_name in ordered_tables:
                        conn.execute(text(f'DELETE FROM "{target_schema}"."{table_name}"'))
                finally:
                    conn.execute(text("SET session_replication_role = 'origin';"))
                conn.commit()
                logger.info("✅ [DB_MANAGER] Replica clear executed and committed successfully")
                return

            logger.info("✅ [DB_MANAGER] Running TRUNCATE CASCADE on %s", tables_to_clear)
            conn.execute(truncate_sql)
            conn.commit()
            logger.info("✅ [DB_MANAGER] TRUNCATE CASCADE executed and committed successfully")

    def set_fk_constraints_temporarily_disabled(
        self,
        target_schema: str,
        ordered_tables: list[str],
        disabled: bool = True,
    ) -> None:
        if not ordered_tables:
            return
        action = "DISABLE" if disabled else "ENABLE"
        with self._m.target_engine.connect() as conn:
            with conn.begin():
                for table_name in ordered_tables:
                    exists_row = conn.execute(
                        text("SELECT to_regclass(:reg_name)"),
                        {"reg_name": f'"{target_schema}"."{table_name}"'},
                    ).fetchone()
                    if not exists_row or not exists_row[0]:
                        continue
                    conn.execute(
                        text(f'ALTER TABLE "{target_schema}"."{table_name}" {action} TRIGGER ALL')
                    )
        logger.info(
            "✅ [DB_MANAGER] %sD FK trigger checks for %s table(s) in schema '%s'",
            "DISABLE" if disabled else "ENABLE",
            len(ordered_tables),
            target_schema,
        )
