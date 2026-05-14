# -*- coding: utf-8 -*-
"""Subset tracking and sequential anonymization batch execution."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.database.db_manager import DBManager

logger = logging.getLogger(__name__)


class BatchExecutor:
    """Runs multi-table anonymization inside a single target connection/transaction."""

    __slots__ = ("_m",)

    def __init__(self, manager: "DBManager") -> None:
        self._m = manager

    def prepare_subset_metadata(self, conn: Any) -> None:
        """Create temp subset_tracking table visible for the rest of the transaction."""
        conn.execute(
            text("""
            CREATE TEMP TABLE IF NOT EXISTS subset_tracking (
                column_name VARCHAR(255),
                key_value VARCHAR(255),
                PRIMARY KEY (column_name, key_value)
            ) ON COMMIT PRESERVE ROWS;
        """)
        )

        logger.info("✅ [DB_MANAGER] Temp table subset_tracking created")

        try:
            conn.execute(
                text(
                    "INSERT INTO subset_tracking (column_name, key_value) VALUES ('test', '1') ON CONFLICT DO NOTHING"
                )
            )
            logger.info("✅ [DB_MANAGER] Debug INSERT succeeded")
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Error during debug INSERT: %s", e)

    def register_keys(self, conn: Any, column_name: str, values: list[Any]) -> None:
        """Insert key values into subset_tracking for downstream JOIN filtering."""
        if not values:
            return

        data = [{"c": column_name, "v": str(v)} for v in values]
        conn.execute(
            text("""
            INSERT INTO subset_tracking (column_name, key_value)
            VALUES (:c, :v)
            ON CONFLICT DO NOTHING
        """),
            data,
        )

    def execute_anonymization_batch(
        self,
        selected_schema: str,
        target_schema: str,
        full_plan: dict[str, Any],
        ordered_tables: list[str],
    ) -> None:
        """Enterprise batch: subset JOINs, per-table anonymization, FK re-hook."""
        all_rehook_commands: list[str] = []

        with self._m.target_engine.connect() as conn:
            with conn.begin():
                self.prepare_subset_metadata(conn)

                all_relations = self._m.get_all_foreign_keys(selected_schema)

                for table_name in ordered_tables:
                    if table_name not in full_plan:
                        continue

                    data = full_plan[table_name]
                    plan = data.get("plan", [])
                    base_where = str(data.get("where", "")).strip()

                    parent_filters: list[tuple[str, str]] = []
                    for rel in all_relations:
                        child_table, child_col, parent_table, parent_col = rel[0], rel[1], rel[2], rel[3]
                        if child_table == table_name:
                            parent_filters.append((child_col, parent_col))

                    query = f"SELECT t.* FROM {selected_schema}.{table_name} t"

                    if parent_filters:
                        for i, (c_col, p_col) in enumerate(parent_filters):
                            alias = f"s{i}"
                            query += f" JOIN subset_tracking {alias} ON t.{c_col}::VARCHAR = {alias}.key_value"
                            query += f" AND {alias}.column_name = '{p_col}'"

                    if base_where:
                        query += f" WHERE ({base_where})"

                    logger.info("✅ [DB_MANAGER] Batch processing %s", table_name)

                    df = pd.read_sql(text(query), conn)

                    if df.empty:
                        logger.warning(
                            "⚠️ [DB_MANAGER] Table %s is empty after subsetting. Skipping",
                            table_name,
                        )
                        continue

                    for rel in all_relations:
                        if rel[2] == table_name:
                            p_col = rel[3]
                            if p_col in df.columns:
                                unique_keys = df[p_col].unique().tolist()
                                self.register_keys(conn, p_col, unique_keys)

                    table_fks = self._m.align_db_types(target_schema, table_name, plan, conn=conn)
                    if table_fks:
                        all_rehook_commands.extend(table_fks)

                    table_salt, _, _ = self._m.ensure_plan_security_metadata(selected_schema, table_name)
                    df_anon = self._m.apply_anonymization_rules(df, plan, salt=table_salt)
                    self._m.save_anonymized_table(
                        df_anon,
                        table_name,
                        target_schema,
                        conn=conn,
                        source_schema=selected_schema,
                        preserve_native_columns=[
                            i.get("column")
                            for i in plan
                            if isinstance(i, dict) and str(i.get("strategy", "keep")).lower() == "keep"
                        ],
                    )

                if all_rehook_commands:
                    unique_fks = list(set(all_rehook_commands))
                    self._m.rehook_foreign_keys(conn, unique_fks)

            logger.info("✅ [DB_MANAGER] Batch process completed successfully")
