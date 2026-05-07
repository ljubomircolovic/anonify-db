import logging
import os
from typing import Any

from src.core.domain.services.strategy_driven_anonymization_service import (
    StrategyDrivenAnonymizationService,
)
from src.core.ports.anonymization_engine_port import AnonymizationEnginePort
from src.core.ports.database_port import DatabasePort
from src.core.ports.metadata_repository_port import MetadataRepositoryPort
from src.core.ports.plan_repository_port import PlanRepositoryPort

logger = logging.getLogger(__name__)


class DBManagerAdapter(
    DatabasePort,
    AnonymizationEnginePort,
    PlanRepositoryPort,
    MetadataRepositoryPort,
):
    """
    Strangler bridge adapter.
    Delegates all calls to the existing DBManager instance to preserve 100% of
    current runtime behavior while introducing interface boundaries.
    """

    def __init__(self, db_manager: Any):
        self._db_manager = db_manager
        self._strategy_service = StrategyDrivenAnonymizationService()

    @property
    def raw(self) -> Any:
        """Temporary escape hatch for legacy callsites during migration."""
        return self._db_manager

    def __getattr__(self, item: str) -> Any:
        # Preserves broad compatibility for untouched callsites.
        return getattr(self._db_manager, item)

    def get_all_schemas(self) -> list[str]:
        return self._db_manager.get_all_schemas()

    def get_tables(self, schema_name: str = "public") -> list[str]:
        return self._db_manager.get_tables(schema_name=schema_name)

    def get_tables_in_schema(self, schema: str = "public") -> list[str]:
        return self._db_manager.get_tables_in_schema(schema=schema)

    def read_table(
        self,
        table_name: str,
        schema_name: str = "public",
        where: str | None = None,
        limit: int = 100,
        params: dict | None = None,
    ) -> Any:
        return self._db_manager.read_table(
            table_name=table_name,
            schema_name=schema_name,
            where=where,
            limit=limit,
            params=params,
        )

    def get_columns(self, table_name: str, schema_name: str = "public") -> list[str]:
        return self._db_manager.get_columns(table_name=table_name, schema_name=schema_name)

    def get_column_details(self, table_name: str, schema_name: str) -> dict:
        return self._db_manager.get_column_details(table_name=table_name, schema_name=schema_name)

    def get_primary_keys(self, schema: str, table: str) -> list[str]:
        return self._db_manager.get_primary_keys(schema=schema, table=table)

    def get_all_foreign_keys(self, schema_name: str) -> list[tuple]:
        return self._db_manager.get_all_foreign_keys(schema_name=schema_name)

    def get_execution_order(self, selected_tables: list[str], schema_name: str = "public") -> list[str]:
        return self._db_manager.get_execution_order(selected_tables=selected_tables, schema_name=schema_name)

    def create_anonymized_table(
        self,
        source_schema: str,
        table_name: str,
        target_db: str,
        target_schema: str = "anon",
    ) -> tuple[bool, str]:
        return self._db_manager.create_anonymized_table(
            source_schema=source_schema,
            table_name=table_name,
            target_db=target_db,
            target_schema=target_schema,
        )

    def save_anonymized_table(
        self,
        df: Any,
        table_name: str,
        target_schema: str = "anon",
        conn: Any = None,
        source_schema: str = "public",
        preserve_native_columns: list[str] | None = None,
    ) -> bool:
        return self._db_manager.save_anonymized_table(
            df=df,
            table_name=table_name,
            target_schema=target_schema,
            conn=conn,
            source_schema=source_schema,
            preserve_native_columns=preserve_native_columns,
        )

    def truncate_anon_tables(
        self,
        target_schema: str,
        ordered_tables: list[str],
        clear_mode: str = "truncate_cascade",
    ) -> None:
        return self._db_manager.truncate_anon_tables(
            target_schema=target_schema,
            ordered_tables=ordered_tables,
            clear_mode=clear_mode,
        )

    def apply_anonymization_rules(self, df: Any, table_plan: list[dict], salt: str | None = None) -> Any:
        use_strategy_engine = str(
            os.getenv("ANONIFY_USE_STRATEGY_ENGINE", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        if use_strategy_engine:
            logger.info("[ANON_ENGINE] Using strategy-driven anonymization engine.")
            return self._strategy_service.apply_anonymization_rules(
                df=df,
                table_plan=table_plan,
                salt=salt,
                faker_instance=getattr(self._db_manager, "fake", None),
                fallback_legacy_transform=lambda d, p, s: self._db_manager.apply_anonymization_rules(
                    df=d, table_plan=p, salt=s
                ),
            )

        logger.info("[ANON_ENGINE] Using legacy DBManager anonymization engine.")
        return self._db_manager.apply_anonymization_rules(df=df, table_plan=table_plan, salt=salt)

    def ensure_plan_security_metadata(self, schema_name: str, table_name: str) -> tuple[str, str, bool]:
        return self._db_manager.ensure_plan_security_metadata(schema_name=schema_name, table_name=table_name)

    def get_table_sample(self, schema: str, table: str, limit: int = 5) -> list[dict]:
        return self._db_manager.get_table_sample(schema=schema, table=table, limit=limit)

    def get_unified_ai_scan_payload(self, schema: str, tables: list[str], sample_limit: int = 5) -> list[dict]:
        return self._db_manager.get_unified_ai_scan_payload(schema=schema, tables=tables, sample_limit=sample_limit)

    def save_ai_plan(
        self,
        schema_name: str,
        table_name: str,
        plan_data: list[dict],
        where_condition: str = "",
    ) -> bool:
        return self._db_manager.save_ai_plan(
            schema_name=schema_name,
            table_name=table_name,
            plan_data=plan_data,
            where_condition=where_condition,
        )

    def get_saved_plan(self, schema_name: str, table_name: str) -> dict | None:
        return self._db_manager.get_saved_plan(schema_name=schema_name, table_name=table_name)

    def get_all_saved_plans(self, schema_name: str) -> dict:
        return self._db_manager.get_all_saved_plans(schema_name=schema_name)

    def log_action(
        self,
        user: str,
        schema: str,
        table: str,
        score: int,
        salt: str,
        status: str = "SUCCESS",
    ) -> None:
        self._db_manager.log_action(
            user=user,
            schema=schema,
            table=table,
            score=score,
            salt=salt,
            status=status,
        )

    def log_unified_ai_scan(
        self,
        user: str,
        schema: str,
        tables: list[str],
        status: str = "UNIFIED_AI_SCAN",
        score: int = 0,
        salt: str = "unified_batch",
        estimated_tokens: int = 0,
    ) -> None:
        self._db_manager.log_unified_ai_scan(
            user=user,
            schema=schema,
            tables=tables,
            status=status,
            score=score,
            salt=salt,
            estimated_tokens=estimated_tokens,
        )

    def get_audit_logs(self, limit: int = 50) -> Any:
        return self._db_manager.get_audit_logs(limit=limit)

