# -*- coding: utf-8 -*-
"""Data-frame anonymization and masking logic extracted from DBManager."""

from __future__ import annotations

import hashlib
import json
import logging
import random
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.database.db_manager import DBManager

logger = logging.getLogger(__name__)


class AnonymizationEngine:
    """Applies plan-driven column transformations for previews and batch runs."""

    __slots__ = ("_m",)

    def __init__(self, manager: "DBManager") -> None:
        self._m = manager

    @staticmethod
    def coerce_decimal_for_sql(value: Any) -> float | Decimal | None:
        """Ensures NUMERIC/DECIMAL-compatible python values before INSERT."""
        if value is None or pd.isna(value):
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def cast_dataframe_to_table_types(
        self,
        df: pd.DataFrame,
        active_conn: Any,
        schema_name: str,
        table_name: str,
        preserve_native_columns: set[str] | None = None,
    ) -> pd.DataFrame:
        """Casts DataFrame values to match DB column types before insertion."""
        if df.empty:
            return df

        type_sql = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        rows = active_conn.execute(
            type_sql,
            {"schema_name": schema_name, "table_name": table_name},
        ).fetchall()
        col_types = {row[0]: str(row[1]).lower() for row in rows}

        preserve_native_columns = set(preserve_native_columns or [])
        cast_df = df.copy()
        for col_name, col_type in col_types.items():
            if col_name not in cast_df.columns:
                continue

            if any(token in col_type for token in ["bigint", "integer", "smallint", "int"]):
                if col_name in preserve_native_columns:
                    cast_df[col_name] = cast_df[col_name].apply(
                        lambda v: v if v is None or pd.isna(v) else int(v)
                    )
                    continue
                numeric_series = pd.to_numeric(cast_df[col_name], errors="coerce")
                cast_df[col_name] = numeric_series.apply(
                    lambda v: int(v) if pd.notnull(v) and float(v).is_integer() else (None if pd.notnull(v) else None)
                )
            elif any(token in col_type for token in ["numeric", "decimal", "double", "real"]):
                if col_name in preserve_native_columns:
                    cast_df[col_name] = cast_df[col_name].apply(self.coerce_decimal_for_sql)
                    continue
                coerced = pd.to_numeric(cast_df[col_name], errors="coerce")
                invalid_mask = coerced.isna() & cast_df[col_name].notna()
                if invalid_mask.any():
                    invalid_idx = invalid_mask[invalid_mask].index
                    fallback_values = pd.to_numeric(df.loc[invalid_idx, col_name], errors="coerce")
                    fallback_ok = fallback_values.notna()
                    if fallback_ok.any():
                        restore_idx = fallback_values[fallback_ok].index
                        logger.warning(
                            "⚠️ Numeric cast fallback on '%s': reverting %s values to original.",
                            col_name,
                            int(fallback_ok.sum()),
                        )
                        coerced.loc[restore_idx] = fallback_values[fallback_ok]
                    remaining_invalid = coerced.isna() & cast_df[col_name].notna()
                    if remaining_invalid.any():
                        logger.warning(
                            "⚠️ Numeric cast unresolved on '%s': %s values set to NULL.",
                            col_name,
                            int(remaining_invalid.sum()),
                        )
                cast_df[col_name] = coerced
            elif "boolean" in col_type:
                bool_map = {
                    "true": True,
                    "false": False,
                    "t": True,
                    "f": False,
                    "1": True,
                    "0": False,
                    "yes": True,
                    "no": False,
                }
                cast_df[col_name] = cast_df[col_name].apply(
                    lambda v: bool_map.get(str(v).strip().lower(), v) if pd.notnull(v) else v
                )
            elif any(token in col_type for token in ["date", "timestamp", "time"]):
                cast_df[col_name] = pd.to_datetime(cast_df[col_name], errors="coerce")

        return cast_df

    @staticmethod
    def infer_sql_type_from_series(series: pd.Series) -> str:
        """Infer a conservative SQL type for auto-DDL from a pandas Series."""
        if pd.api.types.is_bool_dtype(series):
            return "BOOLEAN"
        if pd.api.types.is_integer_dtype(series):
            return "BIGINT"
        if pd.api.types.is_float_dtype(series):
            return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        return "VARCHAR(255)"

    def ensure_target_table_from_dataframe(
        self,
        active_conn: Any,
        target_schema: str,
        table_name: str,
        df: pd.DataFrame | None,
    ) -> None:
        """Auto-DDL fallback when mirror metadata is unavailable at write time."""
        quoted_target_schema = self._m.quote_identifier(target_schema)
        quoted_table_name = self._m.quote_identifier(table_name)
        active_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_target_schema}"))
        exists_sql = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :target_schema
                  AND table_name = :table_name
            )
        """)
        exists = active_conn.execute(
            exists_sql, {"target_schema": target_schema, "table_name": table_name}
        ).scalar()
        if exists:
            return
        if df is None or df.empty or len(df.columns) == 0:
            active_conn.execute(
                text(f"CREATE TABLE IF NOT EXISTS {quoted_target_schema}.{quoted_table_name} (id BIGINT)")
            )
            return

        column_defs = []
        for col in df.columns:
            sql_type = self.infer_sql_type_from_series(df[col])
            column_defs.append(f"{self._m.quote_identifier(col)} {sql_type}")
        ddl = ", ".join(column_defs)
        active_conn.execute(
            text(f"CREATE TABLE IF NOT EXISTS {quoted_target_schema}.{quoted_table_name} ({ddl})")
        )

    def mask_value(self, val: Any) -> str:
        """Lightweight string masking for samples and numeric-safe mask paths."""
        s = str(val)
        if len(s) <= 3:
            return "***"
        if "@" in s:
            parts = s.split("@")
            return f"{parts[0][:2]}**@{parts[1][:2]}**.com"
        return f"{s[:3]}***"

    def get_global_mapping(self, col_name: str, orig_val: Any, salt: str) -> Any:
        """Return stored anonymized value for a column/value/salt triple, if any."""
        query = text("""
            SELECT anonymized_value FROM _anon_metadata.global_id_mapping
            WHERE column_name = :c AND original_value = :o AND salt_used = :s
        """)
        try:
            with self._m.target_engine.connect() as conn:
                res = conn.execute(query, {"c": col_name, "o": str(orig_val), "s": salt}).fetchone()
                return res[0] if res else None
        except Exception:
            return None

    def save_global_mapping(self, col_name: str, orig_val: Any, anon_val: Any, salt: str) -> None:
        """Persist a global ID mapping row (best-effort)."""
        query = text("""
            INSERT INTO _anon_metadata.global_id_mapping (column_name, original_value, anonymized_value, salt_used)
            VALUES (:c, :o, :a, :s)
            ON CONFLICT DO NOTHING
        """)
        try:
            with self._m.target_engine.connect() as conn:
                conn.execute(query, {"c": col_name, "o": str(orig_val), "a": str(anon_val), "s": salt})
                conn.commit()
        except Exception:
            pass

    def get_mapping_value(self, original_value: Any, category: str, locale: str, salt: str) -> str:
        """Deterministic pick from mapping pool for category/locale."""
        with self._m.target_engine.connect() as conn:
            query = text("""
                SELECT v.fake_value
                FROM _anon_metadata.mapping_values v
                JOIN _anon_metadata.mapping_catalog c ON v.catalog_id = c.id
                WHERE c.category_name = :cat AND c.locale = :loc
                ORDER BY v.fake_value ASC
            """)
            res = conn.execute(query, {"cat": category, "loc": locale})
            pool = [row[0] for row in res]

        if not pool:
            return f"Fake_{category}"

        combined = f"{original_value}{salt}".encode("utf-8")
        hash_int = int(hashlib.sha256(combined).hexdigest(), 16)
        index = hash_int % len(pool)

        return pool[index]

    def get_mapping_values_by_locale(self, category: str, locale: str) -> list[Any]:
        """Return ordered fake values for a catalog category/locale."""
        query = text("""
            SELECT v.fake_value
            FROM _anon_metadata.mapping_values v
            JOIN _anon_metadata.mapping_catalog c ON v.catalog_id = c.id
            WHERE c.category_name = :cat AND c.locale = :loc
            ORDER BY v.fake_value ASC
        """)
        with self._m.target_engine.connect() as conn:
            res = conn.execute(query, {"cat": category, "loc": locale})
            return [row[0] for row in res]

    @staticmethod
    def deterministic_map(original_value: Any, mapping_list: list[Any], salt: str) -> Any:
        """Hash-modulo selection into a sorted mapping list."""
        if not mapping_list:
            return original_value
        combined = f"{original_value}{salt}".encode("utf-8")
        hash_int = int(hashlib.sha256(combined).hexdigest(), 16)
        index = hash_int % len(mapping_list)
        return mapping_list[index]

    def apply_anonymization_rules(
        self,
        df: pd.DataFrame,
        table_plan: Any,
        salt: str | None = None,
        consistency_seed_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Transform dataframe rows according to plan strategies (type-safe)."""
        if df.empty:
            return df

        df_anon = df.copy()

        if isinstance(table_plan, str):
            try:
                table_plan = json.loads(table_plan)
            except Exception:
                logger.error("❌ [AI_SCAN] Failed to parse plan in apply_anonymization_rules")
                return df_anon

        effective_salt = salt or "default_plan_salt"
        consistency_seed_map = consistency_seed_map or {}
        self._m._apply_runtime_seed()

        for item in table_plan:
            if not isinstance(item, dict):
                continue

            col = item.get("column")
            strategy = str(item.get("strategy", "keep")).lower()
            original_series = df_anon[col].copy() if col in df_anon.columns else None

            if not col or col not in df_anon.columns:
                continue
            if strategy == "keep":
                df_anon[col] = original_series
                continue

            if strategy == "null":
                df_anon[col] = None

            elif strategy in ["faker_name", "faker_email", "faker_phone"]:
                def get_faker(strat: str) -> str:
                    try:
                        if strat == "faker_name":
                            return self._m.fake.name()
                        if strat == "faker_email":
                            return self._m.fake.email()
                        if strat == "faker_phone":
                            return self._m.fake.phone_number()
                    except Exception:
                        return "Redacted"
                    return "Redacted"

                df_anon[col] = [get_faker(strategy) for _ in range(len(df_anon))]

            elif strategy == "date_shift":
                if pd.api.types.is_datetime64_any_dtype(df_anon[col]):
                    def shift_date(val: Any) -> Any:
                        if pd.isnull(val):
                            return val
                        days_to_shift = random.randint(-30, 30)
                        try:
                            return val + timedelta(days=days_to_shift)
                        except Exception:
                            return val

                    df_anon[col] = df_anon[col].apply(shift_date)

            elif strategy == "noise":
                if pd.api.types.is_numeric_dtype(df_anon[col]):
                    def add_noise(val: Any) -> Any:
                        if pd.isnull(val):
                            return val
                        variation = float(val) * random.uniform(-0.1, 0.1)
                        return type(val)(val + variation)

                    df_anon[col] = df_anon[col].apply(add_noise)

            elif strategy == "mapping":
                category = "first_name"
                if "last" in col.lower():
                    category = "last_name"
                elif "city" in col.lower():
                    category = "city"

                m_list = self.get_mapping_values_by_locale(category, "de")
                if m_list:
                    df_anon[col] = df_anon[col].apply(
                        lambda x: self.deterministic_map(x, m_list, effective_salt) if pd.notnull(x) else x
                    )

            elif strategy == "hash":
                def secure_hash(val: Any) -> Any:
                    if pd.isnull(val):
                        return val
                    column_seed = consistency_seed_map.get(col, effective_salt)
                    hash_obj = hashlib.sha256(f"{val}{column_seed}".encode())
                    return hash_obj.hexdigest()[:12]

                df_anon[col] = df_anon[col].apply(secure_hash)

            elif strategy == "mask":
                is_numeric_like_column = pd.api.types.is_numeric_dtype(original_series)
                if is_numeric_like_column:
                    import re

                    def sanitize_numeric_mask(masked_val: Any, fallback_val: Any) -> Any:
                        if pd.isnull(masked_val):
                            return fallback_val
                        cleaned = re.sub(r"[^0-9.]", "", str(masked_val))
                        if cleaned.count(".") > 1:
                            first_dot = cleaned.find(".")
                            cleaned = cleaned[: first_dot + 1] + cleaned[first_dot + 1 :].replace(".", "")
                        if cleaned in ("", "."):
                            logger.warning(
                                "⚠️ Numeric sanitization fallback on column '%s' for value '%s'. Keeping original value.",
                                col,
                                masked_val,
                            )
                            return fallback_val
                        try:
                            return float(cleaned)
                        except Exception:
                            logger.warning(
                                "⚠️ Numeric cast fallback on column '%s' for value '%s'. Keeping original value.",
                                col,
                                masked_val,
                            )
                            return fallback_val

                    masked_series = df_anon[col].apply(
                        lambda x: self.mask_value(x) if pd.notnull(x) else x
                    )
                    df_anon[col] = [
                        sanitize_numeric_mask(masked_val, fallback_val)
                        for masked_val, fallback_val in zip(masked_series, original_series, strict=False)
                    ]
                else:
                    df_anon[col] = df_anon[col].apply(
                        lambda x: self.mask_value(x) if pd.notnull(x) else x
                    )

            if pd.api.types.is_numeric_dtype(original_series):
                coerced = pd.to_numeric(df_anon[col], errors="coerce")
                invalid_mask = coerced.isna() & original_series.notna()
                if invalid_mask.any():
                    logger.warning(
                        "⚠️ Numeric integrity fallback on '%s': %s invalid values reverted to original.",
                        col,
                        int(invalid_mask.sum()),
                    )
                    df_anon.loc[invalid_mask, col] = original_series.loc[invalid_mask]

        return df_anon

    def load_forced_mappings_from_db(self, schema_name: str = "ecommerce") -> dict[str, dict[str, Any]]:
        query = text(
            f'SELECT column_name, is_pii, strategy, reason FROM "{schema_name}"."anon_forced_mappings"'
        )
        try:
            with self._m.source_engine.connect() as conn:
                result = conn.execute(query)
                return {
                    row.column_name.lower(): {
                        "is_pii": row.is_pii,
                        "strategy": row.strategy,
                        "reason": row.reason,
                    }
                    for row in result
                }
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Database error: %s", str(e))
            return {}

    def analyze_table_structure(
        self, df_sample: pd.DataFrame, agent: Any, schema_name: str = "ecommerce"
    ) -> dict[str, Any]:
        columns = df_sample.columns.tolist()
        db_mappings = self.load_forced_mappings_from_db(schema_name)
        to_analyze: list[dict[str, Any]] = []
        final_plan: list[dict[str, Any]] = []

        for col in columns:
            col_lower = col.lower()
            if col_lower in db_mappings:
                rule = db_mappings[col_lower].copy()
                rule["column"] = col
                final_plan.append(rule)
            else:
                sample_data = df_sample[col].dropna().head(3).tolist()
                to_analyze.append({"column": col, "sample_values": [str(v) for v in sample_data]})

        if to_analyze:
            ai_response = agent.analyze_metadata(to_analyze)
            if ai_response and hasattr(ai_response, "plan"):
                for item in ai_response.plan:
                    final_plan.append(
                        {
                            "column": item.column,
                            "is_pii": item.is_pii,
                            "strategy": item.strategy,
                            "reason": item.reason,
                        }
                    )

        return {"plan": final_plan}
