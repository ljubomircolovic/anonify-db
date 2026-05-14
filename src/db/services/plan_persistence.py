# -*- coding: utf-8 -*-
"""Plan JSON persistence, security metadata, and audit rows for anonymization plans."""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
from typing import TYPE_CHECKING, Any

from faker import Faker
from sqlalchemy import text

from src.db.queries import metadata_reads

if TYPE_CHECKING:
    from src.database.db_manager import DBManager

logger = logging.getLogger(__name__)


class PlanPersistence:
    """Save/load anonymization plans and related metadata on the target engine."""

    __slots__ = ("_m",)

    def __init__(self, manager: "DBManager") -> None:
        self._m = manager

    @staticmethod
    def generate_plan_salt() -> str:
        """Creates cryptographically-strong per-plan salt."""
        return secrets.token_hex(32)

    def compute_source_list_hash(self) -> str:
        """Deterministic fingerprint of replacement sources (Faker + mapping catalog)."""
        payload: dict[str, Any] = {
            "faker_version": getattr(Faker, "VERSION", "unknown"),
            "mapping_catalog": [],
        }
        query = text("""
            SELECT c.category_name, c.locale, v.fake_value
            FROM _anon_metadata.mapping_catalog c
            LEFT JOIN _anon_metadata.mapping_values v ON v.catalog_id = c.id
            ORDER BY c.category_name ASC, c.locale ASC, v.fake_value ASC
        """)
        try:
            with self._m.target_engine.connect() as conn:
                rows = conn.execute(query).fetchall()
            payload["mapping_catalog"] = [
                {"category": row[0], "locale": row[1], "value": row[2]}
                for row in rows
            ]
        except Exception as e:
            logger.warning("⚠️ [DB_MANAGER] Failed to read mapping sources for fingerprint: %s", e)
        payload_json = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def ensure_plan_security_metadata(
        self, schema_name: str, table_name: str
    ) -> tuple[str, str, bool]:
        """Ensure per-plan salt + source list fingerprint exist; return (salt, hash, mismatch)."""
        current_hash = self.compute_source_list_hash()
        query = text("""
            SELECT salt, source_list_hash
            FROM _anon_metadata.ai_plans
            WHERE schema_name = :s AND table_name = :t
            LIMIT 1
        """)
        upsert = text("""
            INSERT INTO _anon_metadata.ai_plans
                (schema_name, table_name, plan_json, where_condition, salt, source_list_hash, last_updated)
            VALUES
                (:s, :t, '[]'::jsonb, '', :salt, :source_hash, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                salt = COALESCE(_anon_metadata.ai_plans.salt, EXCLUDED.salt),
                source_list_hash = COALESCE(_anon_metadata.ai_plans.source_list_hash, EXCLUDED.source_list_hash)
        """)
        mirror_upsert = text("""
            INSERT INTO metadata.plans
                (schema_name, table_name, plan_json, where_condition, salt, source_list_hash, last_updated)
            VALUES
                (:s, :t, '[]'::jsonb, '', :salt, :source_hash, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                salt = COALESCE(metadata.plans.salt, EXCLUDED.salt),
                source_list_hash = COALESCE(metadata.plans.source_list_hash, EXCLUDED.source_list_hash)
        """)
        with self._m.target_engine.connect() as conn:
            row = conn.execute(query, {"s": schema_name, "t": table_name}).fetchone()
            existing_salt = row[0] if row and row[0] else None
            stored_hash = row[1] if row and row[1] else None
            plan_salt = existing_salt or self.generate_plan_salt()
            if not row or not existing_salt or not stored_hash:
                conn.execute(
                    upsert,
                    {
                        "s": schema_name,
                        "t": table_name,
                        "salt": plan_salt,
                        "source_hash": stored_hash or current_hash,
                    },
                )
                conn.execute(
                    mirror_upsert,
                    {
                        "s": schema_name,
                        "t": table_name,
                        "salt": plan_salt,
                        "source_hash": stored_hash or current_hash,
                    },
                )
                conn.commit()
            mismatch = bool(stored_hash and stored_hash != current_hash)
            effective_hash = stored_hash or current_hash
        logger.info(
            "✅ [DB_MANAGER] Plan security context %s.%s | salt=%s | source_list_hash=%s",
            schema_name,
            table_name,
            plan_salt,
            effective_hash,
        )
        return plan_salt, effective_hash, mismatch

    def init_metadata_tables(self) -> None:
        """Create metadata schemas/tables if they do not exist."""
        query = """
        CREATE SCHEMA IF NOT EXISTS _anon_metadata;
        CREATE SCHEMA IF NOT EXISTS metadata;
        CREATE TABLE IF NOT EXISTS _anon_metadata.ai_plans (
            schema_name TEXT,
            table_name TEXT,
            plan_json JSONB,
            where_condition TEXT DEFAULT '',
            salt TEXT,
            source_list_hash TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (schema_name, table_name)
        );
        ALTER TABLE _anon_metadata.ai_plans
            ADD COLUMN IF NOT EXISTS where_condition TEXT DEFAULT '';
        ALTER TABLE _anon_metadata.ai_plans
            ADD COLUMN IF NOT EXISTS salt TEXT;
        ALTER TABLE _anon_metadata.ai_plans
            ADD COLUMN IF NOT EXISTS source_list_hash TEXT;
        CREATE TABLE IF NOT EXISTS metadata.plans (
            schema_name TEXT,
            table_name TEXT,
            plan_json JSONB,
            where_condition TEXT DEFAULT '',
            salt TEXT,
            source_list_hash TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (schema_name, table_name)
        );
        CREATE TABLE IF NOT EXISTS _anon_metadata.global_id_mapping (
            column_name TEXT,
            original_value TEXT,
            anonymized_value TEXT,
            salt_used TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (column_name, original_value, salt_used)
        );
        CREATE TABLE IF NOT EXISTS _anon_metadata.audit_log (
            id SERIAL PRIMARY KEY,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_name TEXT,
            schema_name TEXT,
            table_name TEXT,
            privacy_score INTEGER,
            estimated_tokens INTEGER,
            salt_used TEXT,
            status TEXT
        );
        ALTER TABLE _anon_metadata.audit_log
            ADD COLUMN IF NOT EXISTS estimated_tokens INTEGER;
        CREATE TABLE IF NOT EXISTS _anon_metadata.pending_fks (
            id SERIAL PRIMARY KEY,
            target_schema TEXT,
            table_name TEXT,
            constraint_name TEXT,
            rehook_sql TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS _anon_metadata.mapping_catalog (
            id SERIAL PRIMARY KEY,
            category_name TEXT NOT NULL,
            locale TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS _anon_metadata.mapping_values (
            id SERIAL PRIMARY KEY,
            catalog_id INTEGER REFERENCES _anon_metadata.mapping_catalog(id) ON DELETE CASCADE,
            fake_value TEXT NOT NULL
        );
        """
        try:
            with self._m.target_engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Metadata init error: %s", e)

    def save_ai_plan(
        self,
        schema_name: str,
        table_name: str,
        plan_data: Any,
        where_condition: str = "",
    ) -> bool:
        """Persist plan JSON and WHERE clause to _anon_metadata.ai_plans (+ mirror)."""
        plan_salt, current_source_hash, source_mismatch = self.ensure_plan_security_metadata(
            schema_name, table_name
        )

        query = text("""
            INSERT INTO _anon_metadata.ai_plans (
                schema_name,
                table_name,
                plan_json,
                where_condition,
                salt,
                source_list_hash,
                last_updated
            )
            VALUES (:s, :t, :p, :w, :salt, :source_hash, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                plan_json = EXCLUDED.plan_json,
                where_condition = EXCLUDED.where_condition,
                last_updated = CURRENT_TIMESTAMP
        """)
        mirror_query = text("""
            INSERT INTO metadata.plans (
                schema_name,
                table_name,
                plan_json,
                where_condition,
                salt,
                source_list_hash,
                last_updated
            )
            VALUES (:s, :t, :p, :w, :salt, :source_hash, CURRENT_TIMESTAMP)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                plan_json = EXCLUDED.plan_json,
                where_condition = EXCLUDED.where_condition,
                salt = EXCLUDED.salt,
                source_list_hash = EXCLUDED.source_list_hash,
                last_updated = CURRENT_TIMESTAMP
        """)

        try:
            with self._m.target_engine.connect() as conn:
                conn.execute(
                    query,
                    {
                        "s": schema_name,
                        "t": table_name,
                        "p": json.dumps(plan_data),
                        "w": where_condition,
                        "salt": plan_salt,
                        "source_hash": current_source_hash,
                    },
                )
                conn.execute(
                    mirror_query,
                    {
                        "s": schema_name,
                        "t": table_name,
                        "p": json.dumps(plan_data),
                        "w": where_condition,
                        "salt": plan_salt,
                        "source_hash": current_source_hash,
                    },
                )
                conn.commit()
                if source_mismatch:
                    logger.warning(
                        "⚠️ [DB_MANAGER] Source list version mismatch for %s.%s: "
                        "stored hash differs from current replacement source hash.",
                        schema_name,
                        table_name,
                    )
                logger.info("✅ Plan & Filter successfully saved for %s.%s", schema_name, table_name)
                return True
        except Exception as e:
            logger.error("❌ Error saving to _anon_metadata.ai_plans for %s: %s", table_name, e)
            return False

    def get_saved_plan(self, schema_name: str, table_name: str) -> dict[str, Any] | None:
        """Load plan JSON and WHERE; normalize to a Python list under key ``plan``."""
        query = text("""
            SELECT plan_json, where_condition, salt, source_list_hash
            FROM _anon_metadata.ai_plans
            WHERE schema_name = :s AND table_name = :t
            LIMIT 1
        """)

        try:
            with self._m.target_engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name, "t": table_name}).fetchone()

                if result:
                    raw_plan = result[0]
                    where_cond = result[1] or ""
                    plan_salt = result[2]
                    stored_source_hash = result[3]
                    current_source_hash = self.compute_source_list_hash()
                    source_list_mismatch = bool(
                        stored_source_hash
                        and current_source_hash
                        and stored_source_hash != current_source_hash
                    )

                    if isinstance(raw_plan, str):
                        try:
                            plan_data = json.loads(raw_plan)
                        except json.JSONDecodeError:
                            logger.error(
                                "❌ [DB_MANAGER] JSON error for %s: Invalid format in database.",
                                table_name,
                            )
                            plan_data = []
                    else:
                        plan_data = raw_plan

                    if isinstance(plan_data, str):
                        plan_data = json.loads(plan_data)

                    if isinstance(plan_data, dict) and "plan" in plan_data:
                        final_plan = plan_data["plan"]
                    elif isinstance(plan_data, list):
                        final_plan = plan_data
                    else:
                        final_plan = []

                    if not isinstance(final_plan, list):
                        logger.warning(
                            "⚠️ [DB_MANAGER] Plan for %s is not a list but %s",
                            table_name,
                            type(final_plan),
                        )
                        final_plan = []

                    return {
                        "plan": final_plan,
                        "where": where_cond,
                        "salt": plan_salt,
                        "source_list_hash": stored_source_hash,
                        "source_list_hash_current": current_source_hash,
                        "source_list_mismatch": source_list_mismatch,
                    }

                return None
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Error loading _anon_metadata.ai_plans: %s", e)
            return None

    def get_all_saved_plans(self, schema_name: str) -> dict[str, Any]:
        """Return table_name -> plan list for all rows in ai_plans for a schema."""
        query = text("""
            SELECT table_name, plan_json
            FROM _anon_metadata.ai_plans
            WHERE schema_name = :s
        """)

        try:
            with self._m.source_engine.connect() as conn:
                result = conn.execute(query, {"s": schema_name})
                plans: dict[str, Any] = {}
                for row in result:
                    table_name_db = row[0]
                    raw_plan = row[1]

                    if raw_plan is None:
                        plans[table_name_db] = []
                        continue

                    if isinstance(raw_plan, str):
                        plans[table_name_db] = json.loads(raw_plan)
                    else:
                        plans[table_name_db] = raw_plan

                return plans
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Error fetching all saved plans: %s", e)
            return {}

    def log_action(
        self,
        user: str,
        schema: str,
        table: str,
        score: int,
        salt: str,
        status: str = "SUCCESS",
    ) -> None:
        """Insert a single-table execution row into audit_log."""
        query = text("""
            INSERT INTO _anon_metadata.audit_log (user_name, schema_name, table_name, privacy_score, salt_used, status)
            VALUES (:u, :s, :t, :score, :salt, :status)
        """)
        try:
            with self._m.target_engine.connect() as conn:
                conn.execute(
                    query,
                    {
                        "u": user,
                        "s": schema,
                        "t": table,
                        "score": score,
                        "salt": salt,
                        "status": status,
                    },
                )
                conn.commit()
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Audit logging error: %s", e)

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
        """Record one audit_log event for a unified batch AI scan."""
        table_list_payload = json.dumps(list(tables or []), ensure_ascii=False)
        query = text("""
            INSERT INTO _anon_metadata.audit_log (user_name, schema_name, table_name, privacy_score, estimated_tokens, salt_used, status)
            VALUES (:u, :s, :t, :score, :estimated_tokens, :salt, :status)
        """)
        try:
            with self._m.target_engine.connect() as conn:
                conn.execute(
                    query,
                    {
                        "u": user or "system",
                        "s": schema,
                        "t": table_list_payload,
                        "score": int(score or 0),
                        "estimated_tokens": int(estimated_tokens or 0),
                        "salt": salt,
                        "status": status,
                    },
                )
                conn.commit()
        except Exception as e:
            logger.error("❌ [DB_MANAGER] Unified AI audit logging error: %s", e)

    def get_audit_logs(self, limit: int = 50):
        """Return recent audit rows as a DataFrame."""
        return metadata_reads.fetch_audit_logs(
            self._m.target_engine, self._m.metadata_schema, limit
        )
