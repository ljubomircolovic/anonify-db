# -*- coding: utf-8 -*-
"""Plan-database URL helpers, admin connections, and target engine switching."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from urllib.parse import quote_plus

if TYPE_CHECKING:
    from src.database.db_manager import DBManager

logger = logging.getLogger(__name__)


def slugify_name(text_value: str | None) -> str:
    """Converts a plan name into a Postgres-safe identifier fragment."""
    normalized = re.sub(r"[^a-z0-9]+", "_", str(text_value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return (normalized or "default_plan")[:63]


def create_pooled_engine(db_url: str):
    """Create a standard SQLAlchemy engine with pooling tuned for AnonifyDB."""
    return create_engine(
        db_url,
        connect_args={"client_encoding": "utf8"},
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=10,
        max_overflow=20,
    )


class ConnectionFactory:
    """Bootstrap and switch plan databases while keeping URLs consistent."""

    __slots__ = ("_m",)

    def __init__(self, manager: "DBManager") -> None:
        self._m = manager

    def build_target_db_name(self, plan_name: str) -> str:
        source_db_name = make_url(self._m.source_db_url).database or "source"
        slugified = slugify_name(plan_name)
        return f"anon_{source_db_name}_{slugified}"[:63]

    @staticmethod
    def normalize_target_db_name(name_value: str | None) -> str:
        raw_value = str(name_value or "").strip()
        if not raw_value:
            return ""
        lowered = raw_value.lower()
        safe_value = re.sub(r"[^a-z0-9_]+", "_", lowered)
        safe_value = re.sub(r"_+", "_", safe_value).strip("_")
        return safe_value[:63]

    def build_database_url(self, database_name: str) -> str:
        parsed = make_url(self._m.source_db_url)
        drivername = parsed.drivername
        username = parsed.username or ""
        password = parsed.password
        host = parsed.host or "localhost"
        port = f":{parsed.port}" if parsed.port else ""
        encoded_password = quote_plus(password) if password is not None else ""
        auth_segment = username
        if encoded_password:
            auth_segment = f"{username}:{encoded_password}"
        if auth_segment:
            auth_segment = f"{auth_segment}@"
        return f"{drivername}://{auth_segment}{host}{port}/{database_name}"

    def build_postgres_admin_url(self) -> str:
        return self.build_database_url("postgres")

    def database_exists(self, database_name: str) -> bool:
        admin_engine = create_engine(
            self.build_postgres_admin_url(),
            connect_args={"client_encoding": "utf8"},
        )
        try:
            with admin_engine.connect() as conn:
                row = conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :db_name LIMIT 1"),
                    {"db_name": str(database_name or "").strip()},
                ).fetchone()
                return row is not None
        finally:
            admin_engine.dispose()

    def plan_exists(
        self,
        plan_name: str,
        custom_db_name: str | None = None,
        allow_non_anon_prefix: bool = False,
    ) -> tuple[bool, str]:
        if custom_db_name:
            normalized_custom_name = self.normalize_target_db_name(custom_db_name)
            if not normalized_custom_name:
                return False, ""
            if (not allow_non_anon_prefix) and (not normalized_custom_name.startswith("anon_")):
                normalized_custom_name = f"anon_{normalized_custom_name}"
            candidate_db_name = normalized_custom_name
        else:
            candidate_db_name = self.build_target_db_name(plan_name)
        return self.database_exists(candidate_db_name), candidate_db_name

    def set_target_engine(self, db_url: str) -> None:
        self._m.target_db_url = db_url
        self._m.db_url = db_url
        self._m.target_engine = create_pooled_engine(db_url)
        self._m.engine = self._m.target_engine

    def bootstrap_plan_database(
        self,
        plan_name: str,
        custom_db_name: str | None = None,
        allow_non_anon_prefix: bool = False,
    ) -> tuple[str, bool]:
        generated_name = self.build_target_db_name(plan_name)
        if custom_db_name:
            normalized_custom_name = self.normalize_target_db_name(custom_db_name)
            if not normalized_custom_name:
                raise ValueError("Custom database name cannot be empty.")
            if not allow_non_anon_prefix and not normalized_custom_name.startswith("anon_"):
                raise ValueError("Custom database names must start with anon_.")
            target_db_name = normalized_custom_name
        else:
            target_db_name = generated_name
        admin_url = self.build_postgres_admin_url()
        admin_engine = create_engine(admin_url, connect_args={"client_encoding": "utf8"})
        created_now = False

        try:
            with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(f'CREATE DATABASE "{target_db_name}"'))
                created_now = True
        except Exception as e:
            error_text = str(e).lower()
            if "already exists" not in error_text and "duplicate_database" not in error_text:
                raise
        finally:
            admin_engine.dispose()

        target_url = self.build_database_url(target_db_name)
        self.set_target_engine(target_url)
        self._m._init_metadata_table()
        return target_db_name, created_now

    def connect_to_existing_plan_database(self, plan_db_name: str) -> str:
        target_url = self.build_database_url(plan_db_name)
        self.set_target_engine(target_url)
        self._m._init_metadata_table()
        return plan_db_name

    def list_existing_plan_databases(self) -> list[str]:
        source_db_name = make_url(self._m.source_db_url).database or "source"
        prefix = f"anon_{source_db_name}_"
        admin_engine = create_engine(
            self.build_postgres_admin_url(),
            connect_args={"client_encoding": "utf8"},
        )
        try:
            with admin_engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT datname
                        FROM pg_database
                        WHERE datname LIKE :prefix
                        ORDER BY datname
                    """),
                    {"prefix": f"{prefix}%"},
                ).fetchall()
                return [row[0] for row in rows]
        finally:
            admin_engine.dispose()
