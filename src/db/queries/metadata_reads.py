# -*- coding: utf-8 -*-
"""Lightweight connectivity and audit-log reads against the target engine."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def verify_source_connection(engine: Engine) -> tuple[bool, str]:
    """Run ``SELECT 1`` on ``engine`` and return ``(ok, message)``."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connection successful! ✅"
    except Exception as exc:  # noqa: BLE001
        return False, f"Connection failed: {str(exc)} ❌"


def fetch_audit_logs(engine: Engine, metadata_schema: str, limit: int = 50) -> pd.DataFrame:
    """Return recent audit rows from ``metadata_schema.audit_log``."""
    query = text(
        f"""
            SELECT * FROM {metadata_schema}.audit_log
            ORDER BY execution_time DESC
            LIMIT :limit_val
        """
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"limit_val": int(limit)})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc).lower()
        if "does not exist" in error_text or "undefined_table" in error_text:
            return pd.DataFrame()
        logger.error("❌ [metadata_reads] Audit log read error: %s", exc)
        return pd.DataFrame()
