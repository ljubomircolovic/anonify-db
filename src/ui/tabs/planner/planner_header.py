# -*- coding: utf-8 -*-
"""Planner header: target/metadata context banner."""

from __future__ import annotations

from urllib.parse import urlparse

import streamlit as st
from sqlalchemy import text


def render_target_context_banner(db, context="default"):
    """Render metadata vs target context with target connectivity check."""
    plan_meta = st.session_state.get("plan_metadata", {}) or {}
    metadata_conn_url = str(
        plan_meta.get("source_db_connection")
        or plan_meta.get("db_connection")
        or getattr(db, "source_db_url", "")
        or ""
    ).strip()
    target_conn_url = str(
        plan_meta.get("target_db_connection")
        or getattr(db, "target_db_url", "")
        or ""
    ).strip()
    target_db = str(
        plan_meta.get("plan_db_name")
        or (urlparse(target_conn_url).path.lstrip("/") if target_conn_url else "")
        or st.session_state.get("active_plan_db_name", "None")
    ).strip() or "None"
    active_schema = str(plan_meta.get("schema_name") or "public").strip() or "public"
    metadata_host = urlparse(metadata_conn_url).hostname or "unknown-host"
    metadata_db = (
        urlparse(metadata_conn_url).path.lstrip("/")
        if metadata_conn_url and urlparse(metadata_conn_url).path
        else str(plan_meta.get("db_name") or "unknown-db")
    ) or "unknown-db"
    target_host = urlparse(target_conn_url).hostname or "unknown-host"

    cols = st.columns([2, 2, 2, 0.8], gap="small")
    with cols[0]:
        st.caption(f"**📂 Metadata:** `{metadata_db}` @ `{metadata_host}`")
    with cols[1]:
        st.caption(f"**🎯 Target:** `{target_db}` (`{active_schema}`)")
    with cols[2]:
        st.caption(f"**🖥️ Host:** `{target_host}`")
    with cols[3]:
        if st.button("⚡ Test", key=f"{context}_target_conn_test_btn", use_container_width=True):
            try:
                if db is None:
                    raise RuntimeError("No active DB manager found")
                with db.target_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                st.session_state[f"{context}_target_conn_test_result"] = ("ok", f"{target_host}")
            except Exception:
                st.session_state[f"{context}_target_conn_test_result"] = ("fail", f"{target_host}")
        test_result = st.session_state.get(f"{context}_target_conn_test_result")
        if isinstance(test_result, tuple):
            icon = "✅" if test_result[0] == "ok" else "❌"
            st.caption(f"{icon}")
    st.caption("Metadata and anonymized target databases can run on different physical servers by design.")

