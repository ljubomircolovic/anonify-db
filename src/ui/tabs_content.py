# -*- coding: utf-8 -*-
"""Tab routing and lightweight helpers kept outside the planner package."""

from __future__ import annotations

import logging

import streamlit as st
from sqlalchemy import text

from src.ui.tabs.comparison_tab import render_comparison_tab
from src.ui.tabs.planner.planner_table_config import render_planner_tab

logger = logging.getLogger(__name__)


def render_tabs(db):
    """Route primary application tabs (plan, comparison, audit)."""
    tab_list = ["🛠️ Plan", "🔍 Comparison", "📜 Audit"]
    tabs = st.tabs(tab_list)

    with tabs[0]:
        render_planner_tab(db)

    with tabs[1]:
        render_comparison_tab(db)

    with tabs[2]:
        log_df = db.get_audit_logs(limit=50)
        if log_df.empty:
            st.info("No audit logs found yet.")
        else:
            st.dataframe(log_df, width="stretch")


def sync_anon_ddl_with_plan(db, target_schema, table_name, plan):
    """
    Aligns anon schema data types with anonymization plan.
    Receives 'db' (DBManager instance) instead of self.
    """
    text_strategies = ["hash", "faker_name", "faker_email", "faker_phone", "mask", "mapping"]

    with db.engine.connect() as conn:
        for item in plan:
            col = item["column"]
            strategy = item.get("strategy", "keep").lower()

            if strategy in text_strategies:
                check_query = text("""
                    SELECT data_type FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t AND column_name = :c
                """)
                current_type = conn.execute(check_query, {"s": target_schema, "t": table_name, "c": col}).scalar()

                if current_type and any(
                    num_type in current_type.lower() for num_type in ["int", "numeric", "double", "real"]
                ):
                    logger.info(
                        "✅ [DB_MANAGER] DDL sync: converting %s.%s from %s to VARCHAR(255)",
                        table_name,
                        col,
                        current_type,
                    )

                    alter_query = text(f"""
                        ALTER TABLE "{target_schema}"."{table_name}"
                        ALTER COLUMN "{col}" TYPE VARCHAR(255)
                        USING "{col}"::VARCHAR
                    """)
                    conn.execute(alter_query)
                    conn.commit()
                    logger.info("✅ [DB_MANAGER] DDL aligned: %s.%s converted to VARCHAR", table_name, col)


def get_all_foreign_keys(db, schema_name):
    """
    Fetches all FK relations for a schema.
    Receives 'db' (DBManager instance) instead of self.
    """
    query = text("""
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
    """)

    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"s": schema_name})
            return [(row[0], row[1], row[2], row[3]) for row in result]
    except Exception as e:
        logger.error("❌ [DB_MANAGER] Error fetching foreign keys: %s", e)
        return []


def render_global_preview_section(db):
    """Persistent live preview panel with SQL context."""

    if "selected_table_info" in st.session_state:
        table_name, schema_name = st.session_state["selected_table_info"]
        current_full_name = f"{schema_name}.{table_name}"

        if st.session_state.get("last_previewed_table") != current_full_name:
            if "current_df" in st.session_state:
                del st.session_state["current_df"]
            st.session_state["last_previewed_table"] = current_full_name

        with st.container():
            p_col1, p_col2 = st.columns([3, 7])

            with p_col1:
                st.write(f"**Current Context:** `{current_full_name}`")

                where_clause = st.session_state.get(f"where_clause_{table_name}", "")

                if where_clause:
                    st.info(f"🔍 **Active Filter:**\n`{where_clause}`")
                    st.caption("Debug SQL Query:")
                    st.code(f"SELECT * FROM {current_full_name} WHERE {where_clause} LIMIT 100;", language="sql")
                else:
                    st.caption("No active filter. Showing top 100 records.")

                if st.button("🔄 Refresh Data", key="global_preview_refresh_btn", width="stretch"):
                    with st.spinner(f"Fetching {table_name}..."):
                        try:
                            df = db.read_table(table_name, schema_name, where=where_clause, limit=100)
                            st.session_state["current_df"] = df
                            st.rerun()
                        except Exception as e:
                            st.error(f"SQL Error: {str(e)}")

            with p_col2:
                if "current_df" in st.session_state:
                    df = st.session_state["current_df"]
                    if df.empty:
                        st.warning("⚠️ This table is empty or no records match your WHERE clause.")
                    else:
                        st.dataframe(
                            df,
                            width="stretch",
                            hide_index=True,
                        )
                        st.caption(f"Showing up to 100 rows from {current_full_name}")
                else:
                    st.info("💡 Data not loaded yet. Click **'Refresh Data'** to fetch a snippet.")
    else:
        st.info("👋 Select a table in the **Explorer** or **Plan** tab to enable live preview here.")
