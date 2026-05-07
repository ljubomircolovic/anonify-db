# -*- coding: utf-8 -*-
import streamlit as st
from src.database.db_manager import DBManager
import os


def get_all_connections():
    import os
    connections = {}

    # Prvo uzimamo default bazu
    default = os.getenv("DATABASE_URL")
    if default:
        connections["Local (Default)"] = default

    # Zatim prolazimo kroz SVE environment varijable
    for key, value in os.environ.items():
        if key.startswith("DB_URL_"):
            # Pretvaramo "DB_URL_STAGING" u lepše ime "Staging"Q
            clean_name = key.replace("DB_URL_", "").replace("_", " ").title()
            connections[clean_name] = value

    return connections

# I onda ovo koristiš za selectbox:
DB_CONFIGS = get_all_connections()


def render_sidebar(agent):
    """Sidebar intentionally minimal during plan bootstrapping."""
    with st.sidebar:
        st.empty()


def render_data_source_section(db):
    """Main-page data source configuration, shown after plan initialization."""
    with st.expander("🔌 Connection Settings", expanded=False):
        conn_col1, conn_col2 = st.columns(2)
        with conn_col1:
            st.text_input("Host", key="conn_host", placeholder="localhost")
            st.text_input("Database Name", key="conn_database_name", placeholder="anonify_db")
            st.text_input("User", key="conn_user", placeholder="postgres")
        with conn_col2:
            st.text_input("Port", key="conn_port", placeholder="5432")
            st.text_input("Password", key="conn_password", type="password", placeholder="••••••••")
            st.empty()

    st.markdown("### 🧠 Intelligence Context")

    col1, col2 = st.columns([1, 1])
    with col1:
        db_type = st.selectbox(
            "Database Type",
            options=["PostgreSQL", "MySQL", "SQL Server"],
            key="data_source_database_type",
        )
    with col2:
        domain_type = st.selectbox(
            "Data Domain/Type",
            options=["Customer Data", "Financial Records", "E-commerce", "Healthcare", "Custom"],
            key="data_source_domain_type",
        )

    st.session_state["db_config"] = {
        "database_type": db_type,
        "data_domain": domain_type,
        "connection": {
            "host": st.session_state.get("conn_host", ""),
            "port": st.session_state.get("conn_port", ""),
            "database_name": st.session_state.get("conn_database_name", ""),
            "user": st.session_state.get("conn_user", ""),
            "password": st.session_state.get("conn_password", ""),
        },
    }
    if "plan_metadata" not in st.session_state or not isinstance(st.session_state["plan_metadata"], dict):
        st.session_state["plan_metadata"] = {}
    st.session_state["data_domain"] = domain_type
    st.session_state["plan_metadata"]["data_domain"] = domain_type
    st.session_state["plan_metadata"]["database_type"] = db_type
    if st.button("🔗 Connect to Source", type="primary", use_container_width=True, key="connect_to_source_btn"):
        try:
            schemas = db.get_all_schemas()
            selected_schema = st.session_state.get("selected_schema")
            if not selected_schema and schemas:
                selected_schema = schemas[0]
                st.session_state["selected_schema"] = selected_schema
            tables = db.get_tables_in_schema(selected_schema) if selected_schema else []
            st.session_state["last_confirmed_tables"] = tables
            st.session_state["all_tables_list"] = db.get_execution_order(tables, selected_schema) if selected_schema else []
            st.success("Source connection context loaded.")
        except Exception as e:
            st.error(f"DB Error: {e}")