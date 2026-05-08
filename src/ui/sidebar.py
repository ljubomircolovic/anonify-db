# -*- coding: utf-8 -*-
import streamlit as st
from src.database.db_manager import DBManager
import os
from urllib.parse import urlparse


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


def render_sidebar(agent, db=None):
    """Sidebar reserved for global navigation/settings."""
    with st.sidebar:
        st.caption("Global navigation and app-wide controls.")


def render_metadata_storage_section(db):
    """Central workflow metadata storage configuration."""
    if "plan_metadata" not in st.session_state or not isinstance(st.session_state["plan_metadata"], dict):
        st.session_state["plan_metadata"] = {}

    metadata_url = str(
        st.session_state["plan_metadata"].get("source_db_connection")
        or st.session_state["plan_metadata"].get("db_connection")
        or getattr(db, "source_db_url", "")
        or ""
    ).strip()
    parsed = urlparse(metadata_url) if metadata_url else None
    metadata_host = (parsed.hostname if parsed else None) or "unknown-host"
    metadata_db = (parsed.path.lstrip("/") if parsed and parsed.path else "") or "unknown-db"
    metadata_schema = str(st.session_state.get("selected_schema", "public")).strip() or "public"
    engine_scheme = str(parsed.scheme if parsed else "").lower()
    if "postgres" in engine_scheme:
        metadata_engine = "PostgreSQL"
    elif engine_scheme:
        metadata_engine = engine_scheme.upper()
    else:
        metadata_engine = "Unknown"

    with st.expander("⚙️ Project Storage & Metadata Configuration", expanded=False):
        info_cols = st.columns([2, 2, 2, 1.4], gap="small")
        with info_cols[0]:
            st.caption(f"**📂 Metadata Host:** `{metadata_host}`")
        with info_cols[1]:
            st.caption(f"**🗄️ Database:** `{metadata_db}`")
        with info_cols[2]:
            st.caption(f"**📐 Schema:** `{metadata_schema}`")
        with info_cols[3]:
            if st.button(
                "⚡ Test Metadata Connection",
                key="test_metadata_connection_btn",
                use_container_width=True,
                help="Validates metadata credentials loaded from .env.sh.",
            ):
                success, message = db.test_metadata_connection()
                st.session_state["metadata_test_result"] = ("ok", message) if success else ("fail", message)
            test_result = st.session_state.get("metadata_test_result")
            if isinstance(test_result, tuple):
                if test_result[0] == "ok":
                    st.markdown("✅ **Connection successful!**")
                else:
                    st.error(f"{test_result[1]} Check your .env.sh credentials.")

        st.caption("**Configuration Source:** `.env.sh`")
        st.caption(f"**Metadata Engine:** {metadata_engine}")
        st.info(
            "About Metadata Database: `.env.sh` provides credentials to the metadata PostgreSQL instance "
            "(the 'Brain'). This database is isolated from target data and preserves anonymization "
            "intelligence across execution environments."
        )
        st.markdown("**Current Metadata Schema**")
        st.markdown(
            "**`metadata.ai_plans`**: Stores LLM-generated suggestions and AI-optimized anonymization strategies.\n\n"
            "**`metadata.audit_log`**: Detailed record of all system activities, schema changes, and execution history for compliance.\n\n"
            "**`metadata.global_id_mapping`**: Cross-reference table for maintaining consistent IDs across different datasets (Deterministic Anonymization).\n\n"
            "**`metadata.mapping_catalog`**: Central registry of all active data masking rules and their associated metadata.\n\n"
            "**`metadata.mapping_values`**: Dictionary of pre-defined substitution values used for categorical data masking.\n\n"
            "**`metadata.pending_fks`**: Queue of discovered Foreign Key relationships that require manual validation or AI confirmation.\n\n"
            "**`metadata.plans`**: The main repository for user-defined, executable anonymization plans."
        )


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

    col1, col2, col3 = st.columns([1, 1, 1.2])
    with col1:
        db_type = st.selectbox(
            "DB Type",
            options=["PostgreSQL", "MySQL", "SQL Server"],
            key="data_source_database_type",
        )
    with col2:
        domain_type = st.selectbox(
            "Domain",
            options=["Customer Data", "Financial Records", "E-commerce", "Healthcare", "Custom"],
            key="data_source_domain_type",
        )
    with col3:
        st.markdown('<div style="padding-top: 28px;"></div>', unsafe_allow_html=True)
        if st.button(
            "🚀 Initialize Session",
            type="primary",
            use_container_width=True,
            key="sidebar_initialize_session_btn",
            help="Click to connect and load metadata",
        ):
            st.session_state["trigger_session_initialize"] = True

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
    st.caption("Initialize Session validates connectivity and indexes metadata.")

    source_url = str(getattr(db, "source_db_url", "") or "").strip()
    if not source_url:
        source_url = str(os.getenv("DATABASE_URL", "")).strip()
    source_parsed = urlparse(source_url) if source_url else None
    source_host = (source_parsed.hostname if source_parsed else None) or "unknown-host"
    source_db_name = (source_parsed.path.lstrip("/") if source_parsed and source_parsed.path else "") or "unknown-db"
    source_schema = "public"
    source_catalog = db.get_source_schema_catalog(schema_name=source_schema)

    with st.expander("📚 Current Source Schema", expanded=False):
        st.caption(f"**📂 Source Host:** `{source_host}`")
        st.caption(f"**🗄️ Database:** `{source_db_name}`")
        st.caption("**📐 Schema:** `public`")
        if source_catalog:
            for table_row in source_catalog:
                table_name = str(table_row.get("table_name", "")).strip()
                description = str(table_row.get("description", "No description available")).strip() or "No description available"
                st.markdown(f"- `{table_name}`: {description}")
        else:
            st.warning("No tables discovered in `public` schema or catalog access failed.")