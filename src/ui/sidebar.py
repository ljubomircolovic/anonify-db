# -*- coding: utf-8 -*-
import streamlit as st
from src.db import DBManager
from src.ui.source.source_session_init import handle_initialization
import os
from urllib.parse import urlparse


def get_all_connections():
    import os
    connections: dict[str, str] = {}
    env_key_by_label: dict[str, str] = {}

    default = os.getenv("DATABASE_URL")
    if default:
        connections["Local (Default)"] = default
        env_key_by_label["Local (Default)"] = "DATABASE_URL"

    for key, value in os.environ.items():
        if key.startswith("DB_URL_") and value:
            clean_name = key.replace("DB_URL_", "").replace("_", " ").title()
            connections[clean_name] = value
            env_key_by_label[clean_name] = key

    return connections, env_key_by_label


DB_CONFIGS, DB_ENV_KEY_BY_LABEL = get_all_connections()


def render_sidebar(agent, db=None):
    """Sidebar reserved for global navigation/settings."""
    with st.sidebar:
        st.caption("Global navigation and app-wide controls.")


def render_connection_dashboard(
    db,
    metadata_env_url: str = "",
    metadata_message: str = "",
    *,
    source_tooltip: str = "Source: Reading from DATABASE_URL in .env",
    mappings_tooltip: str = "Mappings: Reading from DATABASE_URL in .env",
    export_tooltip: str = "Export: Plan target uses the active metadata connection from DATABASE_URL in .env",
):
    """Three-column horizontal banner showing live status of the three
    data sources the app interacts with:

      1. 📂 Source       — the raw database to anonymize (from DATABASE_URL)
      2. 🧠 Mappings     — the metadata "Brain" schema (formerly Metadata)
      3. 🚀 Export Target — the plan's anonymized target database

    Status is dynamic: 🟢 reflects the most recently confirmed live state
    (source_connected flag, metadata_live_status, plan binding), 🔴 means
    not-yet-connected / unreachable / not-configured.
    """
    source_confirmed = bool(st.session_state.get("source_confirmed"))

    # ---- Source ----------------------------------------------------------
    source_url = str(getattr(db, "source_db_url", "") or "").strip() or str(
        os.getenv("DATABASE_URL", "")
    ).strip()
    source_parsed = urlparse(source_url) if source_url else None
    source_host = (source_parsed.hostname if source_parsed else None) or "—"
    source_db = (
        source_parsed.path.lstrip("/") if source_parsed and source_parsed.path else None
    ) or "—"
    source_ready = bool(st.session_state.get("source_connected"))
    source_dot = "🟢" if source_ready else "🔴"
    source_state = "Connected" if source_ready else "Idle"
    source_hint = "" if source_ready else "Run Initialize Session in the Source tab"
    if not source_confirmed:
        source_hint = "Pending Source — confirm the Source tab configuration"

    # ---- Mappings (Brain) -----------------------------------------------
    metadata_ok_flag = bool(st.session_state.get("metadata_live_status", False))
    metadata_schema = "metadata"  # fixed by DBManager / init_db
    meta_url = str(metadata_env_url or getattr(db, "source_db_url", "") or "").strip()
    meta_parsed = urlparse(meta_url) if meta_url else None
    meta_db = (
        meta_parsed.path.lstrip("/") if meta_parsed and meta_parsed.path else None
    ) or "—"

    if not source_confirmed:
        metadata_dot = "🟡"
        metadata_state = "Pending Source"
        metadata_hint = "Confirm Source in the Source tab before using Mappings."
    else:
        metadata_dot = "🟢" if metadata_ok_flag else "🔴"
        metadata_state = "Connected" if metadata_ok_flag else "Unreachable"
        metadata_hint = "" if metadata_ok_flag else (metadata_message or "Check .env credentials")

    # ---- Export Target ---------------------------------------------------
    plan_name = str(st.session_state.get("active_plan_db_name", "None") or "None")
    connected_plan = str(st.session_state.get("connected_plan_db_name", "None") or "None")
    target_url = str(getattr(db, "target_db_url", "") or "").strip()
    target_parsed = urlparse(target_url) if target_url else None
    target_host = (target_parsed.hostname if target_parsed else None) or "—"
    target_db_display = (
        target_parsed.path.lstrip("/")
        if target_parsed and target_parsed.path
        else plan_name if plan_name not in ("", "None") else "—"
    )
    target_bound = (
        plan_name not in ("", "None")
        and connected_plan == plan_name
        and target_url
        and target_url != (getattr(db, "source_db_url", "") or "")
    )

    if not source_confirmed:
        target_dot = "🟡"
        target_state = "Pending Source"
        target_hint = "Confirm Source in the Source tab before using Export."
    else:
        target_dot = "🟢" if target_bound else "🔴"
        if plan_name in ("", "None"):
            target_state = "Not configured"
            target_hint = "Activate a plan in Mappings"
        elif not target_bound:
            target_state = "Pending"
            target_hint = "Reconnecting to plan database"
        else:
            target_state = "Bound"
            target_hint = ""

    def _status_indicator_line(dot: str, title: str, tooltip: str) -> None:
        """Prefer `st.write(..., help=)` when supported; else `st.markdown(..., help=)`."""
        try:
            st.write(f"{dot} {title}", help=tooltip)
        except TypeError:
            st.markdown(f"{dot} **{title}**", help=tooltip)

    col_a, col_b, col_c = st.columns(3, gap="small")
    with col_a:
        _status_indicator_line(source_dot, "Source", source_tooltip)
        st.caption(f"`{source_db}` @ `{source_host}` · {source_state}")
        if source_hint:
            st.caption(source_hint)
    with col_b:
        _status_indicator_line(metadata_dot, "Mappings", mappings_tooltip)
        st.caption(f"schema `{metadata_schema}` on `{meta_db}` · {metadata_state}")
        if metadata_hint:
            st.caption(metadata_hint)
    with col_c:
        _status_indicator_line(target_dot, "Export", export_tooltip)
        st.caption(f"`{target_db_display}` @ `{target_host}` · {target_state}")
        if target_hint:
            st.caption(target_hint)
        # Source of truth for SQL mirroring / twin schema label (same string as this caption).
        if target_bound and isinstance(target_db_display, str) and target_db_display.strip() not in ("", "—"):
            st.session_state["export_target"] = target_db_display.strip()
        else:
            st.session_state.pop("export_target", None)


def render_metadata_storage_section(db):
    """Mappings (plan) storage inherits the active Source connection; only the brain schema differs."""
    if "plan_metadata" not in st.session_state or not isinstance(st.session_state["plan_metadata"], dict):
        st.session_state["plan_metadata"] = {}

    source_url = str(getattr(db, "source_db_url", "") or "").strip() or str(
        os.getenv("SOURCE_DB_URL") or os.getenv("DATABASE_URL", "")
    ).strip()
    parsed = urlparse(source_url) if source_url else None
    metadata_host = (parsed.hostname if parsed else None) or "unknown-host"
    metadata_db = (parsed.path.lstrip("/") if parsed and parsed.path else "") or "unknown-db"
    brain_schema = "metadata"
    engine_scheme = str(parsed.scheme if parsed else "").lower()
    if "postgres" in engine_scheme:
        metadata_engine = "PostgreSQL"
    elif engine_scheme:
        metadata_engine = engine_scheme.upper()
    else:
        metadata_engine = "Unknown"

    with st.expander("⚙️ Project Storage & Metadata Configuration", expanded=False):
        st.caption(
            "**Connection inheritance:** Mappings do not use a separate database login. "
            "They reuse the **Source** credentials from the Source tab and persist plans under "
            f"the **`{brain_schema}`** schema on that same PostgreSQL instance."
        )
        info_cols = st.columns([2, 2, 2, 1.4], gap="small")
        with info_cols[0]:
            st.caption(f"**📂 Host (from Source):** `{metadata_host}`")
        with info_cols[1]:
            st.caption(f"**🗄️ Database (from Source):** `{metadata_db}`")
        with info_cols[2]:
            st.caption(f"**📐 Mappings schema:** `{brain_schema}`")
        with info_cols[3]:
            if st.button(
                "⚡ Test Metadata Connection",
                key="test_metadata_connection_btn",
                use_container_width=True,
                help="Validates connectivity using the same pool as the Source / Mappings workflow.",
            ):
                success, message = db.test_metadata_connection()
                st.session_state["metadata_test_result"] = ("ok", message) if success else ("fail", message)
            test_result = st.session_state.get("metadata_test_result")
            if isinstance(test_result, tuple):
                if test_result[0] == "ok":
                    st.markdown("✅ **Connection successful!**")
                else:
                    st.error(f"{test_result[1]} Check your .env credentials.")

        st.caption("**Configuration Source:** `.env` (see Command Center header tooltips for variable names)")
        st.caption(f"**Engine:** {metadata_engine}")
        st.info(
            "The **Brain** (`metadata` schema) stores AI plans, audit history, and deterministic mapping catalogs "
            "on the same server you confirm in the Source tab — not on an isolated second connection."
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
    """Main-page data source configuration.

    Independent of plan selection: the user can configure connection details
    and trigger an "Initialize Session" (source scan) with or without an
    active plan. When a plan is also active, ``handle_initialization`` in
    ``source_session_init`` additionally binds the plan to the freshly-scanned source.
    """
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
        st.button(
            "🚀 Initialize Session",
            type="primary",
            use_container_width=True,
            key="sidebar_initialize_session_btn",
            help="Test the source connection and index its schema. No plan required.",
            on_click=lambda: handle_initialization(db),
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