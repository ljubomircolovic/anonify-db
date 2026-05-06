# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import logging
from dotenv import load_dotenv

# Importi tvojih modula
from src.database.db_manager import DBManager
from src.adapters.legacy.db_manager_adapter import DBManagerAdapter
from src.agents.privacy_agent import PrivacyAgent
from src.ui.auth import check_login
from src.ui.sidebar import render_sidebar
from src.ui.tabs_content import render_tabs
from init_db import initialize_metadata
import datetime
logger = logging.getLogger(__name__)

# --- 1. SETUP & AUTH ---
load_dotenv()
st.set_page_config(page_title="AnonifyDB", layout="wide")


@st.cache_resource(show_spinner=False)
def get_cached_db_manager(db_url: str, session_scope: str):
    """
    Returns one DBManager instance per (session, db_url) scope.
    Using cache_resource avoids recreating connection pools on reruns.
    """
    return DBManager(db_url=db_url)


@st.cache_resource(show_spinner=False)
def get_cached_db_adapter(db_url: str, session_scope: str):
    """
    Returns one DBManagerAdapter per (session, db_url) scope.
    Adapter delegates to DBManager so behavior remains unchanged.
    """
    manager = get_cached_db_manager(db_url=db_url, session_scope=session_scope)
    return DBManagerAdapter(manager)

if not check_login():
    st.stop()

# --- 2. COMPACT TOP BAR ---
top_col1, top_col2, top_col3 = st.columns([2, 2, 1])
top_col1.markdown(f"👤 **{st.session_state.get('user_name', 'Admin')}**")

from src.ui.sidebar import DB_CONFIGS
selected_env = top_col2.selectbox(
    "🔌 Target Environment",
    options=list(DB_CONFIGS.keys()),
    label_visibility="collapsed"
)

if 'db' not in st.session_state or st.session_state.get('last_env') != selected_env:
    current_url = DB_CONFIGS[selected_env]
    if 'session_scope' not in st.session_state:
        st.session_state['session_scope'] = f"session-{os.urandom(8).hex()}"
    st.session_state['db'] = get_cached_db_adapter(
        db_url=current_url,
        session_scope=st.session_state['session_scope']
    )
    st.session_state['last_env'] = selected_env

db = st.session_state['db']

if (
    st.session_state.get("active_plan_db_name")
    and st.session_state.get("last_env") == selected_env
    and st.session_state.get("connected_plan_db_name") != st.session_state.get("active_plan_db_name")
):
    try:
        db.connect_to_existing_plan_database(st.session_state["active_plan_db_name"])
        st.session_state["connected_plan_db_name"] = st.session_state["active_plan_db_name"]
    except Exception:
        st.session_state["project_initialized"] = False

if top_col3.button("Logout", width="stretch"):
    st.session_state['authenticated'] = False
    st.rerun()

if top_col3.button("⚡ Test", width="stretch"):
    success, message = db.test_connection()
    if success:
        st.success(message)
    else:
        st.error(message)

# --- 3. INICIJALIZACIJA (State Management) ---
if 'agent' not in st.session_state:
    st.session_state['agent'] = PrivacyAgent()

if 'db_initialized' not in st.session_state:
    st.session_state['init_logs'] = initialize_metadata()
    st.session_state['db_initialized'] = True

# --- NOVO: Inicijalizacija Navigacione Istorije ---
if 'navigation_history' not in st.session_state:
    st.session_state['navigation_history'] = []
if 'history_pointer' not in st.session_state:
    st.session_state['history_pointer'] = -1

# Lokalni aliasi
db = st.session_state['db']
agent = st.session_state['agent']

# --- 4. UI: SIDEBAR ---
render_sidebar(agent)

# --- 5. UI: GLAVNI SADRŽAJ ---
st.title("🛡️ AnonifyDB Data Engineering Tool")
with st.container():
    # --- Single Dynamic Status Layer (Top) ---
    hour = datetime.datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"
    user = st.session_state.get('user_name', 'User')
    st.markdown(f"👋 **{greeting}, {user}!**")

    selected_tables_hint = st.session_state.get("selected_tables") or st.session_state.get("last_confirmed_tables") or []
    current_is_initialized = (
        bool(st.session_state.get("active_plan_db_key"))
        and st.session_state.get("project_initialized", False)
        and str(st.session_state.get("active_plan_db_key", "")).startswith(f"{selected_env}:")
    )
    if not current_is_initialized:
        if not selected_tables_hint:
            st.info("👋 Welcome! Start by selecting tables from the sidebar.")
        else:
            st.success(f"✅ {len(selected_tables_hint)} tables selected. Please provide a Plan Name or select an Existing Plan below.")

    # --- Instruction Layer (Middle) ---
    st.markdown("### Plan Selection")
    st.caption(
        "Enter a descriptive name (e.g., 'GDPR Production Prep') or continue with an existing plan database."
    )
    st.markdown(
        """
        <style>
            div.stButton > button {
                white-space: nowrap;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

    row1_left, row1_right = st.columns([5, 2], gap="small", vertical_alignment="bottom")
    schema_hint = st.session_state.get("selected_schema", "schema")
    suggested_plan_name = f"plan_{schema_hint}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"

    if "selected_existing_plan" not in st.session_state:
        st.session_state["selected_existing_plan"] = ""

    def _on_plan_name_change():
        if str(st.session_state.get("plan_name", "")).strip():
            st.session_state["selected_existing_plan"] = ""

    def _on_existing_plan_change():
        if str(st.session_state.get("selected_existing_plan", "")).strip():
            st.session_state["plan_name"] = ""

    with row1_left:
        existing_selected_now = bool(str(st.session_state.get("selected_existing_plan", "")).strip())
        st.text_input(
            "Plan Name",
            key="plan_name",
            placeholder=suggested_plan_name,
            on_change=_on_plan_name_change,
            disabled=existing_selected_now
        )
        plan_name = str(st.session_state.get("plan_name", "")).strip()
    with row1_right:
        selected_existing_value = str(st.session_state.get("selected_existing_plan", "")).strip()
        initialize_clicked = st.button(
            "🚀 Initialize Project",
            type="primary",
            use_container_width=True,
            disabled=(not bool(plan_name)) or bool(selected_existing_value)
        )
    existing_plan_dbs = db.list_existing_plan_databases()

    row2_left, row2_right = st.columns([5, 2], gap="small", vertical_alignment="bottom")
    with row2_left:
        plan_name_present_now = bool(str(st.session_state.get("plan_name", "")).strip())
        st.selectbox(
            "Existing Plan Databases",
            options=[""] + existing_plan_dbs,
            key="selected_existing_plan",
            help="Reuse an existing plan database instead of creating a new one.",
            on_change=_on_existing_plan_change,
            disabled=plan_name_present_now
        )
        selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()
    with row2_right:
        continue_existing_clicked = st.button(
            "🔁 Continue with Existing",
            use_container_width=True,
            disabled=bool(plan_name) or (not bool(selected_existing_plan))
        )

plan_key = f"{selected_env}:{plan_name}" if plan_name else None

if continue_existing_clicked:
    if not selected_existing_plan:
        st.error("Select an existing plan database first.")
    else:
        try:
            db.connect_to_existing_plan_database(selected_existing_plan)
            st.session_state["active_plan_db_name"] = selected_existing_plan
            st.session_state["active_plan_db_key"] = f"{selected_env}:{selected_existing_plan}"
            st.session_state["connected_plan_db_name"] = selected_existing_plan
            st.session_state["project_initialized"] = True
            st.success(f"Connected to existing plan database: `{selected_existing_plan}`")
        except Exception as exc:
            st.session_state["project_initialized"] = False
            st.error(f"Failed to connect to existing plan database: {exc}")

if initialize_clicked:
    selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()
    exactly_one_source = bool(plan_name) ^ bool(selected_existing_plan)
    if not exactly_one_source:
        st.error("Provide either Plan Name or Existing Plan Database (exactly one).")
    else:
        try:
            if selected_existing_plan:
                db.connect_to_existing_plan_database(selected_existing_plan)
                st.session_state["active_plan_db_key"] = f"{selected_env}:{selected_existing_plan}"
                st.session_state["active_plan_db_name"] = selected_existing_plan
                st.session_state["connected_plan_db_name"] = selected_existing_plan
                st.session_state["project_initialized"] = True
                st.success(f"Connected to existing plan database: `{selected_existing_plan}`")
            else:
                created_db_name, created_now = db.bootstrap_plan_database(plan_name)
                st.session_state["active_plan_db_key"] = f"{selected_env}:{created_db_name}"
                st.session_state["active_plan_db_name"] = created_db_name
                st.session_state["connected_plan_db_name"] = created_db_name
                st.session_state["project_initialized"] = True
                if created_now:
                    st.success(f"Project initialized. Using plan database: `{created_db_name}`")
                else:
                    st.info(
                        f"Plan database already existed. Connected to existing database: `{created_db_name}`"
                    )
        except Exception as exc:
            st.session_state["project_initialized"] = False
            error_message = str(exc).lower()
            if "permission denied" in error_message or "createdb" in error_message:
                st.error(
                    "Cannot create a plan database because this user lacks CREATEDB permission. "
                    "Ask your PostgreSQL admin to grant CREATEDB or run with an admin account."
                )
            else:
                st.error(f"Failed to initialize plan database: {exc}")

is_initialized = (
    bool(st.session_state.get("active_plan_db_key"))
    and st.session_state.get("project_initialized", False)
    and str(st.session_state.get("active_plan_db_key", "")).startswith(f"{selected_env}:")
)

selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()

ordered_tables = st.session_state.get("all_tables_list", []) or st.session_state.get("selected_tables", [])
start_enabled = bool(is_initialized and ordered_tables)
if is_initialized:
    st.markdown("---")
    if st.button("🚀 Start Planning / Load", type="primary", use_container_width=True, disabled=not start_enabled):
        selected_schema = st.session_state.get("selected_schema", "public")
        if not ordered_tables:
            st.error("Please select tables in the sidebar before starting planning.")
            st.stop()
        logger.info("[DB_MANAGER] Initializing planning session...")
        for t_name in ordered_tables:
            db.ensure_plan_security_metadata(selected_schema, t_name)
        st.session_state.pop('multi_ai_analysis', None)
        st.session_state['all_tables_list'] = ordered_tables
        st.session_state['selected_tables'] = ordered_tables
        st.session_state['selected_table_info'] = (ordered_tables[0], selected_schema)
        st.session_state['plan_active'] = False
        st.session_state['planning_initialized'] = True
        for key in ['ai_analysis', 'current_plan', 'plan_snapshot', 'last_rendered_table']:
            if key in st.session_state:
                del st.session_state[key]
        logger.info("[DB_MANAGER] Planning session initialized. Awaiting explicit AI scan trigger.")
        st.rerun()

if 'selected_table_info' in st.session_state:
    if is_initialized:
        render_tabs(db)
    else:
        st.info("Plan workflow is locked until the project is initialized.")