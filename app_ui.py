# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

# Importi tvojih modula
from src.database.db_manager import DBManager
from src.agents.privacy_agent import PrivacyAgent
from src.ui.auth import check_login
from src.ui.sidebar import render_sidebar
from src.ui.tabs_content import render_tabs
from init_db import initialize_metadata
import datetime

# --- 1. SETUP & AUTH ---
load_dotenv()
st.set_page_config(page_title="AnonifyDB", layout="wide")

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
    st.session_state['db'] = DBManager(db_url=current_url)
    st.session_state['last_env'] = selected_env

db = st.session_state['db']

if st.session_state.get("active_plan_db_name") and st.session_state.get("last_env") == selected_env:
    try:
        db.connect_to_existing_plan_database(st.session_state["active_plan_db_name"])
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
    st.info("✨ **Create New Anonymization Plan**")
    st.caption(
        "Enter a descriptive name (e.g., 'GDPR Production Prep'). "
        "This will create a dedicated database: `anon_{db}_{plan}`."
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
    with row1_left:
        plan_name = st.text_input(
            "Plan Name",
            value=st.session_state.get("plan_name", ""),
            placeholder="e.g. GDPR Production Prep"
        ).strip()
    with row1_right:
        initialize_clicked = st.button("🚀 Initialize Project", type="primary", use_container_width=True)

    st.session_state["plan_name"] = plan_name
    existing_plan_dbs = db.list_existing_plan_databases()

    row2_left, row2_right = st.columns([5, 2], gap="small", vertical_alignment="bottom")
    with row2_left:
        selected_existing_plan = st.selectbox(
            "Existing Plan Databases",
            options=[""] + existing_plan_dbs,
            index=0,
            help="Reuse an existing plan database instead of creating a new one."
        )
    with row2_right:
        continue_existing_clicked = st.button("🔁 Continue with Existing", use_container_width=True)

plan_key = f"{selected_env}:{plan_name}" if plan_name else None

if continue_existing_clicked:
    if not selected_existing_plan:
        st.error("Select an existing plan database first.")
    else:
        try:
            db.connect_to_existing_plan_database(selected_existing_plan)
            st.session_state["active_plan_db_name"] = selected_existing_plan
            st.session_state["active_plan_db_key"] = f"{selected_env}:{selected_existing_plan}"
            st.session_state["project_initialized"] = True
            st.success(f"Connected to existing plan database: `{selected_existing_plan}`")
        except Exception as exc:
            st.session_state["project_initialized"] = False
            st.error(f"Failed to connect to existing plan database: {exc}")

if initialize_clicked:
    if not plan_name:
        st.error("Please enter a valid plan name before initializing the project.")
    else:
        try:
            created_db_name, created_now = db.bootstrap_plan_database(plan_name)
            st.session_state["active_plan_db_key"] = f"{selected_env}:{created_db_name}"
            st.session_state["active_plan_db_name"] = created_db_name
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

if not is_initialized and not plan_name:
    st.warning("Enter a Plan Name and click 'Initialize Project' before scanning.")
elif not is_initialized:
    st.info("Initialize the project to continue to scanning and planning.")

if 'selected_table_info' in st.session_state:
    if is_initialized:
        render_tabs(db)
    else:
        st.info("Plan workflow is locked until the project is initialized.")
else:
    hour = datetime.datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"
    user = st.session_state.get('user_name', 'User')

    st.info(f"👋 {greeting}, {user}! Please select a table from the sidebar to start.")