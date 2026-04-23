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

if 'selected_table_info' in st.session_state:
    render_tabs(db)
else:
    hour = datetime.datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"
    user = st.session_state.get('user_name', 'User')

    st.info(f"👋 {greeting}, {user}! Please select a table from the sidebar to start.")