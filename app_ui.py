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

# --- 1. SETUP & AUTH ---
load_dotenv()
st.set_page_config(page_title="AnonifyDB", layout="wide")

if not check_login():
    st.stop()

# --- 2. INICIJALIZACIJA (State Management) ---
# Inicijalizujemo Agenta i DB samo jednom da ne trošimo resurse
if 'agent' not in st.session_state:
    st.session_state['agent'] = PrivacyAgent()

if 'db' not in st.session_state:
    st.session_state['db'] = DBManager()

if 'db_initialized' not in st.session_state:
    st.session_state['init_logs'] = initialize_metadata()
    st.session_state['db_initialized'] = True

# Lokalni aliasi radi lakšeg korišćenja
db = st.session_state['db']
agent = st.session_state['agent']

# --- 3. UI: SIDEBAR ---
# Sada 'agent' i 'db' garantovano postoje pre ovog poziva
render_sidebar(agent)

# --- 4. UI: GLAVNI SADRŽAJ ---
st.title("🛡️ AnonifyDB Data Engineering Tool")

if 'selected_table_info' in st.session_state:
    # Prosleđujemo 'db' instancu tabovima
    render_tabs(db)
else:
    st.info("👋 Welcome, Ljubomir! Please select a table from the sidebar and click 'Load Table Data' to start.")