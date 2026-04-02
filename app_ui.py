# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd  # Dodato jer ti treba za Audit log dole
from dotenv import load_dotenv

# Importi tvojih modula
from src.database.db_manager import DBManager
from src.agents.privacy_agent import PrivacyAgent
from src.ui.auth import check_login
from src.ui.sidebar import render_sidebar
from src.ui.tabs_content import render_tabs # Ovo je sada tvoj glavni "motor" za tabove
from init_db import initialize_metadata

# 1. Setup
load_dotenv()
st.set_page_config(page_title="AnonifyDB", layout="wide")

# 2. Provera Autentifikacije
if not check_login():
    st.stop()

# 3. Inicijalizacija Backend-a (samo jednom)
db = DBManager()
agent = PrivacyAgent()

if 'db_initialized' not in st.session_state:
    st.session_state['init_logs'] = initialize_metadata()
    st.session_state['db_initialized'] = True

# 4. Pozivamo modularni Sidebar
# On puni session_state (selected_table, salt, current_df...)
render_sidebar(agent)

# 5. Glavni sadržaj (Modularni Tabovi)
st.title("AnonifyDB obzirom da cesto menjamo k- Data Engineering Tool")

if 'selected_table_info' in st.session_state:
    # Pozivamo jednu funkciju koja u sebi sadrži svu logiku za tabove
    render_tabs(db)
else:
    st.info("👋 Welcome, Ljubomir! Please select a table from the sidebar and click 'Load Table Data' to start.")