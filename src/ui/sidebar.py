# -*- coding: utf-8 -*-
import streamlit as st
from src.database.db_manager import DBManager
import time
import os
from src.ui.batch_processor import handle_batch_execution


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
    with st.sidebar:
        if 'db' not in st.session_state:
            st.error("Database connection not initialized.")
            return

        db = st.session_state['db']

        # --- SECURITY SEKCIJA ---
        st.subheader("🔑 Security")
        st.session_state['salt_input'] = st.text_input("Secret Salt", value="default_salt", type="password")
        st.session_state['selected_locale'] = st.selectbox("Target Locale", options=["de", "us"], index=0)

        st.divider()

        # --- DATA SOURCE ---
        st.subheader("📂 Data Source")
        source_mode = st.radio("Input Type", ["PostgreSQL Database", "CSV Files"])

        if source_mode == "PostgreSQL Database":
            try:
                schemas = db.get_all_schemas()
                # Koristimo key='selected_schema' da bi bio dostupan u celoj aplikaciji
                selected_schema = st.selectbox("Choose Schema:", schemas, index=0, key='selected_schema')

                tables = db.get_tables_in_schema(selected_schema)
                if tables:
                    # Multiselect za tabele
                    selected_tables = st.multiselect(
                        "Choose Tables (Batch Mode):",
                        options=tables,
                        default=st.session_state.get('last_confirmed_tables', []),
                        key='batch_table_selector'
                    )

                    # Reset ako se promeni selekcija tabela
                    if selected_tables != st.session_state.get('last_confirmed_tables'):
                        st.session_state['completed_tables'] = set()
                        st.session_state['last_confirmed_tables'] = selected_tables
                        st.session_state['all_tables_list'] = db.get_execution_order(selected_tables, selected_schema)

                    if selected_tables:
                        ordered_tables = st.session_state.get('all_tables_list', [])

                        # --- VIZUELNI STATUS (OVO JE DOBRO) ---
                        status_icons = []
                        for t in ordered_tables:
                            if t in st.session_state.get('completed_tables', set()):
                                status_icons.append(f"✅ `{t}`")
                            else:
                                # Provera u bazi ako nije u session_state
                                if db.get_saved_plan(selected_schema, t):
                                    status_icons.append(f"✅ `{t}`")
                                    if 'completed_tables' not in st.session_state: st.session_state['completed_tables'] = set()
                                    st.session_state['completed_tables'].add(t)
                                else:
                                    status_icons.append(f"⏳ `{t}`")

                        st.info(f"⛓️ **Execution Order:** \n{' ➔ '.join(status_icons)}")

                        # --- DUGME ZA START ---
                        if st.button("🚀 Start Planning / Load", type="primary", width="stretch"):
                            # Postavljamo na prvu tabelu SAMO kad se klikne Start
                            first_table = ordered_tables[0]
                            st.session_state['selected_table_info'] = (first_table, selected_schema)

                            # Čistimo stare podatke da bi Planner učitao sveže
                            for key in ['ai_analysis', 'current_plan', 'plan_snapshot', 'last_rendered_table']:
                                if key in st.session_state: del st.session_state[key]

                            st.rerun()

                    else:
                        st.warning("👈 Please select tables.")
            except Exception as e:
                st.error(f"DB Error: {e}")