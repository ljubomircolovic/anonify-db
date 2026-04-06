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
    if 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']

        # --- 🛡️ NUKLEARNI RESET (Dodaj OVO odmah ovde) ---
        # Proveravamo da li se tabela u memoriji poklapa sa onom koju prikazujemo
        if st.session_state.get('last_rendered_table') != table_name:
            # 1. Brišemo SVE editor state-ove iz session_state-a
            for key in list(st.session_state.keys()):
                if key.startswith("plan_editor_"):
                    del st.session_state[key]

            # 2. Brišemo trenutni plan da bi se učitao svež iz ai_analysis ili baze
            if 'current_plan' in st.session_state:
                del st.session_state['current_plan']

            # 3. Ažuriramo marker tabele
            st.session_state['last_rendered_table'] = table_name

            # 4. Forsiramo osvežavanje pre nego što iscrta bilo šta pogrešno
            st.rerun()

    with st.sidebar:
        if 'db' not in st.session_state:
            st.error("Database connection not initialized in header.")
            return

        db = st.session_state['db']
        # --- SECURITY SEKCIJA ---
        st.subheader("🔑 Security")
        st.session_state['salt_input'] = st.text_input("Secret Salt", value="default_salt", type="password")

        st.session_state['selected_locale'] = st.selectbox(
            "Target Locale",
            options=["de", "us"],
            index=0,
            help="Choose the region for generated names and cities."
        )

        st.divider()

        # --- DATA SOURCE (Tvoja originalna logika) ---
        st.subheader("📂 Data Source")
        source_mode = st.radio("Input Type", ["PostgreSQL Database", "CSV Files"])

        if source_mode == "PostgreSQL Database":
            try:
                schemas = db.get_all_schemas()
                default_idx = schemas.index('ecommerce') if 'ecommerce' in schemas else 0
                selected_schema = st.selectbox("Choose Schema:", schemas, index=default_idx)

                tables = db.get_tables_in_schema(selected_schema)
                if tables:
                    # 1. Selektujemo tabele koje ulaze u proces
                    if 'last_confirmed_tables' not in st.session_state:
                        st.session_state['last_confirmed_tables'] = []

                    selected_tables = st.multiselect(
                        "Choose Tables (Batch Mode):",
                        options=tables,
                        default=st.session_state['last_confirmed_tables'],
                        key='batch_table_selector'
                    )

                    # Resetuj ako se promeni multiselect
            

                    if selected_tables:
                        # Dobijamo ispravan redosled zbog Foreign Keys
                        ordered_tables = db.get_execution_order(selected_tables, selected_schema)
                        st.session_state['all_tables_list'] = ordered_tables

                        # --- 1. VIZUELNI STATUS PROCESA ---
                        status_icons = []
                        for t in ordered_tables:
                            if db.get_saved_plan(selected_schema, t):
                                status_icons.append(f"✅ `{t}`")
                            else:
                                status_icons.append(f"⚠️ `{t}`")

                        st.info(f"⛓️ **Execution Order:** \n{' ➔ '.join(status_icons)}")

                        # --- 2. DUGME ZA START / LOAD (Samo JEDNOM) ---
                        # DODAJEMO KEY DA BUDEMO 100% SIGURNI
                        if st.button("🚀 Start Planning / Load Data", type="primary", use_container_width=True, key="btn_start_planning"):

                            # Određujemo koju tabelu učitavamo
                            if 'selected_table_info' not in st.session_state:
                                first_table = ordered_tables[0]
                                st.session_state['selected_table_info'] = (first_table, selected_schema)

                            current_table, _ = st.session_state['selected_table_info']

                            # --- KOMPLETNO ČIŠĆENJE ---
                            keys_to_clear = [
                                'ai_analysis', 'current_plan', 'plan_snapshot',
                                'plan_origin', 'current_df', 'last_table_for_plan',
                                'last_rendered_table'
                            ]
                            for key in keys_to_clear:
                                if key in st.session_state:
                                    del st.session_state[key]

                            for key in list(st.session_state.keys()):
                                if key.startswith("plan_editor_"):
                                    del st.session_state[key]

                            with st.spinner(f"Switching context to {current_table}..."):
                                st.session_state['current_df'] = db.read_table(current_table, selected_schema, limit=100)
                                saved_p = db.get_saved_plan(selected_schema, current_table)
                                if saved_p:
                                    st.session_state['ai_analysis'] = saved_p
                                    st.session_state['plan_origin'] = 'saved'
                                    st.session_state['plan_snapshot'] = saved_p
                                else:
                                    raw_df_for_ai = db.read_table(current_table, selected_schema, limit=10)
                                    st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df_for_ai, agent, schema_name=selected_schema)
                                    st.session_state['plan_origin'] = 'new'

                                st.session_state['last_table_for_plan'] = current_table
                                st.rerun()

                        # --- 3. BATCH EXECUTION SEKCIJA (Samo JEDNOM) ---
                        st.divider()
                        st.subheader("🔥 Batch Execution")
                        target_schema = st.text_input("Target Schema Name:", value=f"{selected_schema}_anon", key="input_target_schema")

                        # POZIVAMO METODU (Prosleđujemo instance_id)
                        handle_batch_execution(
                            db=db,
                            ordered_tables=ordered_tables,
                            selected_schema=selected_schema,
                            target_schema=target_schema,
                            selected_tables=selected_tables,
                            instance_id=f"sb_{selected_schema}"
                        )


                    else:
                        st.warning("👈 Please select tables to start.")

                else:
                    st.warning("No tables found.")
            except Exception as e:
                st.error(f"DB Error: {e}")