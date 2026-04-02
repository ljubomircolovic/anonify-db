# -*- coding: utf-8 -*-
import streamlit as st
from src.database.db_manager import DBManager
import time
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
            # Pretvaramo "DB_URL_STAGING" u lepše ime "Staging"
            clean_name = key.replace("DB_URL_", "").replace("_", " ").title()
            connections[clean_name] = value

    return connections

# I onda ovo koristiš za selectbox:
DB_CONFIGS = get_all_connections()


def render_sidebar(agent):
    with st.sidebar:
        st.title("🛡️ AnonifyDB")
        st.caption(f"👤 User: **{st.session_state.get('user_name', 'Admin')}**")
        if st.button("Logout", use_container_width=True):
            st.session_state['authenticated'] = False
            st.rerun()

        st.divider()

        # --- NOVO: CONNECTION MANAGER ---
        st.subheader("🔌 Connection Manager")
        selected_env = st.selectbox("Target Environment", options=list(DB_CONFIGS.keys()))
        current_url = DB_CONFIGS[selected_env]

        # Inicijalizacija DBManager-a (samo ako se promenila baza)
        if 'db' not in st.session_state or st.session_state.get('last_env') != selected_env:
            st.session_state['db'] = DBManager(db_url=current_url)
            st.session_state['last_env'] = selected_env

        db = st.session_state['db']

        if st.button("⚡ Test Connection", use_container_width=True):
            with st.spinner("Checking..."):
                success, message = db.test_connection()
                if success: st.success(message)
                else: st.error(message)

        st.divider()

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
                    selected_tables = st.multiselect(
                        "Choose Tables (Batch Mode):",
                        options=tables,
                        default=st.session_state.get('last_selected_tables', [])
                    )
                    st.session_state['last_selected_tables'] = selected_tables

                    # --- SVA LOGIKA IDE OVDE (AKO SU TABELE IZABRANE) ---
                    if selected_tables:
                        # 1. Redosled izvršavanja
                        ordered_tables = db.get_execution_order(selected_tables, selected_schema)
                        st.info(f"⛓️ **Execution Order:** \n{' ➔ '.join([f'`{t}`' for t in ordered_tables])}")

                        # 2. Selektor za konfiguraciju trenutne tabele
                        selected_table = st.selectbox("Current Table for Analysis:", options=ordered_tables)
                        columns = db.get_columns(selected_table, selected_schema)

                        # 3. Parametri (Filter i Limit)
                        with st.expander("🔍 Filtering & Schema", expanded=False):
                            where_clause = st.text_area("WHERE condition:", placeholder="e.g. id > 100", key=f"where_{selected_table}")
                            limit_val = st.number_input("Limit rows:", value=1000, min_value=1, key=f"limit_{selected_table}")
                            st.info("Available columns:")
                            st.code(", ".join(columns))

                        # 4. Učitavanje starog plana
                        saved_plan_data = db.get_saved_plan(selected_schema, selected_table)
                        if saved_plan_data:
                            if st.button("📂 Load Saved Plan", use_container_width=True):
                                st.session_state['ai_analysis'] = saved_plan_data
                                st.success(f"✅ Plan for {selected_table} loaded!")
                                st.rerun()
                        else:
                            st.caption(f"ℹ️ No saved plan found for {selected_table}.")

                        # 5. Dugme za učitavanje podataka u glavni prozor
                        if st.button("🚀 Load Table Data", type="primary", use_container_width=True):
                            with st.spinner(f"Fetching {selected_table}..."):
                                df = db.read_table(selected_table, selected_schema, where_filter=where_clause, limit=limit_val)
                                st.session_state['current_df'] = df
                                st.session_state['selected_table_info'] = (selected_table, selected_schema)
                                st.success(f"✅ Loaded {len(df)} rows!")

                        st.divider()
                        st.subheader("🤖 Analysis")
                        c1, c2 = st.columns(2)

                        if c1.button("Manual", use_container_width=True):
                            manual_plan = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual"} for c in columns]
                            st.session_state['ai_analysis'] = {"plan": manual_plan}
                            st.rerun()

                        if c2.button("AI Scan", use_container_width=True):
                            with st.spinner("Consulting Database & AI..."):
                                # Čitamo mali uzorak za analizu
                                raw_df = db.read_table(selected_table, selected_schema, limit=10)
                                # Analiza koja prvo gleda 'anon_forced_mappings' u bazi, pa onda pita OpenAI
                                st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df, agent, schema_name=selected_schema)
                                st.rerun()
                    
                    else:
                        st.warning("👈 Please select one or more tables to start.")
                            
                else:
                    st.warning("No tables found in this schema.")
            except Exception as e:
                st.error(f"DB Error: {e}")