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
                    # --- FIX: Rešavamo Double-Click bug ---
                    if 'last_confirmed_tables' not in st.session_state:
                        st.session_state['last_confirmed_tables'] = []

                    selected_tables = st.multiselect(
                        "Choose Tables (Batch Mode):",
                        options=tables,
                        default=st.session_state['last_confirmed_tables'],
                        key='batch_table_selector'
                    )

                    # Ako se selekcija promenila, odmah osvežavamo Execution Order
                    if selected_tables != st.session_state['last_confirmed_tables']:
                        st.session_state['last_confirmed_tables'] = selected_tables
                        st.rerun()

                    if selected_tables:
                        # 1. Dinamički redosled izvršavanja
                        ordered_tables = db.get_execution_order(selected_tables, selected_schema)

                        
                        status_icons = []
                        for t in ordered_tables:
                            if db.get_saved_plan(selected_schema, t):
                                status_icons.append(f"✅ `{t}`")
                            else:
                                status_icons.append(f"⚠️ `{t}`")
                        
                        st.info(f"⛓️ **Execution Order & Status:** \n{' ➔ '.join(status_icons)}")
                        
                        if any("⚠️" in icon for icon in status_icons):
                            st.warning("Napomena: Tabele sa ⚠️ nemaju sačuvan plan. AI Scan-uj ih i klikni 'Save Plan in DB' pre Batch-a.")


                        # 2. Selektor za trenutnu tabelu (za analizu)
                        selected_table = st.selectbox("Current Table for Analysis:", options=ordered_tables)
                        columns = db.get_columns(selected_table, selected_schema)

                        with st.expander("🔍 Filtering & Schema", expanded=False):
                            where_clause = st.text_area("WHERE condition:", placeholder="e.g. id > 100", key=f"where_{selected_table}")
                            limit_val = st.number_input("Limit rows:", value=1000, min_value=1, key=f"limit_{selected_table}")
                            st.code(", ".join(columns))

                        # 3. Učitavanje/Skeniranje plana
                        saved_plan_data = db.get_saved_plan(selected_schema, selected_table)
                        if saved_plan_data:
                            if st.button("📂 Load Saved Plan", use_container_width=True):
                                st.session_state['ai_analysis'] = saved_plan_data
                                st.success(f"✅ Plan for {selected_table} loaded!")
                                st.rerun()
                        
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
                            st.session_state['ai_analysis'] = {"plan": [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual"} for c in columns]}
                            st.rerun()
                        if c2.button("AI Scan", use_container_width=True):
                            with st.spinner("Consulting AI..."):
                                raw_df = db.read_table(selected_table, selected_schema, limit=10)
                                st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df, agent, schema_name=selected_schema)
                                st.rerun()

                        # --- DINAMIČKI BATCH EXECUTION ---
                        st.divider()
                        st.subheader("🚀 Batch Execution")
                        target_schema = st.text_input("Target Schema Name:", value=f"{selected_schema}_anon")

                        if st.button("🔥 RUN FULL ANONYMIZATION", type="primary", use_container_width=True):
                            with st.status("Executing Enterprise Pipeline...", expanded=True) as status:
                                try:
                                    # Faza 0: Clean Wipe
                                    status.write(f"🗑️ Dropping schema `{target_schema}`...")
                                    db.drop_target_schema(target_schema)
                                    
                                    # Faza 1: Skupljanje planova iz baze
                                    status.write(f"📂 Collecting plans for {len(selected_tables)} tables...")
                                    full_plan = {}
                                    for t in ordered_tables:
                                        p = db.get_saved_plan(selected_schema, t)
                                        if p: full_plan[t] = p['plan']
                                    
                                    if len(full_plan) < len(selected_tables):
                                        st.error("❌ Missing saved plans! Please Scan and Save plans for all tables.")
                                        st.stop()

                                    # Faza 2: Izvršavanje
                                    db.execute_anonymization_batch(selected_schema, target_schema, full_plan)
                                    
                                    status.update(label="✅ Success!", state="complete")
                                    st.balloons()
                                    st.success(f"Database anonymized in `{target_schema}`")
                                except Exception as e:
                                    status.update(label="❌ Failed", state="error")
                                    st.error(f"Error: {str(e)}")
                    else:
                        st.warning("👈 Please select tables to start.")
                    st.subheader("")
                    st.subheader("")
                else:
                    st.warning("No tables found in this schema.")
            except Exception as e:
                st.error(f"DB Error: {e}")