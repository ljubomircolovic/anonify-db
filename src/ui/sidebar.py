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
            # Pretvaramo "DB_URL_STAGING" u lepše ime "Staging"Q
            clean_name = key.replace("DB_URL_", "").replace("_", " ").title()
            connections[clean_name] = value

    return connections

# I onda ovo koristiš za selectbox:
DB_CONFIGS = get_all_connections()


def render_sidebar(agent):
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

                    # Resetuj redosled ako se promeni multiselect
                    if selected_tables != st.session_state['last_confirmed_tables']:
                        st.session_state['last_confirmed_tables'] = selected_tables
                        st.rerun()

                    if selected_tables:
                        # Dobijamo ispravan redosled zbog Foreign Keys
                        ordered_tables = db.get_execution_order(selected_tables, selected_schema)
                        
                        # --- KLJUČNA LINIJA ZA AUTOMATIZACIJU ---
                        # Ovo Planner koristi da zna šta je "Next"
                        st.session_state['all_tables_list'] = ordered_tables

                        # 2. Vizuelni status procesa (Samo prikaz, ne može da se menja ovde)
                        status_icons = []
                        for t in ordered_tables:
                            if db.get_saved_plan(selected_schema, t):
                                status_icons.append(f"✅ `{t}`")
                            else:
                                status_icons.append(f"⚠️ `{t}`")
                        
                        st.info(f"⛓️ **Execution Order:** \n{' ➔ '.join(status_icons)}")

                        # 3. Dugme koje te "ubacuje" u prvu neobrađenu tabelu
                        if st.button("🚀 Start Planning / Load Data", type="primary", use_container_width=True):
                            # Ako već nismo negde u procesu, kreni od prve tabele
                            if 'selected_table_info' not in st.session_state:
                                first_table = ordered_tables[0]
                                st.session_state['selected_table_info'] = (first_table, selected_schema)
                            
                            # Učitavamo podatke za trenutno aktivnu tabelu (onu iz session_state)
                            current_table, _ = st.session_state['selected_table_info']
                            
                            with st.spinner(f"Loading {current_table}..."):
                                df = db.read_table(current_table, selected_schema, limit=100)
                                st.session_state['current_df'] = df
                                
                                # Automatski AI Scan ako nema plana, da uštedimo jedan klik
                                if not db.get_saved_plan(selected_schema, current_table):
                                    raw_df_for_ai = db.read_table(current_table, selected_schema, limit=10)
                                    st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df_for_ai, agent, schema_name=selected_schema)
                                
                                st.success(f"Ready! Go to 'Planner' tab to review `{current_table}`.")
                                st.rerun()

                        # --- DINAMIČKI BATCH EXECUTION (Samo dugme za finalni run) ---
                        st.divider()
                        st.subheader("🔥 Batch Execution")
                        target_schema = st.text_input("Target Schema Name:", value=f"{selected_schema}_anon")

                        if st.button("🔥 RUN FULL ANONYMIZATION", type="primary", use_container_width=True):
                            with st.status("Executing Enterprise Pipeline...", expanded=True) as status:
                                try:
                                    full_plan = {}
                                    for t in ordered_tables:
                                        p = db.get_saved_plan(selected_schema, t)
                                        if p: full_plan[t] = p['plan']
                                    
                                    if len(full_plan) < len(selected_tables):
                                        st.error("❌ Missing saved plans! Scan and Save all tables first.")
                                        st.stop()

                                    db.execute_anonymization_batch(selected_schema, target_schema, full_plan)
                                    status.update(label="✅ Success!", state="complete")
                                    st.balloons()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    else:
                        st.warning("👈 Please select tables to start.")
                else:
                    st.warning("No tables found.")
            except Exception as e:
                st.error(f"DB Error: {e}")