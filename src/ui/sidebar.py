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
                default_idx = schemas.index('person') if 'person' in schemas else 0
                selected_schema = st.selectbox("Choose Schema:", schemas, index=default_idx)

                tables = db.get_tables_in_schema(selected_schema)
                if tables:
                    selected_table = st.selectbox("Choose Table:", tables)
                    columns = db.get_columns(selected_table, selected_schema)

                    with st.expander("🔍 Filtering & Schema", expanded=False):
                        # SQL Injection Safe napomena
                        where_clause = st.text_area("WHERE condition:", placeholder="e.g. id > 100")
                        limit_val = st.number_input("Limit rows:", value=1000, min_value=1)
                        st.info("Available columns:")
                        st.code(", ".join(columns))

                    # --- LOAD SAVED PLAN ---
                    saved_plan_data = db.get_saved_plan(selected_schema, selected_table)
                    if saved_plan_data:
                        if st.button("📂 Load Saved Plan", use_container_width=True):
                            st.session_state['ai_analysis'] = saved_plan_data
                            st.success("✅ Saved plan loaded!")
                            st.rerun()
                    else:
                        st.caption("ℹ️ No saved plan found for this table.")

                    # --- LOAD DATA ---
                    if st.button("🚀 Load Table Data", type="primary", use_container_width=True):
                        with st.spinner("Fetching data..."):
                            # Koristimo novu read_table sa zaštitom
                            df = db.read_table(selected_table, selected_schema, where_filter=where_clause, limit=limit_val)
                            st.session_state['current_df'] = df
                            st.session_state['selected_table_info'] = (selected_table, selected_schema)
                            st.session_state['last_where_filter'] = where_clause
                            st.session_state['last_limit_val'] = limit_val
                            st.success(f"✅ Loaded {len(df)} rows!")

                    st.divider()
                    st.subheader("🤖 Analysis")
                    c1, c2 = st.columns(2)

                    if c1.button("Manual", use_container_width=True):
                        manual_plan = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual"} for c in columns]
                        st.session_state['ai_analysis'] = {"plan": manual_plan}
                        st.rerun()

                    if c2.button("AI Scan", use_container_width=True):
                        with st.spinner("AI analyzing via Azure..."):
                            raw_df = db.read_table(selected_table, selected_schema).head(10)
                            # Ovde 'agent' sada koristi Azure OpenAI jer smo ga podesili u DBManager-u
                            st.session_state['ai_analysis'] = agent.analyze_metadata(raw_df.to_dict(orient='records'))
                            st.rerun()
                else:
                    st.warning("No tables found in this schema.")
            except Exception as e:
                st.error(f"DB Error: {e}")

        st.divider()

        # --- SYSTEM INTEGRITY LOGOVI (Tvoja originalna logika) ---
        with st.expander("🩺 System Integrity", expanded=False):
            if 'init_logs' in st.session_state:
                for log in st.session_state['init_logs']:
                    if "⏩" in log: st.markdown(f"**{log}**")
                    elif "🔄" in log: st.markdown(f":blue[{log}]")
                    elif "✅" in log: st.markdown(f":green[{log}]")
                    else: st.write(log)