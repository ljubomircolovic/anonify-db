# -*- coding: utf-8 -*-
import streamlit as st
import time

def render_sidebar(db, agent):
    with st.sidebar:
        st.title("🛡️ AnonifyDB")
        st.caption(f"👤 User: **{st.session_state.get('user_name', 'Admin')}**")
        if st.button("Logout", use_container_width=True):
            st.session_state['authenticated'] = False
            st.rerun()

        st.divider()
        st.subheader("🔑 Security")
        st.session_state['salt_input'] = st.text_input("Secret Salt", value="default_salt", type="password")

        st.session_state['selected_locale'] = st.selectbox(
            "Target Locale",
            options=["de", "us"],
            index=0,
            help="Choose the region for generated names and cities."
        )

        st.divider()
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

                    # Pomeramo definicije filtera ovde da bi dugme dole uvek videlo vrednosti
                    with st.expander("🔍 Filtering & Schema", expanded=False):
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
                        with st.spinner("AI analyzing..."):
                            raw_df = db.read_table(selected_table, selected_schema).head(10)
                            st.session_state['ai_analysis'] = agent.analyze_metadata(raw_df.to_dict(orient='records'))
                            st.rerun()
                else:
                    st.warning("No tables found in this schema.")
            except Exception as e:
                st.error(f"DB Error: {e}")

        st.divider()

        # --- PRIKAZ LOGOVA INTEGRITETA ---

        with st.expander("🩺 System Integrity", expanded=False):
            if 'init_logs' in st.session_state:
                for log in st.session_state['init_logs']:
                    # Umesto st.caption, koristimo st.markdown za bolju kontrolu
                    if "⏩" in log:
                        # Koristimo standardni tekst, bez :gray, da bi bio oštar
                        st.markdown(f"**{log}**")
                    elif "🔄" in log:
                        # Plava boja za promene (uočljivije)
                        st.markdown(f":blue[{log}]")
                    elif "✅" in log:
                        # Zelena za kraj (kao i do sada, ali oštrije)
                        st.markdown(f":green[{log}]")
                    else:
                        st.write(log)