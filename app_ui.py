import streamlit as st
import pandas as pd
from src.database.db_manager import DBManager
import os
from src.agents.privacy_agent import PrivacyAgent, PrivacyAnalysis

db = DBManager()
agent = PrivacyAgent()

st.set_page_config(page_title="AnonifyDB", layout="wide")
st.title("AnonifyDB - Data Engineering Tool")

# --- GLOBAL SIDEBAR ---
st.sidebar.header("Global Settings")
current_salt = st.sidebar.text_input("Enter Secret Salt:", value="default_salt", type="password", key="salt_input")

st.sidebar.header("Data Source Settings")
source_mode = st.sidebar.radio("Select Input Type:", ["CSV Files", "PostgreSQL Database"])

if source_mode == "PostgreSQL Database":
    try:
        schemas = db.get_all_schemas()
        selected_schema = st.sidebar.selectbox("Choose Schema:", schemas)
        tables = db.get_tables_in_schema(selected_schema)

        if tables:
            selected_table = st.sidebar.selectbox("Choose Table:", tables)

            # --- LOAD SAVED PLAN LOGIC ---
            saved_plan = db.get_saved_plan(selected_schema, selected_table)
            if saved_plan:
                st.sidebar.info("Found saved plan in DB!")
                if st.sidebar.button("Load Existing Plan"):
                    st.session_state['ai_analysis'] = PrivacyAnalysis(plan=saved_plan)
                    st.sidebar.success("Plan loaded!")

            st.sidebar.divider()

            if st.sidebar.button("Load Table Data"):
                st.session_state['current_df'] = db.read_table(selected_table, selected_schema)
                st.session_state['selected_table_info'] = (selected_table, selected_schema)

            if st.sidebar.button("Run AI Privacy Scan"):
                metadata = db.get_ai_ready_metadata(selected_table, schema=selected_schema)
                st.session_state['ai_analysis'] = agent.analyze_metadata(metadata)
    except Exception as e:
        st.error(f"Database Error: {e}")

# --- MAIN CONTENT ---
if 'current_df' in st.session_state:
    if 'ai_analysis' in st.session_state:
        plan_df = pd.DataFrame([p.dict() for p in st.session_state['ai_analysis'].plan])
        st.write("### Anonymization Plan Editor")
        edited_plan_df = st.data_editor(plan_df, use_container_width=True, key="plan_editor")

        tab1, tab2, tab3 = st.tabs(["Data Explorer", "Execution", "Comparison"])

        with tab1:
            st.dataframe(st.session_state['current_df'].head(100))

        with tab2:
            if st.button("Apply Final Configuration and Run"):
                table_name, schema_name = st.session_state['selected_table_info']
                final_plan = edited_plan_df.to_dict('records')
                full_df = db.read_table(table_name, schema_name)

                anon_df = db.apply_anonymization(full_df, final_plan, salt=current_salt)
                db.save_anonymized_table(anon_df, table_name)
                db.save_ai_plan(schema_name, table_name, final_plan) # SAVE PLAN TO DB
                st.success("Success! Table and Plan saved.")

        with tab3:
            st.subheader("Live Preview")
            raw_sample = st.session_state['current_df'].head(10)
            preview_df = db.apply_anonymization(raw_sample, edited_plan_df.to_dict('records'), salt=current_salt)
            c1, c2 = st.columns(2)
            c1.write("**Original**")
            c1.dataframe(raw_sample)
            c2.write(f"**Anonymized (Salt: {current_salt})**")
            c2.dataframe(preview_df)
    else:
        st.info("Load data and run AI Scan to start.")