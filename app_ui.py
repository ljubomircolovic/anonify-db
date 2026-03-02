import streamlit as st
import pandas as pd
from src.database.db_manager import DBManager
import os
from src.agents.privacy_agent import PrivacyAgent

# 1. Initialize Objects
db = DBManager()
agent = PrivacyAgent()

st.set_page_config(page_title="AnonifyDB", layout="wide")
st.title("AnonifyDB - Data Engineering Tool")

# --- SIDEBAR: Data Source Selection ---
st.sidebar.header("Data Source Settings")
source_mode = st.sidebar.radio("Select Input Type:", ["CSV Files", "PostgreSQL Database"])

DATA_FOLDER = "data/input"

if source_mode == "CSV Files":
    st.sidebar.subheader("Local Container Files")

    if os.path.exists(DATA_FOLDER):
        files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]

        if files:
            selected_file = st.sidebar.selectbox("Select a file from /data/input:", files)
            file_path = os.path.join(DATA_FOLDER, selected_file)

            if st.sidebar.button("Load Selected File"):
                st.session_state['current_df'] = pd.read_csv(file_path, encoding_errors='replace')
                st.sidebar.success(f"Loaded: {selected_file}")
        else:
            st.sidebar.warning("No CSV files found in /app/data/input")
    else:
        st.sidebar.error(f"Directory not found: {DATA_FOLDER}")

else:
    # --- DATABASE MODE LOGIC ---
    st.sidebar.subheader("Database Explorer")

    try:
        # Fetch available schemas
        schemas = db.get_all_schemas()
        default_index = schemas.index('person') if 'person' in schemas else 0
        selected_schema = st.sidebar.selectbox("Choose Schema:", schemas, index=default_index)

        # Fetch tables for the selected schema
        tables = db.get_tables_in_schema(selected_schema)

        if tables:
            selected_table = st.sidebar.selectbox("Choose Table:", tables)

            # Execution logic moved inside button to avoid Scope errors
            if st.sidebar.button("Load Table Data"):
                with st.spinner(f"Fetching data from {selected_schema}.{selected_table}..."):
                    st.session_state['current_df'] = db.read_table(selected_table, selected_schema)
                    st.session_state['selected_table_info'] = (selected_table, selected_schema)
                    st.sidebar.success(f"Loaded {len(st.session_state['current_df'])} rows!")

            # Trigger AI Privacy Analysis
            if st.sidebar.button("?? AI Privacy Scan"):
                with st.spinner("Llama 3 is analyzing data privacy..."):
                    # Extract vertically shuffled and masked metadata
                    metadata = db.get_ai_ready_metadata(selected_table, schema=selected_schema)

                    # Analyze via PrivacyAgent
                    analysis_result = agent.analyze_metadata(metadata)

                    if analysis_result:
                        # Store Pydantic object for the Engine to use
                        st.session_state['ai_analysis'] = analysis_result
                    else:
                        st.sidebar.error("AI Agent could not complete the analysis.")
        else:
            st.sidebar.warning("No tables found in this schema.")

    except Exception as e:
        st.error(f"? Connection/Logic Error: {e}")
        st.sidebar.error("Connectivity issue detected.")

# --- MAIN CONTENT AREA ---
if 'current_df' in st.session_state:
    working_df = st.session_state['current_df']

    tab1, tab2 = st.tabs(["Original Data", "AI-Guided Anonymization"])

    with tab1:
        st.subheader("Raw Data Preview")
        st.dataframe(working_df.head(100))

    with tab2:
        if 'ai_analysis' in st.session_state:
            st.subheader("?? AI Privacy Recommendations (Llama 3)")

            # Display Recommendations Table
            plan_table = [
                {"Column": item.column, "Strategy": item.strategy.upper(), "Reason": item.reason}
                for item in st.session_state['ai_analysis'].plan
            ]
            st.table(plan_table)

            st.subheader("Anonymization Settings")
            user_salt = st.text_input("Enter Secret Salt (for consistency):", value="my_secret_key", type="password")

            # Trigger the actual Anonymization Engine based on AI Plan
            if st.button("?? Run Anonymization Engine"):
                with st.spinner("Processing data..."):
                    table_name, schema_name = st.session_state['selected_table_info']

                    # 1. Fetch fresh data
                    full_df = db.read_table(table_name, schema_name)

                    # 2. Convert Pydantic plan to dictionary list
                    plan_data = [p.dict() for p in st.session_state['ai_analysis'].plan]

                    # 3. Transform data using DBManager logic
                    anon_df = db.apply_anonymization(full_df, plan_data, salt=user_salt)

                    # 4. Save to database in 'anon' schema
                    success = db.save_anonymized_table(anon_df, table_name, target_schema='anon')

                    if success:
                        st.success(f"Success! Data saved to schema 'anon', table '{table_name}'")
                        st.subheader("Anonymized Result Preview")
                        st.dataframe(anon_df.head(50))
                    else:
                        st.error("Failed to save anonymized data to database.")
        else:
            st.info("Please run the 'AI Privacy Scan' from the sidebar to generate a plan.")