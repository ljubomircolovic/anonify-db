import streamlit as st
import pandas as pd
from src.database.db_manager import DBManager
from src.engine.anonify_engine import anonymize_dataframe
from src.database.db_manager import DBManager
import os

# Initialize DB Manager
db = DBManager()

st.set_page_config(page_title="AnonifyDB", layout="wide")
st.title("AnonifyDB - Data Engineering Tool")

# --- SIDEBAR: Data Source Selection ---
st.sidebar.header("Data Source Settings")
source_mode = st.sidebar.radio("Select Input Type:", ["CSV Files", "PostgreSQL Database"])

df = None # Initializing the dataframe holder
DATA_FOLDER = "data/input"

if source_mode == "CSV Files":
    st.sidebar.subheader("Local Container Files")

    # Check if directory exists inside the container
    if os.path.exists(DATA_FOLDER):
        # List only CSV files from the container's directory
        files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]

        if files:
            # Create a dropdown menu with available files
            selected_file = st.sidebar.selectbox("Select a file from /data/input:", files)

            # Full path for pandas to read
            file_path = os.path.join(DATA_FOLDER, selected_file)

            if st.sidebar.button("Load Selected File"):
                # Load the file directly into session state
                st.session_state['current_df'] = pd.read_csv(file_path, encoding_errors='replace')
                st.sidebar.success(f"Loaded: {selected_file}")
        else:
            st.sidebar.warning("No CSV files found in /app/data/input")
    else:
        st.sidebar.error(f"Directory not found: {DATA_FOLDER}")
else:
    # --- DATABASE MODE LOGIC ---
    if source_mode == "PostgreSQL Database":
        st.sidebar.subheader("Database Explorer")

        try:
            # 1. Get all schemas (Person, Sales, Production, etc.)
            schemas = db.get_all_schemas()

            # Pre-select 'public' or 'person' if they exist
            default_schema_index = schemas.index('person') if 'person' in schemas else 0
            selected_schema = st.sidebar.selectbox("Choose Schema:", schemas, index=default_schema_index)

            # 2. Get tables for the selected schema
            tables = db.get_tables_in_schema(selected_schema)

            if tables:
                selected_table = st.sidebar.selectbox("Choose Table:", tables)

                if st.sidebar.button("Load Table Data"):
                    with st.spinner(f"Fetching data from {selected_schema}.{selected_table}..."):
                        # Load directly into session state
                        st.session_state['current_df'] = db.read_table(selected_table, selected_schema)
                        st.sidebar.success(f"Loaded {len(st.session_state['current_df'])} rows!")
            else:
                st.sidebar.warning("No tables found in this schema.")

        except Exception as e:
            st.sidebar.error("Could not connect to database.")
            st.sidebar.info("Make sure the 'db' container is running and healthy.")

# --- MAIN CONTENT AREA ---
if 'current_df' in st.session_state:
    working_df = st.session_state['current_df']

    tab1, tab2 = st.tabs(["Original Data", "Anonymized Preview"])

    with tab1:
        st.subheader("Raw Data Preview")
        st.dataframe(working_df.head(100))

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            is_deterministic = st.checkbox("Deterministic Mode", value=True)
        with col2:
            st.write("") # Placeholder for alignment

        if st.button("Run Anonymization Engine"):
            # The same engine you built!
            result = anonymize_dataframe(working_df, is_deterministic=is_deterministic)
            st.session_state['result_df'] = result
            st.rerun()

        if 'result_df' in st.session_state:
            st.subheader("Anonymized Results")
            st.dataframe(st.session_state['result_df'].head(100))

            # Additional DB functionality if in Database mode
            if source_mode == "PostgreSQL Database":
                st.divider()
                st.subheader("Database Export")
                if st.button("Push Anonymized Data to 'anon' Schema"):
                    success = db.save_anonymized_table(
                        st.session_state['result_df'],
                        f"anon_{selected_table}"
                    )
                    if success:
                        st.success(f"Table 'anon_{selected_table}' created successfully!")

# Check if we have results to save
if 'result_df' in st.session_state and source_mode == "PostgreSQL Database":
    st.divider()
    st.subheader("?? Database Export")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        target_table_name = st.text_input("Target Table Name:", f"anon_{selected_table}")
    
    with col2:
        st.write("##") # Spacer
        if st.button("Push to Database", use_container_width=True):
            with st.spinner("Writing to 'anon' schema..."):
                # Call our DBManager to save the data
                success = db.save_anonymized_table(
                    st.session_state['result_df'], 
                    target_table_name,
                    target_schema='anon' # This is where the magic happens
                )
                
                if success:
                    st.success(f"? Success! Table '{target_table_name}' is now in 'anon' schema.")
                    st.balloons()
                else:
                    st.error("? Failed to save data to database.")