import streamlit as st
import pandas as pd
import os
import sys
import psycopg2

# Add the root directory to sys.path to ensure 'src' is discoverable as a package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the core transformation logic from your engine
from src.engine.anonify_engine import anonymize_dataframe

# Page configuration for the Streamlit Web UI
st.set_page_config(page_title="AnonifyDB UI", layout="wide")

# --- DATABASE HEALTH CHECK ---
def check_db_connection():
    """
    Attempts to connect to the PostgreSQL service defined in docker-compose.
    Returns True if successful, False otherwise.
    """
    try:
        # 'host="db"' maps to the container name in the Docker network
        conn = psycopg2.connect(
            host="db",
            database="anonify_db",
            user="user",
            password="password",
            connect_timeout=2
        )
        conn.close()
        return True
    except Exception:
        return False

# --- SIDEBAR: STATUS & CONFIGURATION ---
st.sidebar.title("System Status")

# Real-time Database connectivity indicator
if check_db_connection():
    st.sidebar.success("? Database Online")
else:
    st.sidebar.error("? Database Offline")

# Define path for local data input (mapped via Docker volumes)
data_dir = "/app/data/input"

# List all supported files in the input directory
if os.path.exists(data_dir):
    available_files = [f for f in os.listdir(data_dir) if f.endswith(('.json', '.csv'))]
else:
    available_files = []

selected_file = st.sidebar.selectbox("Select file from /data/input:", ["None"] + available_files)

# --- MAIN INTERFACE ---
st.title("AnonifyDB - Data Engineering Tool")
st.markdown("---")

df = None

if selected_file != "None":
    file_path = os.path.join(data_dir, selected_file)

    try:
        # Load logic based on file extension
        if file_path.endswith('.json'):
            df = pd.read_json(file_path)
        else:
            # ROBUST CSV LOADING: Handles encoding issues like the 0x9e byte error
            try:
                # Standard UTF-8 attempt
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # Fallback to ISO-8859-1 for ANSI/Excel-exported files
                df = pd.read_csv(file_path, encoding='iso-8859-1')

        # UI Preview of the source data
        st.write(f"### Raw Data Preview: {selected_file}")
        st.dataframe(df.head(10), use_container_width=True)

        # Execution trigger
        if st.button("?? Process & Anonymize"):
            with st.spinner('Engine is processing data...'):
                try:
                    # Execute the anonymization logic from src/engine/anonify_engine.py
                    result_df = anonymize_dataframe(df)

                    st.success("Transformation Complete!")

                    # UI Preview of the processed data
                    st.write("### Anonymized Data Preview")
                    st.dataframe(result_df.head(10), use_container_width=True)

                    # Prepare the processed data for user download
                    output_filename = f"anon_{selected_file.replace('.json', '.csv')}"
                    csv_output = result_df.to_csv(index=False).encode('utf-8')

                    st.download_button(
                        label="?? Download Results (CSV)",
                        data=csv_output,
                        file_name=output_filename,
                        mime="text/csv"
                    )
                except Exception as e:
                    st.error(f"Engine Processing Error: {e}")

    except Exception as e:
        st.error(f"File Access Error: {e}")
else:
    st.info("Select a file from the sidebar to begin processing.")

# Application Footer
st.caption("AnonifyDB Framework | Developed for Secure Data Engineering")