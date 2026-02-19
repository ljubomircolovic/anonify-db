import streamlit as st
import pandas as pd
import os
import sys
import psycopg2

# Add the project root to sys.path to ensure modules in 'src' are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the core engine logic
from src.engine.anonify_engine import anonymize_dataframe

# Basic Streamlit page configuration
st.set_page_config(page_title="AnonifyDB Dashboard", layout="wide")

# --- DATABASE HEALTH CHECK ---
def check_db_connection():
    """
    Check if the PostgreSQL database is reachable.
    Uses credentials compatible with the Docker environment.
    """
    try:
        # 'db' is the hostname of the database container in docker-compose
        conn = psycopg2.connect(
            host="db",
            database="anonify_db",
            user="user",
            password="password",
            connect_timeout=1
        )
        conn.close()
        return True
    except Exception:
        return False

# --- SIDEBAR: SYSTEM STATUS & CONFIGURATION ---
st.sidebar.title("AnonifyDB Settings")

# Database Status Indicator
if check_db_connection():
    st.sidebar.success("Database Online")
else:
    st.sidebar.error("Database Offline")

st.sidebar.markdown("---")

# --- OPTION B: ANONYMIZATION RULES ---
st.sidebar.header("Processing Rules")

# Select the Faker locale (localization for fake names)
selected_locale = st.sidebar.selectbox(
    "Target Locale",
    ["en_US", "de_DE"],
    index=1,
    help="Select the region for synthetic data generation. Using Latin script for Serbian."
)

# Toggle for ASCII conversion
ascii_mode = st.sidebar.toggle(
    "Convert to ASCII",
    value=True,
    help="Remove special characters (e.g., accents) from generated names."
)

# Toggle for Deterministic vs Random mapping
is_deterministic = st.sidebar.toggle(
    "Deterministic Mapping",
    value=True,
    help="If ON, the same ID will always result in the same fake name."
)

st.sidebar.markdown("---")

# --- FILE SELECTION ---
data_dir = "/app/data/input"
available_files = [f for f in os.listdir(data_dir) if f.endswith(('.json', '.csv'))] if os.path.exists(data_dir) else []
selected_file = st.sidebar.selectbox("Choose a source file:", ["None"] + available_files)

# --- MAIN INTERFACE ---
st.title("AnonifyDB - Data Engineering Tool")
st.info("Upload or select a file to preview and anonymize sensitive data.")

if selected_file != "None":
    file_path = os.path.join(data_dir, selected_file)

    try:
        # Load the file with encoding fallback for robust CSV handling
        if file_path.endswith('.json'):
            df = pd.read_json(file_path)
        else:
            try:
                # Primary attempt with standard UTF-8
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # Fallback for ANSI/Excel/Windows encoded files
                df = pd.read_csv(file_path, encoding='iso-8859-1')

        # Display Source Data Preview
        st.write(f"### Source Preview: `{selected_file}`")
        st.dataframe(df.head(10), use_container_width=True)

        # Trigger the Anonymization Engine
        if st.button("Run Anonymization Engine"):
            with st.spinner('Processing data according to rules...'):
                try:
                    # Execute transformation using settings from the sidebar
                    result_df = anonymize_dataframe(
                        df,
                        locale=selected_locale,
                        use_ascii=ascii_mode,
                        is_deterministic=is_deterministic
                    )

                    st.success(f"Successfully processed! (Mode: {'Deterministic' if is_deterministic else 'Randomized'})")

                    # Display Resulting Data Preview
                    st.write("### Anonymized Output Preview")
                    st.dataframe(result_df.head(10), use_container_width=True)

                    # Generate Download Link
                    output_filename = f"anon_{selected_file.replace('.json', '.csv')}"
                    csv_data = result_df.to_csv(index=False).encode('utf-8')

                    st.download_button(
                        label="Download Resulting CSV",
                        data=csv_data,
                        file_name=output_filename,
                        mime="text/csv"
                    )
                except Exception as e:
                    st.error(f"Engine Error: {e}")

    except Exception as e:
        st.error(f"Failed to read file: {e}")
else:
    st.warning("Please select a file from the sidebar to begin.")

# Application Footer
st.markdown("---")
st.caption("AnonifyDB v1.0 | Data Engineering Portfolio | Built by Ljubomir")