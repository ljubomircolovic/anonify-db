import streamlit as st
import pandas as pd
import os
import sys
import psycopg2

# 1. OVO MORA BITI PRVA STREAMLIT KOMANDA
st.set_page_config(page_title="AnonifyDB Dashboard", layout="wide")

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the core engine logic
from src.engine.anonify_engine import anonymize_dataframe

# --- DATABASE HEALTH CHECK ---
def check_db_connection():
    try:
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

if check_db_connection():
    st.sidebar.success("Database Online")
else:
    st.sidebar.error("Database Offline")

st.sidebar.markdown("---")
st.sidebar.header("Processing Rules")

selected_locale = st.sidebar.selectbox("Target Locale", ["en_US", "de_DE"], index=1)
ascii_mode = st.sidebar.toggle("Convert to ASCII", value=True)
is_deterministic = st.sidebar.toggle("Deterministic Mapping", value=True)

st.sidebar.markdown("---")

# --- FILE SELECTION ---
data_dir = "/app/data/input"
available_files = [f for f in os.listdir(data_dir) if f.endswith(('.json', '.csv'))] if os.path.exists(data_dir) else []
selected_file = st.sidebar.selectbox("Choose a source file:", ["None"] + available_files)

# --- MAIN INTERFACE ---
st.title("AnonifyDB - Data Engineering Tool")

# Initialize result_df in session state so it persists between clicks
if 'result_df' not in st.session_state:
    st.session_state.result_df = None

if selected_file != "None":
    file_path = os.path.join(data_dir, selected_file)

    try:
        if file_path.endswith('.json'):
            df = pd.read_json(file_path)
        else:
            df = pd.read_csv(file_path, encoding='utf-8')

        st.write(f"### Source Preview: `{selected_file}`")
        st.dataframe(df.head(10), use_container_width=True)

        if st.button("Run Anonymization Engine"):
            with st.spinner('Processing data...'):
                try:
                    # Run engine and save to session state
                    st.session_state.result_df = anonymize_dataframe(
                        df,
                        locale=selected_locale,
                        use_ascii=ascii_mode,
                        is_deterministic=is_deterministic
                    )
                    st.success("Successfully processed!")
                except Exception as e:
                    st.error(f"Engine Error: {e}")

        # Show results if they exist in session state
        if st.session_state.result_df is not None:
            res_df = st.session_state.result_df
            
            st.write("### Anonymized Output Preview")
            # DEBUG ISPIS - Sada je na sigurnom mestu
            st.write("DEBUG - Columns generated:", res_df.columns.tolist())
            st.dataframe(res_df.head(10), use_container_width=True)

            # --- DATABASE EXPORT & DASHBOARD ---
            st.markdown("---")
            st.subheader("?? Data Warehouse & Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("?? Export to PostgreSQL"):
                    try:
                        conn = psycopg2.connect(host="db", database="anonify_db", user="user", password="password")
                        cursor = conn.cursor()
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS anonymized_data (
                                id SERIAL PRIMARY KEY,
                                full_name TEXT, email TEXT, salary_range TEXT,
                                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            );
                        """)
                        for _, row in res_df.iterrows():
                            # Koristimo .get() da spre?imo pucanje ako kolona fali
                            s_bucket = row.get('salary_bucket', row.get('salary_status', 'N/A'))
                            cursor.execute("""
                                INSERT INTO anonymized_data (full_name, email, salary_range)
                                VALUES (%s, %s, %s)
                            """, (row['full_name'], row['email'], s_bucket))
                        conn.commit()
                        st.success("Data saved to DB!")
                        conn.close()
                    except Exception as e:
                        st.error(f"DB Error: {e}")

            with col2:
                st.write("Salary Distribution")
                # Proveravamo obe varijante imena kolone
                col_name = 'salary_bucket' if 'salary_bucket' in res_df.columns else 'salary_status'
                if col_name in res_df.columns:
                    st.bar_chart(res_df[col_name].value_counts())
                else:
                    st.warning("No salary column found for chart.")

    except Exception as e:
        st.error(f"Failed to read file: {e}")

st.markdown("---")
st.caption("AnonifyDB v1.0 | Data Engineering Portfolio | Built by Ljubomir")