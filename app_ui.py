import streamlit as st
import pandas as pd
import os
import sys
import psycopg2
import logging

# 1. Page configuration must be the first Streamlit command
st.set_page_config(page_title="AnonifyDB Dashboard", layout="wide")

# Localization mapping for UI elements
LANG_MAP = {
    "en_US": {
        "metrics_total": "Total Processed",
        "metrics_market": "Target Market",
        "chart_title": "Salary Distribution",
        "db_btn": "?? Export to PostgreSQL",
        "success_msg": "Data successfully saved!"
    },
    "de_DE": {
        "metrics_total": "Gesamt verarbeitet",
        "metrics_market": "Zielmarkt",
        "chart_title": "Gehaltsverteilung",
        "db_btn": "?? Export nach PostgreSQL",
        "success_msg": "Daten erfolgreich gespeichert!"
    }
}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from src.engine.anonify_engine import anonymize_dataframe

def check_db_connection():
    """Check if PostgreSQL container is reachable."""
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
    except:
        return False

# Sidebar configuration
st.sidebar.title("AnonifyDB Settings")
if check_db_connection():
    st.sidebar.success("Database Online")
else:
    st.sidebar.error("Database Offline")

selected_locale = st.sidebar.selectbox("Target Locale", ["en_US", "de_DE"], index=1)
ascii_mode = st.sidebar.toggle("Convert to ASCII", value=True)
is_deterministic = st.sidebar.toggle("Deterministic Mapping", value=True)

# File selection logic
data_dir = "/app/data/input"
available_files = [f for f in os.listdir(data_dir) if f.endswith(('.json', '.csv'))] if os.path.exists(data_dir) else []
selected_file = st.sidebar.selectbox("Choose a source file:", ["None"] + available_files)

st.title("AnonifyDB - Data Engineering Tool")

if 'result_df' not in st.session_state:
    st.session_state.result_df = None

if selected_file != "None":
    file_path = os.path.join(data_dir, selected_file)
    try:
        # Load file with fallback encoding handling
        if file_path.endswith('.json'):
            df = pd.read_json(file_path)
        else:
            df = pd.read_csv(file_path, encoding='utf-8', errors='replace')

        if st.button("Run Anonymization Engine"):
            with st.spinner('Anonymizing data...'):
                st.session_state.result_df = anonymize_dataframe(
                    df,
                    locale=selected_locale,
                    use_ascii=ascii_mode,
                    is_deterministic=is_deterministic
                )
                st.success("Processing complete!")

        # Display dashboard if data exists
        if st.session_state.result_df is not None:
            res_df = st.session_state.result_df
            
            tab1, tab2 = st.tabs(["?? Anonymized Data", "?? Original Data"])
            
            with tab1:
                st.write("### Processed Output (Ready for DB)")
                # WE ONLY SHOW THE ANONYMIZED COLUMNS
                # This prevents seeing the old "160k" next to the new bucket
                display_cols = ['full_name', 'email', 'salary_bucket'] 
                available_display = [c for c in display_cols if c in res_df.columns]
                st.dataframe(res_df[available_display].head(10), use_container_width=True)
            
            with tab2:
                st.write("### Original Input (Source)")
                st.dataframe(df.head(10), use_container_width=True)

            st.markdown("---")
            ui_text = LANG_MAP.get(selected_locale, LANG_MAP["en_US"])
            st.subheader(f"?? {ui_text['chart_title']}")

            # Metrics display
            m_col1, m_col2, m_col3 = st.columns(3)
            m_col1.metric(ui_text["metrics_total"], f"{len(res_df)}")
            m_col2.metric(ui_text["metrics_market"], selected_locale)
            m_col3.metric("DB Status", "Online" if check_db_connection() else "Offline")

            col1, col2 = st.columns(2)
            with col1:
                if st.button(ui_text["db_btn"]):
                    try:
                        conn = psycopg2.connect(host="db", database="anonify_db", user="user", password="password")
                        cursor = conn.cursor()
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS anonymized_data (
                                id SERIAL PRIMARY KEY,
                                full_name TEXT,
                                email TEXT,
                                salary_range TEXT,
                                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            );
                        """)
                        for _, row in res_df.iterrows():
                            # Detect correct salary column
                            s_bucket = row.get('salary_bucket', row.get('salary_status', 'N/A'))
                            cursor.execute("""
                                INSERT INTO anonymized_data (full_name, email, salary_range)
                                VALUES (%s, %s, %s)
                            """, (row['full_name'], row['email'], s_bucket))
                        conn.commit()
                        conn.close()
                        st.success(ui_text["success_msg"])
                    except Exception as e:
                        st.error(f"DB Export Error: {e}")

            with col2:
                # Standardized order for DACH salary buckets
                order = [
                    f"< 50.000 \u20ac",
                    f"50.000 \u20ac - 100.000 \u20ac",
                    f"100.000 \u20ac - 150.000 \u20ac",
                    f"> 150.000 \u20ac"
                ]
                # Dynamic column detection for the chart
                target_col = 'salary_bucket' if 'salary_bucket' in res_df.columns else 'salary_status'
                if target_col in res_df.columns:
                    st.bar_chart(res_df[target_col].value_counts().reindex(order).fillna(0))

    except Exception as e:
        st.error(f"Application Error: {e}")

st.markdown("---")
st.caption("AnonifyDB v1.0 | Data Engineering Portfolio | Built by Ljubomir")