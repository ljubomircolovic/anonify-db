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


            # Check if we already have a plan in the database
            saved_plan = db.get_saved_plan(selected_schema, selected_table)

            if saved_plan:
                st.sidebar.info("?? Saved plan found!")
                if st.sidebar.button("Load Existing Plan"):
                    # We wrap the saved list back into our Pydantic structure
                    from src.agents.privacy_agent import PrivacyAnalysis
                    st.session_state['ai_analysis'] = PrivacyAnalysis(plan=saved_plan)
                    st.sidebar.success("Plan loaded from DB!")

            st.sidebar.divider()

            if st.sidebar.button("?? Run New AI Scan"):
                with st.spinner("Llama 3 is analyzing..."):
                    # 1. Get metadata
                    metadata = db.get_ai_ready_metadata(selected_table, schema=selected_schema)

                    # 2. Get AI analysis
                    res = agent.analyze_metadata(metadata)

                    if res:
                        # Ako je skeniranje uspelo, sa?uvaj u session_state i bazu
                        st.session_state['ai_analysis'] = res

                        clean_plan = [item.dict() for item in res.plan]
                        db.save_ai_plan(selected_schema, selected_table, clean_plan)

                        st.sidebar.success("New scan complete and saved!")
                    else:
                        st.sidebar.error("AI Scan failed. Please try again.")



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

    tab1, tab2, tab3 = st.tabs(["?? Data Preview", "?? AI Anonymizer", "??? Database Explorer"])

    with tab1:
        st.subheader("Raw Data Preview")
        st.dataframe(working_df.head(100))

    with tab2:
        if 'ai_analysis' in st.session_state:
            st.subheader("??? Review & Finalize Anonymization Plan")
            st.info("AI suggested these strategies. You can override them below. If you set strategy to 'none', please provide a reason.")

            # 1. Convert Pydantic plan to a DataFrame for editing
            plan_df = pd.DataFrame([p.dict() for p in st.session_state['ai_analysis'].plan])

            # 2. Setup the Data Editor
            edited_plan_df = st.data_editor(
                plan_df,
                column_config={
                    "column": st.column_config.TextColumn("Database Column", disabled=True),
                    "strategy": st.column_config.SelectboxColumn(
                        "Strategy",
                        options=["hash", "mask", "noise", "none"],
                        required=True,
                    ),
                    "reason": st.column_config.TextColumn("Reasoning / Justification", width="large")
                },
                hide_index=True,
                use_container_width=True,
                key="plan_editor"
            )

            # 3. Settings for the Engine
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                user_salt = st.text_input("Enter Secret Salt:", value="my_secret_key", type="password")

            # 4. Final Run Button
            if st.button("?? Apply Final Configuration & Run"):
                # Check if 'none' strategies have reasons
                incomplete_reasons = edited_plan_df[(edited_plan_df['strategy'] == 'none') & (edited_plan_df['reason'].str.len() < 5)]

                if not incomplete_reasons.empty:
                    st.error("Please provide reasons for 'none' strategy.")
                else:
                    with st.spinner("Executing final anonymization plan..."):
                        # 1. PRVO izvuci info o tabeli
                        table_name, schema_name = st.session_state['selected_table_info']

                        # 2. Pretvori editor u listu diktova
                        final_plan_data = edited_plan_df.to_dict('records')

                        # 3. SACUVAJ u bazu (da zapamti tvoje izmene)
                        db.save_ai_plan(schema_name, table_name, final_plan_data)

                        # 4. Ucitaj podatke i kreni u akciju
                        full_df = db.read_table(table_name, schema_name)

                        # 5. Primeni anonimizaciju
                        anon_df = db.apply_anonymization(full_df, final_plan_data, salt=user_salt)

                        # 6. Snimi anonimizovane podatke u 'anon' semu
                        success = db.save_anonymized_table(anon_df, table_name, target_schema='anon')
        else:
            st.warning("Please run 'AI Privacy Scan' from the sidebar first.")

    with tab3:
        st.subheader("??? Explore Anonymized Database")
        # Izlistaj sve tabele iz 'anon' seme
        anon_tables = db.get_tables_in_schema(schema='anon')

        if anon_tables:
            selected_anon = st.selectbox("Select Anonymized Table:", anon_tables)
            if st.button("View Data"):
                df_anon = db.read_table(selected_anon, schema='anon')
                st.dataframe(df_anon)
        else:
            st.info("No anonymized tables found yet. Run the engine in Tab 2 first!")