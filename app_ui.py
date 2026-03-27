import streamlit as st
import pandas as pd
from src.database.db_manager import DBManager
from sqlalchemy import text
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
# --- 1. Provera da li ve? postoji plan u bazi ---
            saved_plan = db.get_saved_plan(selected_schema, selected_table)
            if saved_plan:
                if st.sidebar.button(" Load Saved Plan"):
                    from src.agents.privacy_agent import PrivacyAnalysis
                    st.session_state['ai_analysis'] = PrivacyAnalysis(plan=saved_plan)
                    st.sidebar.success("Loaded existing plan from DB!")

            st.sidebar.divider()


            col_btn1, col_btn2 = st.sidebar.columns(2)

            if st.sidebar.button("Load Table Data"):
                st.session_state['current_df'] = db.read_table(selected_table, selected_schema)
                st.session_state['selected_table_info'] = (selected_table, selected_schema)

            st.sidebar.divider()

            # --- 2. Manual i AI opcije jedna pored druge ---
            if col_btn1.button("Manual"):

                with db.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT * FROM \"{selected_schema}\".\"{selected_table}\" LIMIT 0"))
                    columns = result.keys()

                manual_plan = [{"column": c, "strategy": "keep", "confidence": 1.0, "reason": "Manual"} for c in columns]

                from src.agents.privacy_agent import PrivacyAnalysis
                st.session_state['ai_analysis'] = PrivacyAnalysis(plan=manual_plan)
                st.sidebar.success("Columns loaded!")

            if col_btn2.button("AI Scan"):
                with st.spinner("AI is analyzing..."):
                    metadata = db.get_ai_ready_metadata(selected_table, schema=selected_schema)
                    st.session_state['ai_analysis'] = agent.analyze_metadata(metadata)
                    st.sidebar.success("AI analysis complete!")



    except Exception as e:
        st.error(f"Database Error: {e}")

# --- MAIN CONTENT ---
if 'current_df' in st.session_state:
    if 'ai_analysis' in st.session_state:
        plan_df = pd.DataFrame([p.dict() for p in st.session_state['ai_analysis'].plan])

        st.write("### Anonymization Plan Editor")

        column_config = {
            "column": st.column_config.TextColumn("Database Column", disabled=True),
            "strategy": st.column_config.SelectboxColumn(
                "Anonymization Strategy",
                help="Select how to protect this column",
                width="medium",
                options=[
                    "keep",
                    "hash",
                    "mask",
                    "synthetic",
                    "noise",
                    "date_shift"
                ],
                required=True,
            )
        }


        table_name, schema_name = st.session_state['selected_table_info']
        unique_key = f"editor_{schema_name}_{table_name}"


        edited_plan_df = st.data_editor(
            plan_df,
            column_config=column_config,
            use_container_width=True,
            key=unique_key,  # <--- Ovo je klju?na promena
            hide_index=True
        )



        tab1, tab2, tab3 = st.tabs(["Data Explorer", "Execution", "Comparison"])

        with tab1:
            st.dataframe(st.session_state['current_df'].head(100))

        with tab2:
            if 'ai_analysis' in st.session_state:
                st.subheader("Review & Finalize Plan")

                plan_df = pd.DataFrame([p.dict() for p in st.session_state['ai_analysis'].plan])
                edited_plan_df = st.data_editor(
                    plan_df,
                    column_config={
                        "column": st.column_config.TextColumn("Database Column", disabled=True),
                        "strategy": st.column_config.SelectboxColumn(
                            "Strategy",
                            options=["keep", "mask", "hash", "noise", "synthetic", "date_shift"],
                            required=True
                        )
                    },
                    hide_index=True,
                    use_container_width=True,
                    key="plan_editor"
                )

                st.divider()
                col_exec, col_save = st.columns(2)

                with col_exec:
                    if st.button("Run Anonymization", use_container_width=True):
                        with st.spinner("Processing data..."):
                            table_name, schema_name = st.session_state['selected_table_info']
                            final_plan_data = edited_plan_df.to_dict('records')

                            full_df = db.read_table(table_name, schema_name)

                            anon_df, notes = db.apply_anonymization(full_df, final_plan_data, salt=current_salt)

                            st.session_state['last_run_notes'] = list(set(notes))

                            db.save_anonymized_table(anon_df, table_name, target_schema='anon')
                            st.success(f"Data processed and saved to 'anon.{table_name}'")

            if 'last_run_notes' in st.session_state:
                for n in st.session_state['last_run_notes']:
                    st.info(n)

                with col_save:
                    if st.button("Save/Update Plan in DB", use_container_width=True, type="primary"):
                        with st.spinner("Saving configuration..."):
                            table_name, schema_name = st.session_state['selected_table_info']
                            final_plan_data = edited_plan_df.to_dict('records')

                            db.save_ai_plan(schema_name, table_name, final_plan_data)
                            st.success(f"Configuration for '{table_name}' successfully updated in DB!")
            else:
                st.warning("Please load columns (Manual or AI Scan) first.")



        with tab3:
            st.subheader("Comparison View")

            current_salt = st.session_state.get('salt_input', 'default_salt')

            if 'ai_analysis' in st.session_state and 'selected_table_info' in st.session_state:
                current_plan_data = edited_plan_df.to_dict('records')
                table_name, schema_name = st.session_state['selected_table_info']

                # Uzimamo uzorak za prikaz (Top 10)
                raw_sample = db.read_table(table_name, schema_name).head(10)

                if not raw_sample.empty:
                    # --- IZMENA 1: Otpakivanje (df, notes) ---
                    anonymized_sample, comparison_notes = db.apply_anonymization(raw_sample, current_plan_data, salt=current_salt)

                    # --- IZMENA 2: Prikaz notifikacija o integritetu ---
                    if comparison_notes:
                        for n in set(comparison_notes):
                            st.info(n)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Original Data**")
                        st.dataframe(raw_sample)
                    with col2:
                        st.write(f"**Anonymized (Salt: {current_salt})**")
                        # Sada je anonymized_sample ?ist DataFrame
                        st.dataframe(anonymized_sample)

                    # --- NOVO: EXPORT SEKCIJA ---
                    st.divider()
                    st.subheader("Export Result")

                    # Dugme za procesiranje CELE tabele za download (ne samo head(10))
                    if st.button("Prepare Full Download (CSV)"):
                        with st.spinner("Generating full anonymized file..."):
                            full_df = db.read_table(table_name, schema_name)
                            # --- IZMENA 3: Ponovo otpakivanje za punu tabelu ---
                            full_anon, _ = db.apply_anonymization(full_df, current_plan_data, salt=current_salt)

                            csv = full_anon.to_csv(index=False).encode('utf-8')

                            st.download_button(
                                label="Download Anonymized CSV",
                                data=csv,
                                file_name=f"anon_{table_name}.csv",
                                mime="text/csv",
                            )





    else:
        st.info("Load data and run AI Scan to start.")