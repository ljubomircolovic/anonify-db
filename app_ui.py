# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from src.database.db_manager import DBManager
from sqlalchemy import text
import os
import json
from src.agents.privacy_agent import PrivacyAgent, PrivacyAnalysis

# Inicijalizacija
if 'active_tab' not in st.session_state:
    st.session_state['active_tab'] = 0

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
        # Postavljamo public kao default
        default_idx = schemas.index('public') if 'public' in schemas else 0
        selected_schema = st.sidebar.selectbox("Choose Schema:", schemas, index=default_idx)

        tables = db.get_tables_in_schema(selected_schema)

        if tables:
            selected_table = st.sidebar.selectbox("Choose Table:", tables)

            # 1. Provera sačuvanog plana
            saved_plan_data = db.get_saved_plan(selected_schema, selected_table)
            if saved_plan_data:
                if st.sidebar.button("📂 Load Saved Plan"):
                    try:
                        # Ako je sačuvani plan u bazi, učitavamo ga
                        st.session_state['ai_analysis'] = saved_plan_data
                        st.session_state['active_tab'] = 1
                        st.sidebar.success("✅ Plan loaded! Switching to Editor...")
                        st.rerun()
                    except Exception as e:
                        st.sidebar.error(f"❌ Error parsing saved plan: {e}")


            st.sidebar.divider()
            st.sidebar.markdown("🔍 **Data Filtering**")

            # Novo: Expander za filtere
            with st.sidebar.expander("Filter Options", expanded=False):
                where_clause = st.text_area(
                    "WHERE condition:",
                    placeholder="e.g. status = 'active' AND created_at > '2025-01-01'",
                    help="Omit the 'WHERE' keyword. Just write the condition."
                )
                limit_val = st.number_input("Limit rows:", min_value=1, max_value=100000, value=1000)

            # Izmenjeno dugme (sada prosleđuje where_filter i limit)
            if st.sidebar.button("Load Table Data", width="stretch", type="primary"):
                with st.sidebar.spinner("Fetching data from DB..."):
                    try:
                        df = db.read_table(
                            selected_table,
                            selected_schema,
                            where_filter=where_clause,
                            limit=limit_val
                        )
                        st.session_state['current_df'] = df
                        st.session_state['selected_table_info'] = (selected_table, selected_schema)
                        st.sidebar.success(f"✅ Loaded {len(df)} rows!")
                    except Exception as e:
                        st.sidebar.error(f"SQL Error: {e}")






            # --- Anonymization Analysis Sekcija ---
            st.sidebar.divider()
            st.sidebar.markdown(
                "### Anonymization Analysis",
                help="Select manual column loading or run AI scan to determine the best anonymization strategies."
            )

            col_btn1, col_btn2 = st.sidebar.columns(2)

            # Kreiramo prazan prostor odmah ispod kolona za dugmiće
            status_placeholder = st.sidebar.empty()

            # MANUAL DUGME
            if col_btn1.button("Manual", width="stretch"):
                try:
                    columns = db.get_columns(selected_table, selected_schema)
                    manual_plan = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual load"} for c in columns]
                    st.session_state['ai_analysis'] = {"plan": manual_plan}

                    # Prikazujemo poruku direktno ispod
                    status_placeholder.success(
                        "✅ Columns loaded!  \n"
                        "Please adapt columns in  \n"
                        "🛠️ 'Execution & Plan' tab."
                    )
                    import time
                    time.sleep(7) # Čekamo 5 sekundi
                    status_placeholder.empty() # Brišemo poruku

                except Exception as e:
                    st.sidebar.error(f"Error: {e}")

            # AI SCAN DUGME
            if col_btn2.button("AI Scan", width="stretch"):
                with st.sidebar.spinner("AI is analyzing..."):
                    try:
                        raw_df = db.read_table(selected_table, selected_schema).head(10)
                        metadata = raw_df.to_dict(orient='records')
                        ai_result = agent.analyze_metadata(metadata)

                        if ai_result:
                            st.session_state['ai_analysis'] = ai_result
                            status_placeholder.info("🚀 AI Scan complete!")
                            import time
                            time.sleep(3)
                            status_placeholder.empty()
                    except Exception as e:
                        st.sidebar.error(f"AI Error: {e}")



        else:
            st.sidebar.warning("No tables found.")

    except Exception as e:
        st.sidebar.error(f"Database Error: {e}")







# --- MAIN CONTENT ---
if 'current_df' in st.session_state:
    tab_list = ["📊 Data Explorer", "🛠️ Execution & Plan", "🔍 Comparison", "📜 Audit Log"]
    # Napomena: st.tabs nema programski index, ali st.rerun() pomaže u održavanju stanja
    tabs = st.tabs(tab_list)

    with tabs[0]:
        st.subheader(f"Raw Data: {st.session_state['selected_table_info'][0]}")
        st.dataframe(st.session_state['current_df'].head(100), width="stretch")

    with tabs[1]:
        if 'ai_analysis' in st.session_state:
            st.subheader("🛠️ Review & Finalize Plan")

            # Unifikacija podataka (Pydantic vs Dict)
            analysis_data = st.session_state['ai_analysis']
            if hasattr(analysis_data, 'plan'):
                plan_list = [p.model_dump() for p in analysis_data.plan]
            elif isinstance(analysis_data, dict) and 'plan' in analysis_data:
                plan_list = analysis_data['plan']
            else:
                plan_list = []

            plan_df = pd.DataFrame(plan_list)

            if not plan_df.empty:
                if 'is_pii' in plan_df.columns:
                    pii_detected = plan_df[plan_df['is_pii'] == True]['column'].tolist()
                    if pii_detected:
                        st.warning(f"⚠️ **PII Detected:** {', '.join(pii_detected)}. Verify strategies!")

                edited_plan_df = st.data_editor(
                    plan_df,
                    column_config={
                        "column": st.column_config.TextColumn("Database Column", disabled=True),
                        "is_pii": st.column_config.CheckboxColumn("PII Detected", disabled=True),
                        "strategy": st.column_config.SelectboxColumn(
                            "Strategy",
                            options=["keep", "hash", "mask", "synthetic", "noise", "date_shift"],
                            required=True
                        )
                    },
                    hide_index=True,
                    width="stretch",
                    key="plan_editor_final"
                )

            plan_data = edited_plan_df.to_dict('records')

            total_cols = len(plan_data)
            score_points = 0

            for col in plan_data:
                strat = str(col['strategy']).lower()
                if strat in ['synthetic', 'hash']:
                    score_points += 100
                elif strat in ['mask', 'noise', 'date_shift']:
                    score_points += 50
                else:
                    score_points += 0 # keep

            privacy_score = int(score_points / total_cols) if total_cols > 0 else 0

            st.write(f"**Current Privacy Score: {privacy_score}%**")
            if privacy_score < 40:
                st.error(f"🔴 **Low Protection** - Sensitive data might be exposed! Score: {privacy_score}%")
            elif privacy_score < 75:
                st.warning(f"🟡 **Balanced Protection** - Good for internal testing. Score: {privacy_score}%")
            else:
                st.success(f"🟢 **High Protection** - Data is well obfuscated. Score: {privacy_score}%")

            st.progress(privacy_score / 100)

            st.divider()

            # 5. AKCIONI DUGMIÄ†I
            col_exec, col_save = st.columns(2)

            with col_exec:
                if st.button("🚀 Run Anonymization", width="stretch"):
                    with st.spinner("Processing data..."):
                        table_name, schema_name = st.session_state['selected_table_info']
                        current_salt = st.session_state.get('salt_input', 'default_salt')

                        full_df = db.read_table(table_name, schema_name)

                        anon_df, notes = db.apply_anonymization(full_df, plan_data, salt=current_salt)

                        st.session_state['last_run_notes'] = list(set(notes))

                        db.save_anonymized_table(anon_df, table_name, target_schema='anon')

                        st.success(f"✅ Data processed and saved to 'anon.{table_name}'")

                        db.log_action(
                            user="Ljubomir (Admin)",
                            schema=schema_name,
                            table=table_name,
                            score=privacy_score,
                            salt=current_salt
                        )
                        st.success(f"✅ Data processed and logged!")

            with col_save:
                if st.button("💾 Save Plan in DB", width="stretch", type="primary"):
                    table_name, schema_name = st.session_state['selected_table_info']
                    db.save_ai_plan(schema_name, table_name, plan_data)
                    st.success("Plan saved!")
        else:
            st.info("Please run Manual or AI Scan in the sidebar.")






    with tabs[2]:
        st.subheader("🔍 Side-by-Side Comparison")

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
                    st.write("**📄 Original Data**")
                    st.dataframe(raw_sample)
                with col2:
                    st.write(f"**🛡️ Anonymized (Salt: {current_salt})**")
                    # Sada je anonymized_sample Ä�ist DataFrame
                    st.dataframe(anonymized_sample)

                # --- NOVO: EXPORT SEKCIJA ---
                st.divider()
                st.subheader("Export Result")

                if st.button("Prepare Full Download (CSV)"):
                    with st.spinner("Generating full anonymized file..."):
                        full_df = db.read_table(table_name, schema_name)
                        # --- IZMENA 3: Ponovo otpakivanje za punu tabelu ---
                        full_anon, _ = db.apply_anonymization(full_df, current_plan_data, salt=current_salt)

                        csv = full_anon.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')

                        st.download_button(
                            label="Download Anonymized CSV",
                            data=csv,
                            file_name=f"anon_{table_name}.csv",
                            mime="text/csv",
                        )


    with tabs[3]:
        st.subheader("📜 Audit History")
        query = "SELECT * FROM metadata.audit_log ORDER BY execution_time DESC LIMIT 50"
        log_df = pd.read_sql(query, db.engine)
        st.dataframe(log_df, width="stretch")


