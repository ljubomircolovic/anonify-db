# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd

def render_explorer_tab(db):
    if 'current_df' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        st.subheader(f"📊 Raw Data Explorer: {schema_name}.{table_name}")

        if not st.session_state['current_df'].empty:
            st.info(f"Showing {len(st.session_state['current_df'])} rows based on your sidebar filter.")
            st.dataframe(st.session_state['current_df'], use_container_width=True)
        else:
            st.warning(f"⚠️ No records found! The filter returned 0 rows.")

def render_planner_tab(db):
    if 'ai_analysis' in st.session_state:
        st.subheader("🛠️ Review & Finalize Plan")

        # 1. Unifikacija podataka (Pydantic vs Dict)
        analysis_data = st.session_state['ai_analysis']
        if hasattr(analysis_data, 'plan'):
            plan_list = [p.model_dump() for p in analysis_data.plan]
        elif isinstance(analysis_data, dict) and 'plan' in analysis_data:
            plan_list = analysis_data['plan']
        else:
            plan_list = []

        plan_df = pd.DataFrame(plan_list)

        # 2. Data Editor sa unikatnim ključem
        table_name, schema_name = st.session_state['selected_table_info']
        editor_key = f"plan_editor_{table_name}"

        edited_plan_df = st.data_editor(
            plan_df,
            column_config={
                "column": st.column_config.TextColumn("Database Column", disabled=True),
                "is_pii": st.column_config.CheckboxColumn("PII", disabled=True),
                "strategy": st.column_config.SelectboxColumn(
                    "Strategy",
                    options=["keep", "hash", "mask", "mapping", "noise", "date_shift"],
                    required=True
                )
            },
            hide_index=True,
            use_container_width=True,
            key=editor_key
        )

        plan_data = edited_plan_df.to_dict('records')
        st.session_state['current_plan'] = plan_data

        # 3. Privacy Score Obračun
        total_cols = len(plan_data)
        score_points = 0
        for col in plan_data:
            strat = str(col['strategy']).lower()
            if strat in ['mapping', 'hash']: score_points += 100
            elif strat in ['mask', 'noise', 'date_shift']: score_points += 50

        privacy_score = int(score_points / total_cols) if total_cols > 0 else 0

        st.write(f"**Current Privacy Score: {privacy_score}%**")
        st.progress(privacy_score / 100)

        if privacy_score < 40: st.error("🔴 Low Protection")
        elif privacy_score < 75: st.warning("🟡 Balanced Protection")
        else: st.success("🟢 High Protection")

        st.divider()

        # 4. Akcije (Run & Save)
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🚀 Run Anonymization", use_container_width=True):
                with st.spinner("Processing data..."):
                    current_salt = st.session_state.get('salt_input', 'default_salt')
                    current_locale = st.session_state.get('selected_locale', 'de') # NOVO

                    # Čitamo celu tabelu (ili sa filterom) za procesuiranje
                    full_df = db.read_table(table_name, schema_name)

                    # Primenjujemo anonimizaciju
                    anon_df, notes = db.apply_anonymization(
                        full_df,
                        plan_data,
                        salt=current_salt,
                        locale=current_locale
                    )

                    # Čuvanje u 'anon' šemu
                    db.save_anonymized_table(anon_df, table_name, target_schema='anon')

                    # Audit Log
                    db.log_action(
                        user=st.session_state.get('user_name', 'Admin'),
                        schema=schema_name,
                        table=table_name,
                        score=privacy_score,
                        salt=current_salt
                    )
                    st.success(f"✅ Processed and saved to 'anon.{table_name}'")

        with col2:
            if st.button("💾 Save Plan in DB", type="primary", use_container_width=True):
                db.save_ai_plan(schema_name, table_name, plan_data)
                st.success("Plan saved successfully!")

def render_comparison_tab(db):
    st.subheader("🔍 Side-by-Side Comparison")

    if 'current_plan' in st.session_state and 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_plan = st.session_state['current_plan']
        current_salt = st.session_state.get('salt_input', 'default_salt')
        current_locale = st.session_state.get('selected_locale', 'de') # NOVO

        # Uzimamo uzorak za poređenje
        raw_sample = db.read_table(
            table_name,
            schema_name,
            limit=st.session_state.get('last_limit_val', 10),
            where_filter=st.session_state.get('last_where_filter', None)
        )

        if not raw_sample.empty:
            # Primenjujemo anonimizaciju na uzorak sa locale podrškom
            anon_sample, notes = db.apply_anonymization(
                raw_sample,
                current_plan,
                salt=current_salt,
                locale=current_locale
            )

            if notes:
                for n in set(notes): st.info(n)

            c1, c2 = st.columns(2)
            with c1:
                st.write("**📄 Original Data**")
                st.dataframe(raw_sample, use_container_width=True)
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, use_container_width=True)

            # Export Sekcija
            st.divider()
            if st.button("Prepare Full Download (CSV)"):
                with st.spinner("Generating CSV..."):
                    full_df = db.read_table(table_name, schema_name)
                    full_anon, _ = db.apply_anonymization(full_df, current_plan, salt=current_salt, locale=current_locale)
                    csv = full_anon.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Click to Download CSV",
                        data=csv,
                        file_name=f"anon_{table_name}.csv",
                        mime="text/csv",
                    )
        else:
            st.warning("No records to compare.")

def render_tabs(db):
    tab_list = ["📊 Explorer", "🛠️ Plan", "🔍 Comparison", "📜 Audit"]
    tabs = st.tabs(tab_list)

    with tabs[0]:
        render_explorer_tab(db)

    with tabs[1]:
        render_planner_tab(db)

    with tabs[2]:
        render_comparison_tab(db)

    with tabs[3]:
        # Audit log fetch
        log_query = "SELECT * FROM metadata.audit_log ORDER BY execution_time DESC LIMIT 50"
        try:
            log_df = pd.read_sql(log_query, db.engine)
            st.dataframe(log_df, use_container_width=True)
        except:
            st.info("No audit logs found yet.")