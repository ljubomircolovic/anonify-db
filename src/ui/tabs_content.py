# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd

def render_explorer_tab(db):
    if 'current_df' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        st.subheader(f"📊 Raw Data Explorer: {schema_name}.{table_name}")

        if not st.session_state['current_df'].empty:
            st.info(f"Showing {len(st.session_state['current_df'])} rows.")
            st.dataframe(st.session_state['current_df'], use_container_width=True)
        else:
            st.warning("⚠️ No records found with current filter.")

def render_planner_tab(db):
    if 'ai_analysis' in st.session_state:
        st.subheader("🛠️ Review & Finalize Plan")

        # 1. Priprema podataka za Editor
        analysis_data = st.session_state['ai_analysis']
        if hasattr(analysis_data, 'plan'):
            plan_list = [p.model_dump() for p in analysis_data.plan]
        elif isinstance(analysis_data, dict) and 'plan' in analysis_data:
            plan_list = analysis_data['plan']
        else:
            plan_list = []

        plan_df = pd.DataFrame(plan_list)

        # 2. Data Editor (sa unikatnim ključem)
        table_name = st.session_state['selected_table_info'][0]
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

        # 3. Privacy Score Logika
        plan_data = edited_plan_df.to_dict('records')
        st.session_state['current_plan'] = plan_data # Čuvamo za Comparison tab

        # ... ovde možeš dodati onaj tvoj Privacy Score obračun ...

        # 4. Akcije
        col1, col2 = st.columns(2)
        if col1.button("🚀 Run Anonymization", use_container_width=True):
            # Tvoja Run logika...
            pass
        if col2.button("💾 Save Plan", type="primary", use_container_width=True):
            schema_name = st.session_state['selected_table_info'][1]
            db.save_ai_plan(schema_name, table_name, plan_data)
            st.success("Plan saved!")

def render_comparison_tab(db):
    st.subheader("🔍 Side-by-Side Comparison")

    if 'current_plan' in st.session_state and 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_plan = st.session_state['current_plan']
        current_salt = st.session_state.get('salt_input', 'default_salt')

        # Uzimamo uzorak za poređenje
        raw_sample = db.read_table(
            table_name,
            schema_name,
            limit=st.session_state.get('last_limit_val', 10),
            where_filter=st.session_state.get('last_where_filter', None)
        )

        if not raw_sample.empty:
            anon_sample, _ = db.apply_anonymization(raw_sample, current_plan, salt=current_salt)

            c1, c2 = st.columns(2)
            c1.write("**📄 Original**")
            c1.dataframe(raw_sample, use_container_width=True)
            c2.write(f"**🛡️ Anonymized (Salt: {current_salt})**")
            c2.dataframe(anon_sample, use_container_width=True)
            
            
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
        # Audit log direktno ovde
        import pandas as pd
        log_df = pd.read_sql("SELECT * FROM metadata.audit_log ORDER BY execution_time DESC LIMIT 50", db.engine)
        st.dataframe(log_df, use_container_width=True)