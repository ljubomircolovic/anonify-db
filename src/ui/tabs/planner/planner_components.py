import streamlit as st
from typing import Any


def render_status_chain(all_tables, completed_tables):
    """Shows progress chain at the top."""
    if all_tables:
        steps = [f"{'✅' if t in completed_tables else '⏳'} {t}" for t in all_tables]
        st.info(f"⛓️ **Execution Order:** {' ➔ '.join(steps)}")

def render_table_header_info(schema_name, table_name):
    """Shows a stylized header for the active table."""
    st.markdown(f"""
        <div style="background-color: #e8f4f8; padding: 15px; border-radius: 10px; border-left: 5px solid #2e86de; margin-bottom: 20px;">
            <span style="color: #576574; font-size: 16px; font-weight: bold;">Current Table for Analysis:</span><br>
            <span style="color: #2e86de; font-size: 24px; font-weight: 800; font-family: 'Courier New', monospace;">
                {schema_name}.{table_name}
            </span>
        </div>
    """, unsafe_allow_html=True)

def render_ai_audit_log():
    """Shows the latest AI log if present in session."""
    if 'last_ai_audit' in st.session_state:
        with st.expander("📡 Latest AI Audit Log (Data Sent to Cloud)"):
            st.json(st.session_state['last_ai_audit'])


def render_planner_action_buttons(db: Any, table_name: str, schema_name: str) -> None:
    """Renders stacked action buttons and audit log."""
    if st.button("📂 Load Saved", width="stretch", key=f"load_btn_{table_name}"):
        saved_data = db.get_saved_plan(schema_name, table_name)
        if saved_data:
            st.session_state['ai_analysis'] = saved_data['plan']
            st.session_state['plan_snapshot'] = saved_data['plan']
            st.session_state[f"where_clause_{table_name}"] = saved_data['where']
            st.session_state['plan_origin'] = 'saved'
            st.session_state['plan_active'] = True

    if st.button("✍️ Manual", width="stretch", key=f"man_btn_{table_name}"):
        columns = db.get_columns(table_name, schema_name)
        st.session_state['ai_analysis'] = [
            {"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns
        ]
        st.session_state['plan_origin'] = 'new'
        st.session_state['plan_active'] = True

    render_ai_audit_log()