# -*- coding: utf-8 -*-
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
import logging
from dotenv import load_dotenv

# Importi tvojih modula
from src.database.db_manager import DBManager
from src.adapters.legacy.db_manager_adapter import DBManagerAdapter
from src.agents.privacy_agent import PrivacyAgent
from src.ui.auth import check_login
from src.ui.sidebar import render_sidebar, render_data_source_section
from src.ui.tabs_content import render_tabs
from init_db import initialize_metadata
logger = logging.getLogger(__name__)


def _normalize_name_fragment(raw_value: str) -> str:
    sanitized = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in str(raw_value or "").strip().lower())
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized.strip("_")


@st.dialog("High Risk Naming Warning")
def _render_custom_name_warning_dialog():
    st.markdown("### ⚠️ **High Risk Warning**")
    st.markdown(
        "<p style='color:#b91c1c; font-weight:700; margin-top:0.25rem;'>"
        "Warning: Using a custom name increases the risk of overlapping with production databases. "
        "Ensure you do not accidentally overwrite real data."
        "</p>",
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel / Back", width="stretch", key="custom_name_warn_cancel"):
            st.session_state["custom_name_warning_confirmed"] = False
            st.session_state["pending_initialize_after_warning"] = False
            st.rerun()
    with c2:
        if st.button("Confirm (Proceed with custom name)", type="primary", width="stretch", key="custom_name_warn_confirm"):
            st.session_state["custom_name_warning_confirmed"] = True
            st.session_state["pending_initialize_after_warning"] = True
            st.rerun()

# --- 1. SETUP & AUTH ---
load_dotenv()
st.set_page_config(page_title="AnonifyDB", layout="wide", initial_sidebar_state="collapsed")
st.markdown('''
<style>
    /* 1. Target the Multiselect Tags (Pills) */
    [data-baseweb="tag"] {
        background-color: #0078d4 !important;
        color: white !important;
        border: none !important;
    }

    /* 2. Target the text inside the tags specifically */
    [data-baseweb="tag"] span {
        color: white !important;
        -webkit-text-fill-color: white !important;
    }

    /* 3. Target the 'Delete' (X) icon inside the tags */
    [data-baseweb="tag"] svg {
        fill: white !important;
        color: white !important;
    }

    /* 4. Fix for the focused/active state of the tags */
    [data-baseweb="tag"]:focus, [data-baseweb="tag"]:active {
        background-color: #005a9e !important;
    }
    
    /* 5. Ensure NO red color persists in any span within the main container */
    .st-emotion-cache-119tkyc, code {
        color: #0078d4 !important;
        background-color: #f3f2f1 !important;
    }

    /* 1. Force white text for table names inside primary buttons */
    [data-testid="stBaseButton-primary"] p, 
    [data-testid="stBaseButton-primary"] span,
    [data-testid="stBaseButton-primary"] div {
        color: white !important;
        -webkit-text-fill-color: white !important;
    }

    /* 2. Force white text for table names inside secondary buttons */
    [data-testid="stBaseButton-secondary"] p,
    [data-testid="stBaseButton-secondary"] span {
        color: #0078d4 !important;
        -webkit-text-fill-color: #0078d4 !important;
    }

    /* 3. Target code snippets or table name labels that might be red */
    code, .stMarkdown p code {
        color: #0078d4 !important;
        background-color: #f3f2f1 !important;
    }

    /* 4. Kill any remaining red on labels or captions */
    [data-testid="stWidgetLabel"] p, [data-testid="stCaptionContainer"] p {
        color: #323130 !important;
    }
</style>
''', unsafe_allow_html=True)
st.markdown('''
<style>
    /* 1. Force White Text for ALL Primary Buttons */
    [data-testid="stBaseButton-primary"], 
    [data-testid="stBaseButton-primary"] p,
    button[kind="primary"] p {
        color: white !important;
        -webkit-text-fill-color: white !important; /* Fix for some browser overrides */
    }

    /* 2. Fix Checkbox Label Appearance */
    /* Target the label container to remove any shading/opacity */
    [data-testid="stWidgetLabel"], 
    [data-testid="stWidgetLabel"] p {
        color: #323130 !important; /* Azure Charcoal */
        opacity: 1 !important;
        filter: none !important;
        text-shadow: none !important;
        -webkit-font-smoothing: antialiased !important;
    }

    /* 3. Ensure the Checkbox Square remains Azure Blue */
    [data-baseweb="checkbox"] span {
        background-color: #0078d4 !important;
        border-color: #0078d4 !important;
    }

    /* 4. Global Cleanup for Containers */
    /* Remove any inherited filters that cause the 'blurry/shaded' look */
    .stMarkdown, .element-container, .stCheckbox {
        opacity: 1 !important;
        filter: none !important;
    }

    /* 5. Azure Blue for Primary Buttons Background */
    [data-testid="stBaseButton-primary"], button[kind="primary"] {
        background-color: #0078d4 !important;
        border: none !important;
    }
</style>
''', unsafe_allow_html=True)


@st.cache_resource(show_spinner=False)
def get_cached_db_manager(db_url: str, session_scope: str):
    """
    Returns one DBManager instance per (session, db_url) scope.
    Using cache_resource avoids recreating connection pools on reruns.
    """
    return DBManager(db_url=db_url)


@st.cache_resource(show_spinner=False)
def get_cached_db_adapter(db_url: str, session_scope: str):
    """
    Returns one DBManagerAdapter per (session, db_url) scope.
    Adapter delegates to DBManager so behavior remains unchanged.
    """
    manager = get_cached_db_manager(db_url=db_url, session_scope=session_scope)
    return DBManagerAdapter(manager)

if not check_login():
    st.stop()

# --- 2. COMPACT TOP BAR ---
top_col1, top_col2, top_col3 = st.columns([2, 2, 1], vertical_alignment="bottom")
top_col1.markdown(f"👤 **{st.session_state.get('user_name', 'Admin')}**")

from src.ui.sidebar import DB_CONFIGS
selected_env = top_col2.selectbox(
    "🔌 Target Environment",
    options=list(DB_CONFIGS.keys()),
    label_visibility="collapsed"
)

if 'db' not in st.session_state or st.session_state.get('last_env') != selected_env:
    current_url = DB_CONFIGS[selected_env]
    if 'session_scope' not in st.session_state:
        st.session_state['session_scope'] = f"session-{os.urandom(8).hex()}"
    st.session_state['db'] = get_cached_db_adapter(
        db_url=current_url,
        session_scope=st.session_state['session_scope']
    )
    st.session_state['last_env'] = selected_env

db = st.session_state['db']

if (
    st.session_state.get("active_plan_db_name", "None") != "None"
    and st.session_state.get("last_env") == selected_env
    and st.session_state.get("connected_plan_db_name") != st.session_state.get("active_plan_db_name", "None")
):
    try:
        db.connect_to_existing_plan_database(st.session_state.get("active_plan_db_name", "None"))
        st.session_state["connected_plan_db_name"] = st.session_state.get("active_plan_db_name", "None")
    except Exception:
        st.session_state["project_initialized"] = False

if top_col3.button("Logout", width="stretch"):
    st.session_state['authenticated'] = False
    st.rerun()

if top_col3.button("⚡ Test", width="stretch"):
    success, message = db.test_connection()
    if success:
        st.success(message)
    else:
        st.error(message)

# --- 3. INICIJALIZACIJA (State Management) ---
if 'agent' not in st.session_state:
    st.session_state['agent'] = PrivacyAgent()

if 'db_initialized' not in st.session_state:
    st.session_state['init_logs'] = initialize_metadata()
    st.session_state['db_initialized'] = True

# --- NOVO: Inicijalizacija Navigacione Istorije ---
if 'navigation_history' not in st.session_state:
    st.session_state['navigation_history'] = []
if 'history_pointer' not in st.session_state:
    st.session_state['history_pointer'] = -1

# Lokalni aliasi
db = st.session_state['db']
agent = st.session_state['agent']

# --- 4. UI: SIDEBAR ---
render_sidebar(agent)

# --- 5. UI: GLAVNI SADRŽAJ ---
st.title("🛡️ AnonifyDB Data Engineering Tool")
with st.container():
    st.markdown("### Plan Selection")
    st.caption(
        "Enter a descriptive name (e.g., 'GDPR Production Prep') or continue with an existing plan database."
    )
    row1_left, row1_right = st.columns([3, 1], gap="small", vertical_alignment="bottom")

    if "existing_plan_selection" not in st.session_state:
        st.session_state["existing_plan_selection"] = st.session_state.get("selected_existing_plan", "None")
    if "selected_existing_plan" not in st.session_state:
        st.session_state["selected_existing_plan"] = st.session_state.get("existing_plan_selection", "None")
    if "allow_custom_naming" not in st.session_state:
        st.session_state["allow_custom_naming"] = False
    if "custom_name_warning_confirmed" not in st.session_state:
        st.session_state["custom_name_warning_confirmed"] = False
    if "pending_initialize_after_warning" not in st.session_state:
        st.session_state["pending_initialize_after_warning"] = False
    if "plan_name" not in st.session_state:
        st.session_state["plan_name"] = ""
    if "plan_name_input" not in st.session_state:
        st.session_state["plan_name_input"] = st.session_state.get("plan_name", "")
    existing_selected_now = (
        st.session_state.get("existing_plan_selection") is not None
        and st.session_state.get("existing_plan_selection") != "None"
        and str(st.session_state.get("existing_plan_selection", "")).strip() != ""
    )

    def on_plan_name_change():
        st.session_state["plan_name"] = str(st.session_state.get("plan_name_input", ""))
        if str(st.session_state.get("plan_name_input", "")).strip():
            st.session_state["existing_plan_selection"] = "None"
            st.session_state["selected_existing_plan"] = "None"

    def _on_existing_plan_change():
        selected_existing = str(st.session_state.get("existing_plan_selection", "")).strip()
        if selected_existing and selected_existing != "None":
            st.session_state["selected_existing_plan"] = selected_existing
            st.session_state["plan_name"] = ""
            st.session_state["plan_name_input"] = ""
        else:
            st.session_state["selected_existing_plan"] = "None"

    with row1_left:
        st.text_input(
            "Create New Plan",
            key="plan_name_input",
            placeholder="Enter plan name...",
            on_change=on_plan_name_change,
            label_visibility="visible",
        )
    with row1_right:
        initialize_clicked = st.button(
            "🚀 Initialize Project",
            type="primary",
            use_container_width=True,
        )
    plan_name = str(st.session_state.get("plan_name_input", "")).strip()
    if plan_name:
        st.session_state["existing_plan_selection"] = "None"
        st.session_state["selected_existing_plan"] = "None"
    st.session_state["plan_name"] = plan_name
    allow_custom_naming = st.checkbox(
        "Allow custom name (skip default anon_ safety prefix)",
        key="allow_custom_naming",
    )
    custom_db_name = st.text_input(
        "Custom Target Database Name (optional)",
        key="custom_target_db_name",
        placeholder="anon_project_clone",
        disabled=existing_selected_now or (not allow_custom_naming),
    )
    existing_plan_dbs = db.list_existing_plan_databases()

    row2_left, row2_right = st.columns([3, 1], gap="small", vertical_alignment="bottom")
    with row2_left:
        st.selectbox(
            "Or Select Existing Plan",
            options=["None"] + existing_plan_dbs,
            key="existing_plan_selection",
            help="Reuse an existing plan database instead of creating a new one.",
            on_change=_on_existing_plan_change,
            label_visibility="visible",
        )
        selected_existing_plan = str(st.session_state.get("existing_plan_selection", "")).strip()
        st.session_state["selected_existing_plan"] = selected_existing_plan if selected_existing_plan else "None"
    with row2_right:
        continue_existing_clicked = st.button(
            "🔁 Continue with Existing",
            use_container_width=True,
            disabled=bool(plan_name) or (not bool(selected_existing_plan)) or selected_existing_plan == "None"
        )

if st.session_state.get("pending_initialize_after_warning") and st.session_state.get("custom_name_warning_confirmed"):
    initialize_clicked = True
    st.session_state["pending_initialize_after_warning"] = False

if continue_existing_clicked:
    if not selected_existing_plan or selected_existing_plan == "None":
        st.error("Select an existing plan database first.")
    else:
        try:
            db.connect_to_existing_plan_database(selected_existing_plan)
            st.session_state["active_plan_db_name"] = selected_existing_plan
            st.session_state["active_plan_db_key"] = f"{selected_env}:{selected_existing_plan}"
            st.session_state["connected_plan_db_name"] = selected_existing_plan
            st.session_state["project_initialized"] = True
            st.session_state["plan_metadata"] = {
                "plan_db_name": selected_existing_plan,
                "data_domain": st.session_state.get("data_source_domain_type", "Not Set"),
            }
            st.session_state["scroll_to_data_source"] = True
            st.success(f"Plan {selected_existing_plan} active. Configure Data Source below.")
        except Exception as exc:
            st.session_state["project_initialized"] = False
            st.error(f"Failed to connect to existing plan database: {exc}")

if initialize_clicked:
    selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()
    if selected_existing_plan == "None":
        selected_existing_plan = ""
    allow_custom_naming = bool(st.session_state.get("allow_custom_naming", False))
    custom_db_name = str(st.session_state.get("custom_target_db_name", "")).strip()
    exactly_one_source = bool(plan_name) ^ bool(selected_existing_plan)
    if not exactly_one_source:
        st.error("Provide either Plan Name or Existing Plan Database (exactly one).")
    else:
        try:
            if selected_existing_plan:
                db.connect_to_existing_plan_database(selected_existing_plan)
                st.session_state["active_plan_db_key"] = f"{selected_env}:{selected_existing_plan}"
                st.session_state["active_plan_db_name"] = selected_existing_plan
                st.session_state["connected_plan_db_name"] = selected_existing_plan
                st.session_state["project_initialized"] = True
                st.session_state["plan_metadata"] = {
                    "plan_db_name": selected_existing_plan,
                    "data_domain": st.session_state.get("data_source_domain_type", "Not Set"),
                }
                st.session_state["scroll_to_data_source"] = True
                st.success(f"Plan {selected_existing_plan} active. Configure Data Source below.")
            else:
                effective_plan_name = str(plan_name or "").strip()
                if (not allow_custom_naming) and (not effective_plan_name.lower().startswith("anon_")):
                    effective_plan_name = f"anon_{_normalize_name_fragment(effective_plan_name)}"
                    st.session_state["plan_name"] = effective_plan_name

                requires_custom_warning = (
                    allow_custom_naming
                    and (
                        (bool(effective_plan_name) and not effective_plan_name.lower().startswith("anon_"))
                        or (bool(custom_db_name) and not custom_db_name.lower().startswith("anon_"))
                    )
                )
                if requires_custom_warning and not st.session_state.get("custom_name_warning_confirmed", False):
                    st.session_state["pending_initialize_after_warning"] = True
                    _render_custom_name_warning_dialog()
                    st.stop()

                duplicate_exists, duplicate_candidate_name = db.plan_exists(
                    effective_plan_name,
                    custom_db_name=custom_db_name or None,
                    allow_non_anon_prefix=allow_custom_naming,
                )
                if duplicate_exists:
                    st.session_state["project_initialized"] = False
                    st.error(
                        f"Error: A plan with the name {duplicate_candidate_name} already exists. "
                        "Please choose a different name."
                    )
                    st.stop()

                created_db_name, created_now = db.bootstrap_plan_database(
                    effective_plan_name,
                    custom_db_name=custom_db_name or None,
                    allow_non_anon_prefix=allow_custom_naming and st.session_state.get("custom_name_warning_confirmed", False),
                )
                st.session_state["active_plan_db_key"] = f"{selected_env}:{created_db_name}"
                st.session_state["active_plan_db_name"] = created_db_name
                st.session_state["connected_plan_db_name"] = created_db_name
                st.session_state["project_initialized"] = True
                st.session_state["plan_metadata"] = {
                    "plan_db_name": created_db_name,
                    "data_domain": st.session_state.get("data_source_domain_type", "Not Set"),
                }
                st.session_state["scroll_to_data_source"] = True
                st.success(f"Plan {created_db_name} active. Configure Data Source below.")
                st.session_state["custom_name_warning_confirmed"] = False
                st.session_state["pending_initialize_after_warning"] = False
        except Exception as exc:
            st.session_state["project_initialized"] = False
            error_message = str(exc).lower()
            if "permission denied" in error_message or "createdb" in error_message:
                st.error(
                    "Cannot create a plan database because this user lacks CREATEDB permission. "
                    "Ask your PostgreSQL admin to grant CREATEDB or run with an admin account."
                )
            else:
                st.error(f"Failed to initialize plan database: {exc}")

is_initialized = (
    bool(st.session_state.get("active_plan_db_key"))
    and st.session_state.get("project_initialized", False)
    and str(st.session_state.get("active_plan_db_key", "")).startswith(f"{selected_env}:")
)

if st.session_state.get("active_plan_db_name", "None") != "None":
    st.markdown("---")
    st.markdown("### 📂 Data Source")
    components.html('<div id="data-source-section"></div>', height=0)
    render_data_source_section(db)
    if st.session_state.get("scroll_to_data_source"):
        components.html(
            """
            <script>
                const target = window.parent.document.getElementById('data-source-section');
                if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            </script>
            """,
            height=0,
        )
        st.session_state["scroll_to_data_source"] = False

selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()

ordered_tables = st.session_state.get("all_tables_list", []) or st.session_state.get("selected_tables", [])
start_enabled = bool(is_initialized and ordered_tables)
if is_initialized:
    st.markdown("---")
    if st.button("🚀 Start Planning / Load", type="primary", use_container_width=True, disabled=not start_enabled):
        selected_schema = st.session_state.get("selected_schema", "public")
        if not ordered_tables:
            st.error("Please select tables in the sidebar before starting planning.")
            st.stop()
        logger.info("[DB_MANAGER] Initializing planning session...")
        for t_name in ordered_tables:
            db.ensure_plan_security_metadata(selected_schema, t_name)
        st.session_state.pop('multi_ai_analysis', None)
        st.session_state['all_tables_list'] = ordered_tables
        st.session_state['selected_tables'] = ordered_tables
        st.session_state['selected_table_info'] = (ordered_tables[0], selected_schema)
        st.session_state['plan_active'] = False
        st.session_state['planning_initialized'] = True
        for key in ['ai_analysis', 'current_plan', 'plan_snapshot', 'last_rendered_table']:
            if key in st.session_state:
                del st.session_state[key]
        logger.info("[DB_MANAGER] Planning session initialized. Awaiting explicit AI scan trigger.")
        st.rerun()

if 'selected_table_info' in st.session_state:
    if is_initialized:
        render_tabs(db)
    else:
        st.info("Plan workflow is locked until the project is initialized.")