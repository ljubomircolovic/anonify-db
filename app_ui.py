# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import logging
from dotenv import load_dotenv

# Importi tvojih modula
from src.db import DBManager
from src.adapters.legacy.db_manager_adapter import DBManagerAdapter
from src.agents.privacy_agent import PrivacyAgent
from src.ui.auth import check_login
from src.ui.sidebar import (
    render_sidebar,
    render_data_source_section,  # legacy; kept for callsite parity, no longer rendered
    render_metadata_storage_section,
    render_connection_dashboard,
    DB_CONFIGS,
    DB_ENV_KEY_BY_LABEL,
)
from src.ui.source.source_tab import render_source_tab
from src.ui.tabs.planner.planner_table_config import render_planner_tab
from src.ui.tabs.target_database_transfer_tab import render_target_database_transfer_tab
from src.ui.selection_tab import render_selection_tab
from src.ui.main_menu import render_main_menu
from init_db import initialize_metadata
from src.logic.naming import normalize_name_fragment
from src.logic.workflow import (
    bind_plan_metadata_to_source,
    maybe_auto_bind_plan_to_source,
    render_workflow_readiness_warning,
)

logger = logging.getLogger(__name__)


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
# --- st.data_editor / BaseWeb dropdown: Azure highlight (Strategy column, etc.) ---
st.markdown(
    """
<style>
/* Target the background of the selected/hovered item in Streamlit/BaseWeb dropdowns */
[data-baseweb="option"] {
    background-color: transparent !important;
}

/* This targets the actual 'active' or 'hovered' state in the list */
[data-baseweb="option"]:hover,
[data-baseweb="option"]:focus,
[aria-selected="true"] {
    background-color: #007BFF !important; /* Azure Blue */
    color: white !important;
}

/* Fix the focus border of the cell being edited */
div[data-testid="stDataEditor"] :focus-within {
    border-color: #007BFF !important;
}
</style>
    """,
    unsafe_allow_html=True,
)
st.session_state.setdefault(
    "source_confirmed",
    str(os.getenv("SOURCE_CONFIRMED", "")).strip().lower() in ("1", "true", "yes"),
)
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
    
    code, .stMarkdown p code {
        color: #0078d4 !important;
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

    /* 3. Kill any remaining red on labels or captions */
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

    /* --- Brand Header (🛡️ AnonifyDB: Data Engineering Tool) ----------- */
    .adb-app-header {
        display: flex;
        align-items: baseline;
        gap: 0.55rem;
        margin: 0.25rem 0 0.5rem 0;
        line-height: 1.15;
    }
    .adb-app-header-icon {
        font-size: 1.75rem;
        line-height: 1;
        flex: 0 0 auto;
        transform: translateY(0.2rem);
    }
    .adb-app-header-title {
        font-size: 1.95rem;
        font-weight: 700;
        color: #0f172a;
        margin: 0;
        letter-spacing: -0.01em;
        white-space: nowrap;
    }
    .adb-app-header-sub {
        font-weight: 500;
        color: #475569;
        letter-spacing: 0;
        margin-left: 0.15rem;
    }
    @media (max-width: 1200px) {
        .adb-app-header-title { font-size: 1.7rem; white-space: normal; }
    }
    @media (prefers-color-scheme: dark) {
        .adb-app-header-title { color: #e2e8f0; }
        .adb-app-header-sub { color: #94a3b8; }
    }

    /* --- Connection Dashboard (Source · Mappings · Export Target) ------ */
    .adb-conn-dashboard {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.65rem;
        margin: 0.4rem 0 1rem 0;
    }
    .adb-conn-card {
        background: rgba(15, 23, 42, 0.03);
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 10px;
        padding: 0.6rem 0.9rem;
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        min-width: 0;
    }
    .adb-conn-head {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-weight: 600;
        color: #0f172a;
        font-size: 0.95rem;
        line-height: 1.2;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .adb-conn-icon { font-size: 1.05rem; line-height: 1; }
    .adb-conn-label { overflow: hidden; text-overflow: ellipsis; }
    .adb-conn-body {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        color: #475569;
        font-size: 0.84rem;
        flex-wrap: wrap;
    }
    .adb-conn-dot { font-size: 0.85rem; line-height: 1; }
    .adb-conn-state { font-weight: 600; color: #0f172a; }
    .adb-conn-meta { opacity: 0.85; }
    .adb-conn-hint {
        font-size: 0.78rem;
        color: #64748b;
        margin-top: 0.15rem;
    }
    .adb-conn-card code {
        background: rgba(0, 120, 212, 0.10) !important;
        color: #0078d4 !important;
        padding: 0.05rem 0.35rem;
        border-radius: 4px;
        font-size: 0.82rem;
    }
    @media (max-width: 1100px) {
        .adb-conn-dashboard { grid-template-columns: 1fr; }
    }
    @media (prefers-color-scheme: dark) {
        .adb-conn-card {
            background: rgba(148, 163, 184, 0.06);
            border-color: rgba(148, 163, 184, 0.18);
        }
        .adb-conn-head { color: #e2e8f0; }
        .adb-conn-body { color: #94a3b8; }
        .adb-conn-state { color: #e2e8f0; }
        .adb-conn-hint { color: #94a3b8; }
        .adb-conn-card code {
            background: rgba(56, 189, 248, 0.14) !important;
            color: #38bdf8 !important;
        }
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

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = bool(st.session_state.get("authenticated", True))

from src.ui.sidebar import DB_CONFIGS
available_envs = list(DB_CONFIGS.keys())
if not available_envs:
    st.error("No metadata database connection found. Set DATABASE_URL or DB_URL_* environment variables.")
    st.stop()
selected_env = st.session_state.get("selected_env_label", available_envs[0])
if selected_env not in DB_CONFIGS:
    selected_env = available_envs[0]
st.session_state["selected_env_label"] = selected_env

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
metadata_ok, metadata_message = db.test_metadata_connection()
st.session_state["metadata_live_status"] = bool(metadata_ok)

if (
    st.session_state.get("active_plan_db_name", "None") != "None"
    and st.session_state.get("last_env") == selected_env
    and st.session_state.get("connected_plan_db_name") != st.session_state.get("active_plan_db_name", "None")
):
    try:
        db.connect_to_existing_plan_database(st.session_state.get("active_plan_db_name", "None"))
        st.session_state["connected_plan_db_name"] = st.session_state.get("active_plan_db_name", "None")
        if "plan_metadata" not in st.session_state or not isinstance(st.session_state["plan_metadata"], dict):
            st.session_state["plan_metadata"] = {}
        st.session_state["plan_metadata"]["plan_db_name"] = st.session_state.get("active_plan_db_name", "None")
        st.session_state["plan_metadata"]["target_db_connection"] = db.target_db_url
    except Exception:
        st.session_state["project_initialized"] = False

if "logged_in" in st.session_state and st.session_state.get("logged_in"):
    username = str(st.session_state.get("user_name", "admin"))
    user_role = str(st.session_state.get("user_role", "Administrator"))
    with st.container():
        cols = st.columns([3.6, 1.3, 1.0, 0.5], vertical_alignment="center")
        with cols[0]:
            st.markdown(
                """
<div class="adb-app-header">
  <span class="adb-app-header-icon" aria-hidden="true">🛡️</span>
  <h1 class="adb-app-header-title">
    AnonifyDB<span class="adb-app-header-sub">: Data Engineering Tool</span>
  </h1>
</div>
                """,
                unsafe_allow_html=True,
            )
        with cols[1]:
            st.write(f"👤 **{username}**")
        with cols[2]:
            st.write(f"ID: {user_role}")
        with cols[3]:
            render_main_menu()

    # Live status of the three data sources the app interacts with. Replaces
    # the single inline "Metadata: Connected/Disconnected" pill that used to
    # live next to the user/role cluster.
    render_connection_dashboard(
        db,
        metadata_env_url=DB_CONFIGS.get(selected_env, ""),
        metadata_message=metadata_message,
        source_tooltip=(
            "Source: Reading from SOURCE_DB_URL in .env"
            if st.session_state.get("source_confirmed")
            else "Source: Defaults read from DATABASE_URL in .env until you confirm Source (saved to SOURCE_DB_URL in .env)"
        ),
        mappings_tooltip=(
            f"Mappings: Reading from {DB_ENV_KEY_BY_LABEL.get(selected_env, 'DATABASE_URL')} in .env"
        ),
        export_tooltip=(
            f"Export: Reading from {DB_ENV_KEY_BY_LABEL.get(selected_env, 'DATABASE_URL')} in .env "
            "(plan target database name comes from the active plan in Mappings)"
        ),
    )

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
render_sidebar(agent, db)

# --- 5. UI: GLAVNI SADRŽAJ ---
render_metadata_storage_section(db)

# --- 5a. ENTERPRISE TAB-STRIP THEMING --------------------------------------
# Subtle Azure-blue active indicator + slate inactive text, matching the rest
# of the Command Center's enterprise palette.
st.markdown(
    """
<style>
.stTabs [data-baseweb="tab-list"] {
    gap: 0.25rem;
    border-bottom: 1px solid rgba(15, 23, 42, 0.10);
    padding: 0 0 0.15rem 0;
}
.stTabs [data-baseweb="tab"] {
    color: #475569;
    font-weight: 600;
    padding: 0.6rem 1.1rem;
    border-radius: 8px 8px 0 0;
    transition: background-color 140ms ease-out, color 140ms ease-out;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(0, 120, 212, 0.06);
    color: #0078d4;
}
.stTabs [aria-selected="true"] {
    color: #0078d4 !important;
    background: rgba(0, 120, 212, 0.06) !important;
}
.stTabs [data-baseweb="tab-highlight"] {
    background-color: #0078d4 !important;
}
.stTabs [data-baseweb="tab-panel"] {
    padding-top: 1.25rem;
}
@media (prefers-color-scheme: dark) {
    .stTabs [data-baseweb="tab-list"] { border-bottom-color: rgba(148, 163, 184, 0.18); }
    .stTabs [data-baseweb="tab"] { color: #94a3b8; }
    .stTabs [data-baseweb="tab"]:hover { color: #38bdf8; background: rgba(56, 189, 248, 0.08); }
    .stTabs [aria-selected="true"] { color: #38bdf8 !important; background: rgba(56, 189, 248, 0.08) !important; }
    .stTabs [data-baseweb="tab-highlight"] { background-color: #38bdf8 !important; }
}
</style>
    """,
    unsafe_allow_html=True,
)

# --- 5b. FOUR-TAB NAVIGATION -----------------------------------------------
# Tabs are created up-front so the strip's visual order is stable. Each tab's
# content is populated below in script order; tabs are populated out of order
# so the workflow-critical Mappings block runs first (it sets the variables
# the init handlers need).
#
# Preview (step 2) is intentionally placed before Transfer (step 3): users
# validate anonymization in RAM first, then commit to the target database.
_TAB_LABELS = [
    "1. 🔌 Source",
    "2. 🗺️ Mappings",
    "3. 🔍 In-Memory Preview (Source vs Anon)",
    "4. 🚀 Target Database Transfer",
]
tab_source, tab_mappings, tab_data_sel, tab_target_db_transfer = st.tabs(
    _TAB_LABELS
)

_source_session_ready = bool(st.session_state.get("source_connected"))
_TAB_LOCKED_MESSAGE = (
    "Please connect to a data source and initialize your session in the "
    "'1. 🔌 Source' tab to unlock this section."
)

# Defaults for plan handlers when the Mappings UI is locked (no widgets rendered).
initialize_clicked = False
continue_existing_clicked = False
plan_name = str(
    st.session_state.get("plan_name_input", "") or st.session_state.get("plan_name", "")
).strip()
selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()

# ===========================================================================
# TAB 2 — MAPPINGS  (Plan Selection + Rule Definition / Planner)
# ===========================================================================
# This block populates the Mappings tab AND defines the variables consumed by
# the initialization handlers below. Streamlit's tab context only redirects
# *element rendering*; Python variable assignments stay in the enclosing
# script scope, so `plan_name`, `initialize_clicked`, etc. remain accessible
# to the top-level handlers that follow.
with tab_mappings:
    if not _source_session_ready:
        st.info(_TAB_LOCKED_MESSAGE)
    else:
        with st.expander("🛠️ Plan Selection & Rule Definition", expanded=False):
            st.caption(
                "Enter a descriptive name (e.g., 'GDPR Production Prep') or continue with an existing plan database. "
                "Plan selection is independent from the source connection — configure them in any order."
            )
            # Plan activation is now driven exclusively by an explicit button inside
            # this tab. The Source tab's "🚀 Initialize Session" button no longer
            # implicitly creates a plan; the two flows are fully decoupled.
            initialize_clicked = False
        
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
        
            row1_left, row1_right = st.columns([3, 1], gap="small", vertical_alignment="bottom")
            with row1_left:
                st.text_input(
                    "Create New Plan",
                    key="plan_name_input",
                    placeholder="Enter plan name...",
                    on_change=on_plan_name_change,
                    label_visibility="visible",
                )
            plan_name = str(st.session_state.get("plan_name_input", "")).strip()
            if plan_name:
                st.session_state["existing_plan_selection"] = "None"
                st.session_state["selected_existing_plan"] = "None"
            st.session_state["plan_name"] = plan_name
            with row1_right:
                # Dedicated, in-tab plan activation. The Source tab's Initialize
                # Session button no longer triggers this — the workflows are
                # decoupled and either can be completed first.
                activate_plan_clicked = st.button(
                    "✨ Activate Plan",
                    width="stretch",
                    disabled=(not plan_name) or existing_selected_now,
                    help="Create the plan database and activate it for rule definition.",
                    key="mappings_activate_plan_btn",
                )
                if activate_plan_clicked:
                    initialize_clicked = True
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
                    width="stretch",
                    disabled=bool(plan_name) or (not bool(selected_existing_plan)) or selected_existing_plan == "None"
                )
        
            # Inline status hint so users know the plan half is configured (or not)
            # without needing to leave this tab.
            if st.session_state.get("active_plan_db_name", "None") not in (None, "None"):
                st.success(
                    f"Plan active: `{st.session_state.get('active_plan_db_name')}` — "
                    "you can now configure the Source tab (or refine rules below)."
                )
            else:
                st.info("No plan activated yet. Activate or continue a plan above to start defining rules.")

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
                "target_db_connection": db.target_db_url,
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
                    "target_db_connection": db.target_db_url,
                }
                st.session_state["scroll_to_data_source"] = True
                st.success(f"Plan {selected_existing_plan} active. Configure Data Source below.")
            else:
                effective_plan_name = str(plan_name or "").strip()
                if (not allow_custom_naming) and (not effective_plan_name.lower().startswith("anon_")):
                    effective_plan_name = f"anon_{normalize_name_fragment(effective_plan_name)}"
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
                    "target_db_connection": db.target_db_url,
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

selected_existing_plan = str(st.session_state.get("selected_existing_plan", "")).strip()

ordered_tables = st.session_state.get("all_tables_list", []) or st.session_state.get("selected_tables", [])

# --- Source scan (spinner) — DECOUPLED FROM PLAN -------------------------
# The Source tab's "🚀 Initialize Session" button sets
# `trigger_session_initialize`. We now consume it here regardless of plan
# state: the source-only work (connection test + schema/table discovery)
# always runs, and the plan-coupled binding only piggy-backs if a plan
# happens to already be active in this session.
if bool(st.session_state.pop("trigger_session_initialize", False)):
    if not st.session_state.get("source_confirmed"):
        st.warning("Confirm Source in the **Source** tab before running Initialize Session.")
    else:
        with st.spinner("Establishing secure connection and indexing source metadata..."):
            success, message = db.test_connection()
            if not success:
                st.error(message)
                st.stop()

            # ---- Source-only state (works even with no plan) ----
            st.session_state["source_connected"] = True
            st.session_state["source_db_connection"] = dict(
                st.session_state.get("db_config", {}).get("connection", {})
            )

            selected_schema = st.session_state.get("selected_schema")
            schemas = db.get_all_schemas()
            if not selected_schema:
                selected_schema = schemas[0] if schemas else "public"
                st.session_state["selected_schema"] = selected_schema

            tables = db.get_tables_in_schema(selected_schema) if selected_schema else []
            ordered_tables = db.get_execution_order(tables, selected_schema) if selected_schema else []
            st.session_state["last_confirmed_tables"] = tables
            st.session_state["all_tables_list"] = ordered_tables
            st.session_state["selected_tables"] = ordered_tables
            if ordered_tables:
                st.session_state["selected_table_info"] = (ordered_tables[0], selected_schema)

            # ---- Plan-coupled binding (only if a plan is also active) ----
            if is_initialized:
                bind_plan_metadata_to_source(db, selected_schema, ordered_tables, st.session_state)
            else:
                logger.info(
                    "[DB_MANAGER] Source scanned without an active plan; binding deferred."
                )
        st.rerun()

# After plan activation OR after a source-only scan, if both halves are now
# ready but binding hasn't happened yet (e.g. source-first → plan-second),
# wire them together here so dependent tabs work on the next render.
maybe_auto_bind_plan_to_source(db, st.session_state)

# `scroll_to_data_source` was used by the legacy single-page layout to scroll
# the viewport to the data-source section after plan initialization. With the
# tabbed layout the user simply switches to the Source tab, so the flag has
# no UI effect — we just clear it here to keep session state tidy.
st.session_state.pop("scroll_to_data_source", None)


# ===========================================================================
# TAB 1 — SOURCE  (Domain · File · Database · API · Source Log)
# ===========================================================================
# Fully independent from the Mappings tab — the user can configure and scan
# the source database without selecting (or even creating) a plan first. The
# tab is now organized into five collapsible sections rendered by
# `render_source_tab`. Only the Database section is currently wired into the
# rest of the workflow; File/API are functional scaffolds with their own
# session-state slices.
with tab_source:
    st.markdown("### 📂 Data Source · Intelligence Context")
    st.markdown('<div id="data-source-section"></div>', unsafe_allow_html=True)
    render_source_tab(db)

    if st.session_state.get("source_connected"):
        plan_name_disp = st.session_state.get("active_plan_db_name", "None")
        if plan_name_disp not in (None, "None"):
            st.success(
                f"Source connected and bound to plan `{plan_name_disp}`. "
                "Open the **Mappings** tab to define rules, then use **In-Memory Preview** "
                "before **Target Database Transfer**."
            )
        else:
            st.info(
                "Source connected. Activate a plan in the **Mappings** tab to start defining anonymization rules."
            )


# ===========================================================================
# TAB 2 (continued) — MAPPINGS  (Rule Definition / Planner)
# ===========================================================================
# Plan selection and the planner render only after ``source_connected`` is set
# (Initialize Session in Source). The planner additionally needs plan readiness
# via ``render_workflow_readiness_warning``.
with tab_mappings:
    if _source_session_ready and render_workflow_readiness_warning(st.session_state):
        render_planner_tab(db)


# ===========================================================================
# TAB 3 — IN-MEMORY PREVIEW (Source vs Anon · read-only RAM simulation)
# ===========================================================================
with tab_data_sel:
    if not _source_session_ready:
        st.info(_TAB_LOCKED_MESSAGE)
    else:
        st.markdown("## 🔬 Safe In-Memory Preview")
        st.info(
            "**Read-only simulation — no database writes.** "
            "This tab runs anonymization logic entirely **in RAM** for side-by-side validation. "
            "Nothing is persisted, committed, or synchronized to any target database instance. "
            "Use it to inspect results safely before any physical transfer."
        )
        render_selection_tab(db)


# ===========================================================================
# TAB 4 — TARGET DATABASE TRANSFER (Verify / Execute / History · physical write)
# ===========================================================================
with tab_target_db_transfer:
    if not _source_session_ready:
        st.info(_TAB_LOCKED_MESSAGE)
    else:
        st.markdown("## 🚀 Physical Target Database Transfer")
        st.warning(
            "**Physical write layer — data will be copied and committed.** "
            "This tab performs **real** anonymized data transfer: records are written, "
            "synchronized, and committed to the **target database instance** bound to your plan. "
            "Verify results in **In-Memory Preview** first; actions here change persistent storage."
        )
        render_target_database_transfer_tab(db)
