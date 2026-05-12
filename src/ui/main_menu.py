# -*- coding: utf-8 -*-
"""AnonifyDB Command Center — top-right hamburger menu.

Renders a subtle hamburger drawer in the top-right of the page header and
exposes informational modals (Help, About, Feedback, Support, Contact,
Privacy, Terms) plus the existing logout flow. The drawer panel opens
downward and is right-anchored to the trigger so it slides in from the
right edge of the application. Menu items 1–7 open a centered,
dismissible Streamlit dialog so the main Scan/Plan/Execute workflow is
never disrupted.
"""

import streamlit as st


_MENU_ITEMS = [
    ("help",     "Help",     "❓"),
    ("about",    "About",    "ℹ️"),
    ("feedback", "Feedback", "💬"),
    ("support",  "Support",  "🛟"),
    ("contact",  "Contact",  "✉️"),
    ("privacy",  "Privacy",  "🛡️"),
    ("terms",    "Terms",    "📄"),
]

_DIALOG_STATE_KEY = "active_main_menu_dialog"
_POPOVER_CONTAINER_KEY = "main_menu_popover"
_PANEL_CONTAINER_KEY = "main_menu_panel"
_OPEN_STATE_KEY = "main_menu_open"
_TOGGLE_BUTTON_KEY = "main_menu_toggle"


def _inject_styles() -> None:
    """Inject CSS for the hamburger button, drawer panel and dialog tone.

    Streamlit rebuilds the DOM on every rerun, so the `<style>` block must
    be emitted on every render — otherwise the rules silently disappear
    after the first interaction. We therefore re-inject unconditionally.
    """
    st.markdown(
        """
<style>
/* --- Toggle container (wraps the hamburger AND the dropdown panel) -------
   This is the positioned ancestor. We also force `overflow: visible` on
   any Streamlit ancestor that contains the wrapper, so the absolutely
   positioned panel below can spill out of the column/header without being
   clipped. */
.st-key-main_menu_popover {
    position: relative !important;
    overflow: visible !important;
    display: flex !important;
    flex-direction: column !important;
    align-items: flex-end !important;
}
[data-testid="stColumn"]:has(.st-key-main_menu_popover),
[data-testid="stHorizontalBlock"]:has(.st-key-main_menu_popover),
[data-testid="stVerticalBlock"]:has(.st-key-main_menu_popover),
[data-testid="stContainer"]:has(.st-key-main_menu_popover) {
    overflow: visible !important;
}

/* --- Hamburger icon ----------------------------------------------------- */
.st-key-main_menu_toggle {
    z-index: 51 !important;
    position: relative !important;
}
.st-key-main_menu_toggle button {
    background: transparent !important;
    color: #334155 !important;
    border: 1px solid rgba(15, 23, 42, 0.14) !important;
    border-radius: 10px !important;
    box-shadow: none !important;
    font-weight: 700 !important;
    font-size: 1.1rem !important;
    line-height: 1 !important;
    padding: 0.45rem 0.7rem !important;
    min-height: 2.5rem !important;
    transition:
        background-color 160ms ease-out,
        border-color 160ms ease-out,
        color 160ms ease-out,
        box-shadow 160ms ease-out !important;
}
.st-key-main_menu_toggle button:hover {
    background: rgba(0, 120, 212, 0.08) !important;
    color: #0078d4 !important;
    border-color: rgba(0, 120, 212, 0.45) !important;
}
.st-key-main_menu_toggle button:focus,
.st-key-main_menu_toggle button:focus-visible {
    box-shadow: 0 0 0 2px rgba(0, 120, 212, 0.25) !important;
    outline: none !important;
}

/* --- Floating dropdown panel ------------------------------------------- */
/* Absolute child of `.st-key-main_menu_popover`; `top: 100%; right: 0`
   anchors it directly below the hamburger and to the wrapper's right
   edge. No `position: fixed`, no hardcoded pixel offsets. */
@keyframes main-menu-dropdown-in {
    from { opacity: 0; transform: translateY(-4px); }
    to   { opacity: 1; transform: translateY(0);    }
}
.st-key-main_menu_panel,
div.st-key-main_menu_panel,
[data-testid="stElementContainer"].st-key-main_menu_panel {
    position: absolute !important;
    top: 100% !important;
    right: 0 !important;
    left: auto !important;
    bottom: auto !important;
    width: 260px !important;
    min-width: 220px !important;
    max-width: calc(100vw - 1.5rem) !important;
    /* Grow to fit all 8 items by default. If the viewport is shorter than
       the menu, fall back to an internal scrollbar so Logout is always
       reachable. */
    height: auto !important;
    max-height: calc(100vh - 5rem) !important;
    overflow-x: hidden !important;
    overflow-y: auto !important;
    margin: 0.5rem 0 0 0 !important;
    /* 24 px bottom padding so Logout is never flush with the panel edge. */
    padding: 0.5rem 0.5rem 1.5rem 0.5rem !important;
    background: #ffffff !important;
    border: 1px solid rgba(15, 23, 42, 0.08) !important;
    border-radius: 12px !important;
    box-shadow:
        0 20px 40px rgba(15, 23, 42, 0.16),
        0 6px 14px rgba(15, 23, 42, 0.08) !important;
    z-index: 9999 !important;
    animation: main-menu-dropdown-in 160ms cubic-bezier(0.16, 1, 0.3, 1) both !important;
    will-change: transform, opacity;
}

/* Override any inherited height constraints from Streamlit's inner
   containers so the panel grows to fit all menu items. */
.st-key-main_menu_panel [data-testid="stContainer"],
.st-key-main_menu_panel [data-testid="stVerticalBlock"] {
    height: auto !important;
    max-height: none !important;
    min-height: 0 !important;
}

/* Slim native scrollbar so the fallback overflow case looks intentional */
.st-key-main_menu_panel::-webkit-scrollbar { width: 6px; }
.st-key-main_menu_panel::-webkit-scrollbar-thumb {
    background: rgba(15, 23, 42, 0.18);
    border-radius: 999px;
}
.st-key-main_menu_panel::-webkit-scrollbar-track { background: transparent; }

/* Section header / footer */
.main-menu-drawer .menu-header {
    color: #64748b;
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.25rem 0.75rem 0.45rem 0.75rem;
    margin: 0 0 0.3rem 0;
    border-bottom: 1px solid rgba(15, 23, 42, 0.06);
}
.main-menu-drawer .menu-footer {
    color: #94a3b8;
    font-size: 0.68rem;
    margin: 0.3rem 0 0 0;
    padding: 0.4rem 0.75rem 0.25rem 0.75rem;
    border-top: 1px solid rgba(15, 23, 42, 0.06);
    text-align: center;
    letter-spacing: 0.04em;
}

/* Streamlit wraps each child in an stElementContainer; collapse those gaps
   so the menu reads as a tight vertical list. */
.st-key-main_menu_panel [data-testid="stVerticalBlock"] {
    gap: 0 !important;
}
.st-key-main_menu_panel [data-testid="stElementContainer"] {
    margin: 0 !important;
}

/* --- Menu items: clean vertical list of clickable links ----------------- */
.st-key-main_menu_panel button[kind="secondary"],
.st-key-main_menu_panel button[kind="primary"] {
    background: transparent !important;
    color: #1f2937 !important;
    border: none !important;
    border-radius: 8px !important;
    text-align: left !important;
    justify-content: flex-start !important;
    font-weight: 500 !important;
    font-size: 0.92rem !important;
    padding: 0.75rem 1.25rem !important;          /* = 12px 20px */
    min-height: auto !important;
    width: 100% !important;
    box-shadow: none !important;
    transition: background-color 140ms ease-out, color 140ms ease-out !important;
}
.st-key-main_menu_panel button[kind="secondary"]:hover,
.st-key-main_menu_panel button[kind="primary"]:hover {
    background: rgba(0, 120, 212, 0.08) !important;
    color: #0078d4 !important;
    border: none !important;
}
.st-key-main_menu_panel button[kind="secondary"]:focus,
.st-key-main_menu_panel button[kind="primary"]:focus,
.st-key-main_menu_panel button[kind="secondary"]:focus-visible,
.st-key-main_menu_panel button[kind="primary"]:focus-visible {
    background: rgba(0, 120, 212, 0.10) !important;
    color: #0078d4 !important;
    box-shadow: none !important;
    outline: none !important;
}

/* Divider between the info links and the Logout row */
.st-key-main_menu_panel hr {
    margin: 0.65rem 0 0.45rem 0 !important;
    border: 0 !important;
    border-top: 1px solid rgba(15, 23, 42, 0.12) !important;
}

/* Logout inherits the same `button[kind="secondary"]` styling as the other
   items — same color, weight, hover/focus tint. Only the 🚪 icon in the
   label distinguishes it. The <hr/> above provides the visual separation. */

/* --- Dialog body styling ------------------------------------------------ */
.main-menu-dialog-body {
    font-size: 0.95rem;
    line-height: 1.65;
    color: #1f2937;
}
.main-menu-dialog-body .eyebrow {
    display: inline-block;
    background: rgba(0, 120, 212, 0.10);
    color: #0078d4;
    font-weight: 700;
    font-size: 0.7rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.22rem 0.6rem;
    border-radius: 999px;
    margin-bottom: 0.8rem;
}
.main-menu-dialog-body p { margin: 0 0 0.85rem 0; }
.main-menu-dialog-body strong { color: #0f172a; }
.main-menu-dialog-body em { color: #475569; }
.main-menu-dialog-body code {
    background: #f1f5f9 !important;
    color: #0078d4 !important;
    padding: 0.05rem 0.4rem;
    border-radius: 4px;
    font-size: 0.85em;
}

/* --- Dark-mode friendly fallbacks --------------------------------------- */
@media (prefers-color-scheme: dark) {
    .st-key-main_menu_toggle button {
        color: #e2e8f0 !important;
        border-color: rgba(148, 163, 184, 0.25) !important;
    }
    .st-key-main_menu_toggle button:hover {
        color: #38bdf8 !important;
        border-color: rgba(56, 189, 248, 0.55) !important;
        background: rgba(56, 189, 248, 0.10) !important;
    }
    .st-key-main_menu_panel,
    div.st-key-main_menu_panel,
    [data-testid="stElementContainer"].st-key-main_menu_panel {
        background: #0f172a !important;
        border-color: rgba(148, 163, 184, 0.18) !important;
        box-shadow:
            0 20px 40px rgba(2, 6, 23, 0.55),
            0 6px 14px rgba(2, 6, 23, 0.4) !important;
    }
    .main-menu-drawer .menu-header { color: #94a3b8; border-bottom-color: rgba(148, 163, 184, 0.18); }
    .main-menu-drawer .menu-footer { color: #64748b; border-top-color: rgba(148, 163, 184, 0.18); }
    .st-key-main_menu_panel button[kind="secondary"],
    .st-key-main_menu_panel button[kind="primary"] { color: #e2e8f0 !important; }
    .st-key-main_menu_panel button[kind="secondary"]:hover,
    .st-key-main_menu_panel button[kind="primary"]:hover {
        background: rgba(56, 189, 248, 0.10) !important;
        color: #38bdf8 !important;
    }
    .st-key-main_menu_panel hr { border-top-color: rgba(148, 163, 184, 0.22) !important; }
    .st-key-main_menu_panel::-webkit-scrollbar-thumb {
        background: rgba(148, 163, 184, 0.25) !important;
    }
    .main-menu-dialog-body { color: #e2e8f0; }
    .main-menu-dialog-body strong { color: #f8fafc; }
    .main-menu-dialog-body em { color: #cbd5e1; }
    .main-menu-dialog-body code { background: rgba(56, 189, 248, 0.12) !important; color: #38bdf8 !important; }
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _render_dialog_shell(eyebrow: str, body_html: str, close_key: str) -> None:
    """Render the shared body chrome and close action for menu dialogs."""
    st.markdown(
        f"""
<div class="main-menu-dialog-body">
  <span class="eyebrow">{eyebrow}</span>
  {body_html}
</div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")
    if st.button("Close", key=close_key, type="primary", width="stretch"):
        st.rerun()


@st.dialog("Help · Command Center", width="large")
def _dialog_help() -> None:
    _render_dialog_shell(
        "Help",
        "<p>Welcome to the <strong>AnonifyDB Command Center</strong>. "
        "Use <strong>Scan</strong> to map your database structure, "
        "<strong>Plan</strong> to define anonymization rules with AI assistance, "
        "and <strong>Execute</strong> to run the pipeline.</p>"
        "<p>For technical issues, check the <code>.log</code> files in the project root.</p>",
        close_key="close_menu_dialog_help",
    )


@st.dialog("About AnonifyDB", width="large")
def _dialog_about() -> None:
    _render_dialog_shell(
        "About",
        "<p><strong>AnonifyDB v1.0</strong> — an engineering tool for creating "
        "<strong>‘Structural Twin’</strong> database copies.</p>"
        "<p>Specialized for the <strong>DACH market</strong> and "
        "<strong>GDPR compliance</strong>, enabling secure LLM usage over "
        "sensitive data without leakage risks.</p>",
        close_key="close_menu_dialog_about",
    )


@st.dialog("Feedback", width="large")
def _dialog_feedback() -> None:
    _render_dialog_shell(
        "Feedback",
        "<p>Your feedback directly impacts the development of "
        "<strong>ADB Proxy</strong> and <strong>AI Scan</strong> selectivity.</p>"
        "<p>Send us your thoughts on <strong>foreign key mapping accuracy</strong> "
        "and <strong>interface intuitiveness</strong>.</p>",
        close_key="close_menu_dialog_feedback",
    )


@st.dialog("Priority Support", width="large")
def _dialog_support() -> None:
    _render_dialog_shell(
        "Support",
        "<p>As an early-stage user (<strong>Raising Starts</strong> program), you have "
        "<strong>direct priority support</strong> for integrating the "
        "<strong>Azure Managed Application (AMA)</strong> model within your tenant.</p>",
        close_key="close_menu_dialog_support",
    )


@st.dialog("Contact", width="large")
def _dialog_contact() -> None:
    _render_dialog_shell(
        "Contact",
        "<p>For enterprise inquiries and partnerships in the <strong>DACH region</strong>, "
        "contact us at <code>[Your Email/Website]</code>.</p>"
        "<p><em>“Built by Data Engineers for Data Engineers.”</em></p>",
        close_key="close_menu_dialog_contact",
    )


@st.dialog("Privacy", width="large")
def _dialog_privacy() -> None:
    _render_dialog_shell(
        "Privacy",
        "<p>AnonifyDB operates on a <strong>‘Zero-Trust’</strong> principle.</p>"
        "<p>Your data <strong>never leaves the client’s tenant</strong>; our "
        "anonymization logic runs locally or within your locked resource group.</p>",
        close_key="close_menu_dialog_privacy",
    )


@st.dialog("Terms of Use", width="large")
def _dialog_terms() -> None:
    _render_dialog_shell(
        "Terms",
        "<p>License for using AnonifyDB software as part of the "
        "<strong>prototype testing phase</strong>.</p>"
        "<p><strong>Reverse engineering</strong> of the <code>core/</code> logic "
        "defining transformation rules is <strong>strictly prohibited</strong>.</p>",
        close_key="close_menu_dialog_terms",
    )


_DIALOG_FUNCS = {
    "help":     _dialog_help,
    "about":    _dialog_about,
    "feedback": _dialog_feedback,
    "support":  _dialog_support,
    "contact":  _dialog_contact,
    "privacy":  _dialog_privacy,
    "terms":    _dialog_terms,
}


def _perform_logout() -> None:
    """Mirror of the original top-right Logout button behavior."""
    st.session_state["authenticated"] = False
    st.session_state["logged_in"] = False
    st.rerun()


def render_main_menu() -> None:
    """Render the hamburger toggle + dropdown panel and route active dialog.

    Layout pattern:

        <div class="st-key-main_menu_popover">      ← position: relative
          <div class="st-key-main_menu_toggle">    ← z-index: 51
            <button>☰</button>
          </div>
          <div class="st-key-main_menu_panel">     ← position: absolute,
            ...menu items, divider, logout...        top: 100%; right: 0;
          </div>                                     z-index: 50
        </div>

    The panel is a real DOM child of the wrapper (not a portal), so the
    `position: absolute; top: 100%; right: 0` pattern anchors directly to
    the hamburger icon without any hardcoded viewport offsets.
    """
    _inject_styles()

    if _OPEN_STATE_KEY not in st.session_state:
        st.session_state[_OPEN_STATE_KEY] = False

    with st.container(key=_POPOVER_CONTAINER_KEY):
        # Hamburger toggle — single source of truth for the open/closed
        # state. Clicking it flips `main_menu_open`; the conditional block
        # below re-renders the panel as a sibling DOM child.
        if st.button(
            "☰",
            key=_TOGGLE_BUTTON_KEY,
            help="Open / close menu",
        ):
            st.session_state[_OPEN_STATE_KEY] = not st.session_state.get(
                _OPEN_STATE_KEY, False
            )

        if st.session_state.get(_OPEN_STATE_KEY):
            with st.container(key=_PANEL_CONTAINER_KEY):
                st.markdown(
                    "<div class='main-menu-drawer'>"
                    "<div class='menu-header'>AnonifyDB · Command Center</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )

                for key, label, icon in _MENU_ITEMS:
                    if st.button(
                        f"{icon}  {label}",
                        key=f"main_menu_item_{key}",
                        width="stretch",
                    ):
                        st.session_state[_DIALOG_STATE_KEY] = key
                        st.session_state[_OPEN_STATE_KEY] = False
                        st.rerun()

                st.divider()

                # Same kwargs as the other menu items (no `type`, no `help`)
                # so the rendered DOM matches `main_menu_item_help` exactly
                # — only the icon distinguishes it visually.
                if st.button(
                    "🚪  Logout",
                    key="main_menu_item_logout",
                    width="stretch",
                ):
                    st.session_state[_OPEN_STATE_KEY] = False
                    _perform_logout()

                st.markdown(
                    "<div class='main-menu-drawer'>"
                    "<div class='menu-footer'>v1.0 · Built for engineers</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )

    active = st.session_state.pop(_DIALOG_STATE_KEY, None)
    dialog_fn = _DIALOG_FUNCS.get(active)
    if dialog_fn is not None:
        dialog_fn()
