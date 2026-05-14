# -*- coding: utf-8 -*-
"""Source tab entrypoint: vertical flow — selection → parameters → action toolbar."""

from __future__ import annotations

from typing import Any

from src.logic.app_state import AppState
from src.ui.source.source_session_init import handle_initialization
from src.ui.source import source_api_log_panel as api_log
from src.ui.source import source_control_bar as chrome
from src.ui.source import source_database_panel as db_panel
from src.ui.source import source_file_panel as file_panel
from src.ui.source import source_utils as su

__all__ = ["render_source_tab", "handle_initialization"]


def _ensure_source_locked_key(app: AppState) -> None:
    """Seed ``source_locked`` once so core connection widgets have a defined default.

    Defaults to **edit mode** (``False``) on a fresh session. It becomes ``True``
    after **Confirm Source** (see ``handle_confirm_source_click``) or when the
    user returns to **Database** while the source is still confirmed (see
    ``_on_source_type_change`` in ``source_control_bar``). ``Change`` clears it.
    """
    m = app.mapping
    if "source_locked" not in m:
        m["source_locked"] = bool(m.get("source_confirmed"))


def render_source_tab(db: Any, app: AppState | None = None) -> None:
    """Render the Source tab: source type, domain, parameters, action toolbar, log.

    Vertical order:

    1. **Active Data Source** (Database | File | API) — always enabled.
    2. Domain picker (global heuristics).
    3. Mode-specific parameters (connection / file / API forms); ``source_locked``
       governs core fields inside those panels.
    4. **Action toolbar** — four buttons in one row; **Initialize Session** /
       **Test Connection** / **Confirm** / **Change** use ``on_click`` callbacks
       (see ``source_session_init`` and ``source_control_bar``) so session work
       runs at click time.
    5. Source event log.

    Destination mode (in-memory vs physical plan DB) is shown only on the **Source vs Anon** tab,
    where preview and comparison happen.
    """
    app = app or AppState()
    su.init_source_state(app)
    _ensure_source_locked_key(app)
    su.inject_source_control_bar_styles()
    locked = app.get_source_confirmed()

    # --- Top: source type (always switchable) + domain ---
    chrome.render_master_source_selector(app)
    chrome.render_source_domain_picker(app, locked)

    # --- Middle: parameters for the active mode ---
    if app.get_normalized_source_type() == "Database":
        su.sync_db_config_dict_from_session(app.mapping)

    active = app.get_normalized_source_type()
    if active == "Database":
        db_panel.render_db_engine_subselector(locked)
        db_panel.render_db_source_section(db, app, locked)
    elif active == "File":
        file_panel.render_file_format_subselector(locked)
        file_panel.render_file_source_section(app, locked)
    elif active == "API":
        api_log.render_api_source_section(app, locked)

    # --- Bottom: action toolbar (single horizontal row) ---
    chrome.render_source_action_toolbar(db, app, locked)

    api_log.render_source_log_section(app, locked)
