# -*- coding: utf-8 -*-
"""Source tab entrypoint: orchestrates control bar, domain, and source panels."""

from __future__ import annotations

from typing import Any

from src.logic.app_state import AppState
from src.ui.source import source_api_log_panel as api_log
from src.ui.source import source_control_bar as chrome
from src.ui.source import source_database_panel as db_panel
from src.ui.source import source_file_panel as file_panel
from src.ui.source import source_utils as su

__all__ = ["render_source_tab"]


def render_source_tab(db: Any, app: AppState | None = None) -> None:
    """Render the Source tab (exclusive source type: Database, File, or API).

    Parameters
    ----------
    db:
        Database manager used for schema/table discovery and connection tests.
    app:
        Session façade; defaults to Streamlit session state.
    """
    app = app or AppState()
    su.init_source_state(app)
    locked = app.get_source_confirmed()
    if app.get_normalized_source_type() == "Database":
        su.sync_db_config_dict_from_session(app.mapping)

    chrome.render_source_control_bar_and_domain(db, app, locked)
    chrome.render_master_source_selector(app, locked)

    active = app.get_normalized_source_type()
    if active == "Database":
        db_panel.render_db_engine_subselector(locked)
        db_panel.render_db_source_section(db, app, locked)
    elif active == "File":
        file_panel.render_file_format_subselector(locked)
        file_panel.render_file_source_section(app, locked)
    elif active == "API":
        api_log.render_api_source_section(app, locked)

    api_log.render_source_log_section(app, locked)
