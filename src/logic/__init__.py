# -*- coding: utf-8 -*-
"""Application business logic (no Streamlit widgets).

UI modules in ``src.ui`` should call into this package for orchestration,
pure transforms, and session-state mutations that are not tied to layout.
"""

from __future__ import annotations

from src.logic.app_state import AppState, get_session_store
from src.logic.naming import normalize_name_fragment
from src.logic.source_connection import (
    compose_postgresql_source_url,
    connection_test_and_init_disabled,
    parse_env_database_url,
    resolve_postgresql_source_url,
    seed_db_connection_fields_from_env,
    sync_db_config_from_session,
)
from src.logic.workflow import (
    bind_plan_metadata_to_source,
    maybe_auto_bind_plan_to_source,
    render_workflow_readiness_warning,
)

__all__ = [
    "AppState",
    "bind_plan_metadata_to_source",
    "compose_postgresql_source_url",
    "connection_test_and_init_disabled",
    "get_session_store",
    "maybe_auto_bind_plan_to_source",
    "normalize_name_fragment",
    "parse_env_database_url",
    "render_workflow_readiness_warning",
    "resolve_postgresql_source_url",
    "seed_db_connection_fields_from_env",
    "sync_db_config_from_session",
]
