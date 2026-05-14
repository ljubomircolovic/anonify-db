# -*- coding: utf-8 -*-
"""Typed façade over Streamlit ``session_state`` for workflow coordination."""

from __future__ import annotations

from typing import Any, MutableMapping


def get_session_store(explicit: MutableMapping[str, Any] | None = None) -> MutableMapping[str, Any]:
    """Return the backing mapping used for UI session persistence.

    Parameters
    ----------
    explicit:
        Optional mapping (used in tests). When omitted, returns
        ``streamlit.session_state``.

    Returns
    -------
    MutableMapping[str, Any]
        The active session store.
    """
    if explicit is not None:
        return explicit
    import streamlit as st

    return st.session_state


class AppState:
    """Central typed accessors for keys shared by Source and Mappings workflows."""

    __slots__ = ("_m",)

    def __init__(self, store: MutableMapping[str, Any] | None = None) -> None:
        self._m = get_session_store(store)

    @property
    def mapping(self) -> MutableMapping[str, Any]:
        """Underlying mutable session mapping."""
        return self._m

    def get_source_confirmed(self) -> bool:
        """Whether the user confirmed the Source configuration."""
        return bool(self._m.get("source_confirmed"))

    def set_source_confirmed(self, value: bool) -> None:
        """Persist whether the Source tab is locked after confirmation."""
        self._m["source_confirmed"] = bool(value)

    def get_normalized_source_type(self) -> str:
        """Return the canonical active source type (Database / File / API)."""
        from src.ui.source import source_utils as su

        return su.normalize_source_type(self._m.get("source_type"))

    def get_source_connected(self) -> bool:
        """Whether a successful source connectivity check has been recorded."""
        return bool(self._m.get("source_connected"))

    def get_active_plan_db_key(self) -> str | None:
        """Composite key identifying the active plan database, if any."""
        raw = self._m.get("active_plan_db_key")
        return str(raw) if raw else None

    def get_project_initialized(self) -> bool:
        """Whether a plan database has been activated for this session."""
        return bool(self._m.get("project_initialized", False))

    def get_plan_source_binding_key(self) -> str | None:
        """Plan key last bound to source metadata via :func:`bind_plan_metadata_to_source`."""
        raw = self._m.get("plan_source_binding_key")
        return str(raw) if raw else None

    def get_all_tables_list(self) -> list[str]:
        """Ordered table names discovered during source initialization."""
        raw = self._m.get("all_tables_list")
        if isinstance(raw, list):
            return [str(x) for x in raw]
        return []

    def get_selected_schema(self) -> str:
        """Active PostgreSQL schema for planner/execution (defaults to ``public``)."""
        raw = self._m.get("selected_schema")
        return str(raw) if raw else "public"

    def set_selected_schema(self, value: str) -> None:
        """Persist the active schema name for downstream tabs."""
        self._m["selected_schema"] = value

    def ensure_dict(self, key: str) -> dict[str, Any]:
        """Return a dict at ``key``, replacing non-dicts with an empty dict."""
        cur = self._m.get(key)
        if not isinstance(cur, dict):
            cur = {}
            self._m[key] = cur
        return cur

    def pop_key(self, key: str, default: Any = None) -> Any:
        """Remove ``key`` from session if present (Streamlit-compatible pop)."""
        if key in self._m:
            val = self._m[key]
            del self._m[key]
            return val
        return default

    def clear_all_keys(self) -> None:
        """Remove every session key (used when closing a project)."""
        for key in list(self._m.keys()):
            del self._m[key]
