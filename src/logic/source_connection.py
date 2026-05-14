# -*- coding: utf-8 -*-
"""Source database URL composition and session-state synchronization.

All functions are free of Streamlit UI calls; callers pass explicit session
mappings so the same logic can be unit-tested without running a Streamlit app.
"""

from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping
from urllib.parse import quote_plus, urlparse


def parse_env_database_url() -> dict[str, str]:
    """Parse ``SOURCE_DB_URL`` or ``DATABASE_URL`` into connection components.

    Returns
    -------
    dict[str, str]
        Keys ``conn_string``, ``host``, ``port``, ``database_name``, ``user``,
        and ``password``. Missing or invalid URLs yield an empty mapping.
    """
    raw = str(os.getenv("SOURCE_DB_URL") or os.getenv("DATABASE_URL", "")).strip()
    if not raw:
        return {}
    try:
        parsed = urlparse(raw)
    except Exception:
        return {}
    return {
        "conn_string": raw,
        "host": parsed.hostname or "",
        "port": str(parsed.port or ""),
        "database_name": (parsed.path.lstrip("/") if parsed.path else "") or "",
        "user": parsed.username or "",
        "password": parsed.password or "",
    }


def resolve_postgresql_source_url(state: Mapping[str, Any]) -> str:
    """Resolve a PostgreSQL URL from session fields, then from environment.

    Falls back to ``SOURCE_DB_URL`` / ``DATABASE_URL`` when discrete widget
    keys are empty (e.g. after a cold start with a confirmed source in ``.env``).

    Returns
    -------
    str
        Non-empty URL when resolvable, otherwise ``""``.
    """
    direct = str(compose_postgresql_source_url(state) or "").strip()
    if direct:
        return direct
    env = parse_env_database_url()
    return str(env.get("conn_string", "") or "").strip()


def compose_postgresql_source_url(state: Mapping[str, Any]) -> str:
    """Build a PostgreSQL URL from a connection string or discrete fields.

    Parameters
    ----------
    state:
        Mapping containing optional keys ``db_source_conn_string``, ``conn_host``,
        ``conn_database_name``, ``conn_port``, ``conn_user``, ``conn_password``.

    Returns
    -------
    str
        A ``postgresql://`` URL, or an empty string when required parts are absent.
    """
    cs = str(state.get("db_source_conn_string", "")).strip()
    if cs:
        return cs
    host = str(state.get("conn_host", "") or "").strip()
    dbn = str(state.get("conn_database_name", "") or "").strip()
    port = str(state.get("conn_port", "") or "5432").strip()
    user = str(state.get("conn_user", "") or "").strip()
    password = str(state.get("conn_password", "") or "")
    if not (host and dbn):
        return ""
    user_q = quote_plus(user)
    if password:
        auth = f"{user_q}:{quote_plus(password)}"
    else:
        auth = user_q
    return f"postgresql://{auth}@{host}:{port}/{dbn}"


def sync_db_config_from_session(
    state: MutableMapping[str, Any],
    *,
    default_domain: str,
) -> None:
    """Mirror discrete connection fields into ``db_config`` for downstream code.

    Parameters
    ----------
    state:
        Streamlit session mapping to read/write.
    default_domain:
        Fallback when ``source_domain`` is unset (typically the first domain option).
    """
    state["db_config"] = {
        "database_type": state.get("db_source_type", "PostgreSQL"),
        "data_domain": state.get("source_domain", default_domain),
        "connection": {
            "host": state.get("conn_host", ""),
            "port": state.get("conn_port", ""),
            "database_name": state.get("conn_database_name", ""),
            "user": state.get("conn_user", ""),
            "password": state.get("conn_password", ""),
        },
    }
    state["data_source_database_type"] = state.get("db_source_type", "PostgreSQL")


def seed_db_connection_fields_from_env(state: MutableMapping[str, Any]) -> None:
    """Populate DB connection widget keys from environment defaults once.

    Uses :func:`parse_env_database_url` and ``setdefault`` so existing user
    edits in ``state`` are never overwritten.

    When the Source tab is **confirmed (locked)** and both the connection string
    and discrete host/database fields are empty, rehydrate from ``.env`` on every
    run so ``DATABASE_URL`` / ``SOURCE_DB_URL`` keeps session aligned with the
    persisted confirmation (Streamlit ``setdefault`` does not replace explicit
    empty strings from a prior session).

    Parameters
    ----------
    state:
        Session mapping receiving seeded keys.
    """
    env_defaults = parse_env_database_url()
    for ss_key, env_key in (
        ("db_source_conn_string", "conn_string"),
        ("conn_host", "host"),
        ("conn_port", "port"),
        ("conn_database_name", "database_name"),
        ("conn_user", "user"),
    ):
        state.setdefault(ss_key, env_defaults.get(env_key, ""))

    url = str(env_defaults.get("conn_string") or "").strip()
    locked = bool(state.get("source_confirmed"))
    host_empty = not str(state.get("conn_host", "") or "").strip()
    dbn_empty = not str(state.get("conn_database_name", "") or "").strip()
    cs_empty = not str(state.get("db_source_conn_string", "") or "").strip()
    if locked and url and host_empty and dbn_empty and cs_empty:
        for ss_key, env_key in (
            ("db_source_conn_string", "conn_string"),
            ("conn_host", "host"),
            ("conn_port", "port"),
            ("conn_database_name", "database_name"),
            ("conn_user", "user"),
        ):
            state[ss_key] = env_defaults.get(env_key, "")


def connection_test_and_init_disabled(locked: bool, state: Mapping[str, Any]) -> bool:
    """Return whether Test Connection / Initialize Session should be disabled.

    When the source is not confirmed, actions stay enabled whenever the user
    can supply parameters. When confirmed (locked), actions remain enabled as
    long as a URL can be resolved from session **or** from ``SOURCE_DB_URL`` /
    ``DATABASE_URL`` in the environment, so **Change** is not required merely to
    unlock Initialize Session.

    Parameters
    ----------
    locked:
        ``True`` when the Source tab is in confirmed (read-only) mode.
    state:
        Current session mapping.

    Returns
    -------
    bool
        ``True`` if the buttons should be disabled.
    """
    if not locked:
        return False
    if bool(resolve_postgresql_source_url(state).strip()):
        return False
    # Extra guard: env URL present even if session keys are momentarily stale.
    return not bool(str(parse_env_database_url().get("conn_string") or "").strip())
