# -*- coding: utf-8 -*-
"""API source form, response monitor, and Source event log for the Source tab."""

from __future__ import annotations

import json
import time
from typing import Any

import pandas as pd
import streamlit as st

from src.logic.app_state import AppState
from src.ui.source import source_utils as su

__all__ = ["render_api_source_section", "render_source_log_section"]


def detect_response_language(content_type: str) -> str:
    """Pick a ``st.code`` language hint from a ``Content-Type`` header."""
    ct = (content_type or "").lower()
    if "json" in ct:
        return "json"
    if "xml" in ct:
        return "xml"
    if "html" in ct:
        return "html"
    if "javascript" in ct:
        return "javascript"
    return "text"


def render_api_source_section(app: AppState, locked: bool = False) -> None:
    """Render the API Source expander (request builder + response monitor)."""
    m = app.mapping
    api_core = bool(m.get("source_locked", False))
    with st.expander("API Source", expanded=True):
        st.caption(
            "Define an HTTP API as a data source. Provides response monitoring and a "
            "preview panel — not yet plumbed into Mappings / Export."
        )

        c1, c2 = st.columns([3, 1])
        with c1:
            st.text_input(
                "URL",
                key="api_source_url",
                placeholder="https://api.example.com/customers",
                disabled=locked or api_core,
            )
        with c2:
            st.selectbox("Method", options=su.HTTP_METHODS, key="api_source_method", disabled=locked)

        c3, c4 = st.columns(2)
        with c3:
            st.text_input("API Key", type="password", key="api_source_api_key", disabled=locked or api_core)
        with c4:
            st.text_input("Secret", type="password", key="api_source_secret", disabled=locked or api_core)

        st.markdown("**Headers**")
        headers_state = m.setdefault(
            "api_source_headers",
            pd.DataFrame({"key": ["Content-Type"], "value": ["application/json"]}),
        )
        headers_df = st.data_editor(
            headers_state,
            num_rows="dynamic",
            use_container_width=True,
            key="api_source_headers_editor",
            disabled=locked or api_core,
        )
        m["api_source_headers"] = headers_df

        st.text_area(
            "Body (JSON or raw)",
            key="api_source_body",
            height=120,
            placeholder='{"q": "select customers"}',
            disabled=locked or api_core,
        )

        if st.button("Send Request", key="api_source_send_btn", disabled=locked):
            try:
                import requests  # type: ignore  # noqa: WPS433
            except ImportError:
                st.error(
                    "The `requests` library is not installed. "
                    "Add it to `requirements.txt` or run `pip install requests`."
                )
                su.log_source_event(m, "api", "send_failed", error="requests not installed")
            else:
                url = str(m.get("api_source_url", "")).strip()
                method = str(m.get("api_source_method", "GET")).upper()
                if not url:
                    st.error("URL is required.")
                else:
                    headers: dict[str, str] = {}
                    try:
                        for _, row in headers_df.iterrows():
                            k = str(row.get("key", "")).strip()
                            v = str(row.get("value", "")).strip()
                            if k:
                                headers[k] = v
                    except Exception:  # noqa: BLE001
                        pass
                    api_key = str(m.get("api_source_api_key", "")).strip()
                    if api_key and "Authorization" not in headers:
                        headers["Authorization"] = f"Bearer {api_key}"
                    body_raw = str(m.get("api_source_body", "")).strip()
                    body_payload: Any = None
                    if body_raw:
                        try:
                            body_payload = json.loads(body_raw)
                        except json.JSONDecodeError:
                            body_payload = body_raw

                    started = time.perf_counter()
                    try:
                        resp = requests.request(
                            method,
                            url,
                            headers=headers,
                            json=body_payload if isinstance(body_payload, (dict, list)) else None,
                            data=body_payload if isinstance(body_payload, str) else None,
                            timeout=30,
                        )
                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        body_bytes = resp.content or b""
                        try:
                            body_text = body_bytes.decode(resp.encoding or "utf-8", errors="replace")
                        except Exception:  # noqa: BLE001
                            body_text = repr(body_bytes[:4096])
                        m["api_source_last_response"] = {
                            "status": int(resp.status_code),
                            "elapsed_ms": float(elapsed_ms),
                            "size": int(len(body_bytes)),
                            "headers": dict(resp.headers or {}),
                            "body": body_text,
                            "content_type": str(resp.headers.get("Content-Type", "")),
                        }
                        su.log_source_event(
                            m,
                            "api",
                            "request",
                            method=method,
                            url=url,
                            status=int(resp.status_code),
                            elapsed_ms=round(elapsed_ms, 1),
                            size=int(len(body_bytes)),
                        )
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Request failed: {exc}")
                        su.log_source_event(
                            m,
                            "api",
                            "request_failed",
                            method=method,
                            url=url,
                            error=str(exc),
                        )

        last = m.get("api_source_last_response")
        if last:
            st.divider()
            st.markdown("**Response Monitor**")
            m1, m2, m3 = st.columns(3)
            m1.metric("Status", last["status"])
            m2.metric("Response time", f"{last['elapsed_ms']:.0f} ms")
            m3.metric("Size", su.format_size(last["size"]))

            st.caption("Response headers")
            headers_dict = last.get("headers", {})
            if headers_dict:
                st.dataframe(
                    pd.DataFrame(
                        {
                            "header": list(headers_dict.keys()),
                            "value": list(headers_dict.values()),
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

            st.caption("Response body")
            lang = detect_response_language(last.get("content_type", ""))
            body_preview = str(last.get("body", ""))
            if len(body_preview) > 4000:
                body_preview = body_preview[:4000] + "\n... (truncated)"
            st.code(body_preview, language=lang)


def render_source_log_section(app: AppState, locked: bool = False) -> None:
    """Render the bounded Source event log."""
    m = app.mapping
    with st.expander("Source Log", expanded=False):
        log: list = list(m.get("source_event_log", []))
        st.caption(f"Most recent {len(log)} source events (bounded to {su.LOG_MAX_ENTRIES}).")

        ctl1, ctl2 = st.columns([1, 1])
        with ctl1:
            if st.button("Clear log", key="source_log_clear_btn", disabled=locked):
                m["source_event_log"] = []
                st.rerun()
        with ctl2:
            if log:
                st.download_button(
                    "Download log (JSON)",
                    data=json.dumps(log, indent=2),
                    file_name="anonifydb_source_log.json",
                    mime="application/json",
                    key="source_log_download_btn",
                    disabled=locked,
                )

        if not log:
            st.info(
                "No source events yet. Loading a file, sending an API request, or "
                "testing the DB connection will populate this log."
            )
            return

        df_log = pd.DataFrame(log).iloc[::-1].reset_index(drop=True)
        st.dataframe(df_log, use_container_width=True, hide_index=True)
