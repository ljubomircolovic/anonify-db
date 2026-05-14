# -*- coding: utf-8 -*-
"""File format selector and file upload / preview UI for the Source tab."""

from __future__ import annotations

import csv
import io
import os

import pandas as pd
import streamlit as st

from src.logic.app_state import AppState
from src.ui.source import source_utils as su

__all__ = ["load_file_dataframe", "render_file_format_subselector", "render_file_source_section"]


def load_file_dataframe(
    *,
    raw_bytes: bytes,
    file_type: str,
    encoding: str,
    delimiter: str,
    quotechar: str,
    quoting: int,
    escapechar: str | None,
    doublequote: bool,
    has_header: bool,
) -> pd.DataFrame:
    """Parse uploaded or path bytes into a DataFrame based on ``file_type``."""
    if file_type in {"CSV", "TXT"}:
        text_buf = io.StringIO(raw_bytes.decode(encoding, errors="replace"))
        return pd.read_csv(
            text_buf,
            sep=delimiter or ",",
            quotechar=quotechar or '"',
            quoting=quoting,
            escapechar=(escapechar or None),
            doublequote=bool(doublequote),
            header=0 if has_header else None,
            encoding=encoding,
            engine="python",
        )
    if file_type == "JSON":
        return pd.read_json(io.StringIO(raw_bytes.decode(encoding, errors="replace")))
    if file_type == "XML":
        return pd.read_xml(io.BytesIO(raw_bytes))
    raise ValueError(f"Unsupported file type: {file_type}")


def render_file_format_subselector(locked: bool) -> None:
    """Horizontal format pick when File is the active master source."""
    st.radio(
        "File format",
        options=su.FILE_TYPES,
        horizontal=True,
        key="file_source_type",
        disabled=locked,
        label_visibility="collapsed",
        help="File format used for parsing and preview.",
    )


def render_file_source_section(app: AppState, locked: bool = False) -> None:
    """Render the File Source expander (path/upload, parse, preview)."""
    m = app.mapping
    with st.expander("File Source (CSV / JSON / XML / TXT)", expanded=True):
        st.caption(
            "Define a file source. The file is parsed into a Pandas DataFrame for preview only — "
            "it is not yet routed through Mappings / Export."
        )

        c1, c2 = st.columns([2, 2])
        with c1:
            st.text_input("Name", key="file_source_name", placeholder="customers_csv", disabled=locked)
        with c2:
            st.selectbox("Encoding", options=su.ENCODINGS, key="file_source_encoding", disabled=locked)

        st.text_input(
            "Path",
            key="file_source_path",
            placeholder="/absolute/path/to/file.csv (or use the upload below)",
            disabled=locked,
        )
        uploaded = st.file_uploader(
            "Upload file",
            type=["csv", "json", "xml", "txt"],
            key="file_source_uploader",
            disabled=locked,
        )

        c4, c5, c6, c7 = st.columns(4)
        with c4:
            st.text_input("Delimiter", key="file_source_delimiter", max_chars=4, disabled=locked)
        with c5:
            st.text_input("Quotechar", key="file_source_quotechar", max_chars=4, disabled=locked)
        with c6:
            st.selectbox(
                "Quoting",
                options=list(su.QUOTING_OPTIONS.keys()),
                key="file_source_quoting",
                disabled=locked,
            )
        with c7:
            st.text_input("Escapechar", key="file_source_escapechar", max_chars=4, disabled=locked)

        c8, c9 = st.columns(2)
        with c8:
            st.checkbox("Doublequote", key="file_source_doublequote", disabled=locked)
        with c9:
            st.checkbox("First row is header", key="file_source_has_header", disabled=locked)

        if st.button("Load File", key="file_source_load_btn", disabled=locked):
            raw_bytes: bytes | None = None
            size_bytes: int | None = None
            origin: str = ""
            if uploaded is not None:
                raw_bytes = uploaded.getvalue()
                size_bytes = len(raw_bytes)
                origin = f"upload:{uploaded.name}"
            else:
                path = str(m.get("file_source_path", "")).strip()
                if path and os.path.isfile(path):
                    try:
                        with open(path, "rb") as fh:
                            raw_bytes = fh.read()
                        size_bytes = os.path.getsize(path)
                        origin = f"path:{path}"
                    except OSError as exc:
                        st.error(f"Could not read file: {exc}")
                else:
                    st.error("Provide a valid path or upload a file.")

            if raw_bytes is not None:
                try:
                    df = load_file_dataframe(
                        raw_bytes=raw_bytes,
                        file_type=str(m.get("file_source_type", "CSV")),
                        encoding=str(m.get("file_source_encoding", "utf-8")),
                        delimiter=str(m.get("file_source_delimiter", ",")),
                        quotechar=str(m.get("file_source_quotechar", '"')),
                        quoting=su.QUOTING_OPTIONS.get(
                            str(m.get("file_source_quoting", "QUOTE_MINIMAL")),
                            csv.QUOTE_MINIMAL,
                        ),
                        escapechar=str(m.get("file_source_escapechar", "")) or None,
                        doublequote=bool(m.get("file_source_doublequote", True)),
                        has_header=bool(m.get("file_source_has_header", True)),
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Failed to parse file: {exc}")
                    su.log_source_event(m, "file", "load_failed", origin=origin, error=str(exc))
                else:
                    m["file_source_df"] = df
                    m["file_source_size"] = size_bytes
                    m["file_source_column_map"] = pd.DataFrame(
                        {"source": list(df.columns), "target": list(df.columns)}
                    )
                    su.log_source_event(
                        m,
                        "file",
                        "load",
                        origin=origin,
                        rows=int(df.shape[0]),
                        cols=int(df.shape[1]),
                        bytes=size_bytes,
                    )
                    st.success(f"Loaded {df.shape[0]} rows × {df.shape[1]} columns from {origin}.")

        df = m.get("file_source_df")
        if isinstance(df, pd.DataFrame):
            m1, m2, m3 = st.columns(3)
            m1.metric("Columns", int(df.shape[1]))
            m2.metric("Rows", int(df.shape[0]))
            m3.metric("Size", su.format_size(m.get("file_source_size")))

            st.markdown("**Column selection**")
            selected_cols = st.multiselect(
                "Columns to keep",
                options=list(df.columns),
                default=list(df.columns),
                key="file_source_selected_columns",
                disabled=locked,
            )

            st.markdown("**Row filter** (pandas `query` syntax) + Limit")
            qcol, lcol = st.columns([3, 1])
            with qcol:
                where_expr = st.text_input(
                    "WHERE (pandas query)",
                    key="file_source_where",
                    placeholder="age > 30 and country == 'DE'",
                    disabled=locked,
                )
            with lcol:
                row_limit = st.number_input(
                    "LIMIT",
                    min_value=1,
                    max_value=1_000_000,
                    value=int(m.get("file_source_row_limit", 100)),
                    step=10,
                    key="file_source_row_limit",
                    disabled=locked,
                )

            st.markdown("**Column Mapping** (Source → Target)")
            mapping_df = m.get(
                "file_source_column_map",
                pd.DataFrame({"source": list(df.columns), "target": list(df.columns)}),
            )
            edited_map = st.data_editor(
                mapping_df,
                num_rows="fixed",
                use_container_width=True,
                disabled=True if locked else ["source"],
                key="file_source_column_map_editor",
            )
            m["file_source_column_map"] = edited_map

            try:
                preview = df.copy()
                if selected_cols:
                    preview = preview[selected_cols]
                if where_expr.strip():
                    preview = preview.query(where_expr)
                preview = preview.head(int(row_limit)).head(20)

                rename_map = {
                    str(row["source"]): str(row["target"])
                    for _, row in edited_map.iterrows()
                    if str(row["source"]) in preview.columns
                    and str(row["target"]).strip()
                    and str(row["target"]) != str(row["source"])
                }
                if rename_map:
                    preview = preview.rename(columns=rename_map)

                st.divider()
                st.markdown("**Preview** (first 20 records after filter/mapping)")
                st.dataframe(preview, use_container_width=True)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Preview failed: {exc}")
