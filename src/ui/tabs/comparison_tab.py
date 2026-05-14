# -*- coding: utf-8 -*-
"""Side-by-side original vs anonymized preview (Mappings workflow tab)."""

from __future__ import annotations

from typing import Any

import streamlit as st

from src.ui.tabs.planner.planner_secrets import resolve_active_plan_seed


def render_comparison_tab(db: Any) -> None:
    """Render the Comparison tab: load a sample, apply rules, show two dataframes."""
    st.subheader("🔍 Side-by-Side Comparison")

    if "current_plan" in st.session_state and "selected_table_info" in st.session_state:
        table_name, schema_name = st.session_state["selected_table_info"]
        current_plan = st.session_state["current_plan"]
        current_salt = resolve_active_plan_seed(db, schema_name, table_name)
        current_locale = st.session_state.get("selected_locale", "de")

        raw_sample = db.read_table(
            table_name,
            schema_name,
            limit=st.session_state.get("last_limit_val", 10),
            where=st.session_state.get(f"where_clause_{table_name}", ""),
        )

        if not raw_sample.empty:
            anon_sample = db.apply_anonymization_rules(
                raw_sample,
                current_plan,
                salt=current_salt,
            )
            notes: list[str] = []

            if notes:
                for n in set(notes):
                    st.info(n)

            c1, c2 = st.columns(2)
            with c1:
                st.write("**📄 Original Data**")
                st.dataframe(raw_sample, width="stretch")
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, width="stretch")

            if st.button("Prepare Full Download (CSV)"):
                with st.spinner("Generating CSV..."):
                    full_df = db.read_table(table_name, schema_name)
                    full_anon = db.apply_anonymization_rules(
                        full_df,
                        current_plan,
                        salt=current_salt,
                    )
                    csv = full_anon.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    st.download_button(
                        label="Click to Download CSV",
                        data=csv,
                        file_name=f"anon_{table_name}.csv",
                        mime="text/csv",
                    )
        else:
            st.warning("No records to compare.")
