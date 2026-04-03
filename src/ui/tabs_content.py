# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd


def render_explorer_tab(db):
    if 'current_df' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        st.subheader(f"📊 Raw Data Explorer: {schema_name}.{table_name}")

        if not st.session_state['current_df'].empty:
            st.info(f"Showing {len(st.session_state['current_df'])} rows based on your sidebar filter.")
            st.dataframe(st.session_state['current_df'], use_container_width=True)
        else:
            st.warning(f"⚠️ No records found! The filter returned 0 rows.")

def render_fk_explanation():
    """Prikazuje detaljno objašnjenje strategija za strane ključeve."""
    st.markdown("""
    ### 📘 HASH vs KEEP: Referencijalni Integritet

    Kada anonimizujete tabele koje su povezane (npr. `customers` i `orders`), ključevi moraju ostati usklađeni.

    | Karakteristika | **HASH (Preporučeno)** | **KEEP (Originalni ID)** |
    | :--- | :--- | :--- |
    | **Bezbednost** | **Visoka.** ID se ne može vratiti u original. | **Niska.** ID-jevi ostaju javni. |
    | **Integritet** | **Savršen.** JOIN-ovi rade (uz isti Salt). | **Savršen.** JOIN-ovi rade. |
    | **Tip podatka** | Postaje **String** (Hash). | Ostaje **Integer**. |
    | **Rizik** | Minimalan. | Visok (mogućnost povezivanja podataka). |

    **Preporuka:** Koristite **HASH** za sve Foreign Key kolone kako biste osigurali maksimalnu privatnost uz zadržavanje funkcionalnosti baze.
    """)

def render_planner_tab(db):
    if 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        editor_key = f"plan_editor_{table_name}"

        st.subheader("🛠️ Review & Finalize Plan")
        # Koristimo Markdown sa HTML-om za veću kontrolu nad stilom
        st.markdown(f"""
            <div style="background-color: #e8f4f8; padding: 15px; border-radius: 10px; border-left: 5px solid #2e86de; margin-bottom: 20px;">
                <span style="color: #576574; font-size: 16px; font-weight: bold;">Current Table for Analysis:</span><br>
                <span style="color: #2e86de; font-size: 24px; font-weight: 800; font-family: 'Courier New', monospace;">
                    {schema_name}.{table_name}
                </span>
            </div>
        """, unsafe_allow_html=True)

        # --- NOVO: ACTION BAR (AI Scan, Manual & Refresh) ---
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])

        with col_btn1:
            if st.button("🤖 AI Scan", use_container_width=True, type="secondary"):
                with st.spinner("Consulting AI..."):
                    # Uzimamo mali uzorak za AI
                    raw_df = db.read_table(table_name, schema_name, limit=10)
                    agent = st.session_state.get('agent')
                    st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df, agent, schema_name=schema_name)
                    # Brišemo trenutni plan da bi se osvežio sa novim AI rezultatima
                    if 'current_plan' in st.session_state: del st.session_state['current_plan']
                    st.rerun()

        with col_btn2:
            if st.button("✍️ Manual", use_container_width=True):
                columns = db.get_columns(table_name, schema_name)
                st.session_state['ai_analysis'] = {
                    "plan": [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
                }
                if 'current_plan' in st.session_state: del st.session_state['current_plan']
                st.rerun()

        with col_btn3:
            if st.button("👁️ Refresh Data Preview", use_container_width=True):
                df = db.read_table(table_name, schema_name, limit=100)
                st.session_state['current_df'] = df
                st.success("Data refreshed!")

        st.divider()

        # Provera da li imamo analizu (ako nemamo, ne crtamo editor još)
        if 'ai_analysis' not in st.session_state:
            st.warning("⚠️ No analysis found. Please click **AI Scan** to start.")
            return

        # 1. ODREĐIVANJE "SOURCE OF TRUTH"
        if 'current_plan' in st.session_state and st.session_state.get('last_table_for_plan') == table_name:
            plan_list = st.session_state['current_plan']
        else:
            analysis_data = st.session_state['ai_analysis']
            plan_list = [p.model_dump() if hasattr(p, 'model_dump') else p for p in (analysis_data.plan if hasattr(analysis_data, 'plan') else analysis_data.get('plan', []))]
            st.session_state['current_plan'] = plan_list
            st.session_state['last_table_for_plan'] = table_name

        plan_df = pd.DataFrame(plan_list)

        # 2. SINHRONIZACIJA SA EDITOROM
        if editor_key in st.session_state:
            edits = st.session_state[editor_key].get('edited_rows', {})
            for row_idx, changes in edits.items():
                for col_name, new_val in changes.items():
                    if row_idx < len(plan_df):
                        plan_df.at[row_idx, col_name] = new_val
            st.session_state['current_plan'] = plan_df.to_dict('records')

        # 3. STATUS I REORGANIZACIJA
        valid_strategies = ["keep", "hash", "mask", "mapping", "noise", "date_shift"]
        plan_df['status'] = plan_df['strategy'].apply(
            lambda x: "✅ OK" if str(x).lower().strip() in valid_strategies else "❌ MISSING"
        )
        desired_order = ['status', 'column', 'is_pii', 'strategy', 'reason']
        plan_df = plan_df[[c for c in desired_order if c in plan_df.columns]]

        # 4. DATA EDITOR
        edited_plan_df = st.data_editor(
            plan_df,
            column_config={
                "status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                "column": st.column_config.TextColumn("Database Column", disabled=True),
                "is_pii": st.column_config.CheckboxColumn("PII", disabled=True),
                "strategy": st.column_config.SelectboxColumn("Strategy", options=valid_strategies, required=True),
                "reason": st.column_config.TextColumn("AI Reasoning", disabled=True)
            },
            hide_index=True, use_container_width=True, key=editor_key
        )

        plan_data = edited_plan_df.to_dict('records')
        st.session_state['current_plan'] = plan_data

        # 5. PRIVACY SCORE
        score_points = sum(100 if str(col.get('strategy', '')).lower() in ['mapping', 'hash'] else 50 if str(col.get('strategy', '')).lower() in ['mask', 'noise', 'date_shift'] else 0 for col in plan_data)
        privacy_score = int(score_points / len(plan_data)) if plan_data else 0
        st.write(f"**Current Privacy Score: {privacy_score}%**")
        st.progress(privacy_score / 100)

        st.divider()

        # 6. AKCIJE (Save & Move to Next)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🚀 Run Anonymization Preview", use_container_width=True):
                current_salt = st.session_state.get('salt_input', 'default_salt')
                clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]
                with st.spinner("Processing preview..."):
                    raw_table = db.read_table(table_name, schema_name)
                    anon_df = db.apply_anonymization_rules(raw_table, clean_plan, salt=current_salt)
                    db.save_anonymized_table(anon_df, table_name, target_schema='anon')
                    st.success(f"✅ Preview saved to 'anon.{table_name}'")

        with c2:
            if st.button("💾 Save Plan & Next Table", type="primary", use_container_width=True):
                import time
                missing = [item.get('column') for item in plan_data if str(item.get('strategy', '')).lower().strip() not in valid_strategies]

                if missing:
                    st.error(f"❌ Missing strategies for: {', '.join([f'`{m}`' for m in missing])}")
                else:
                    clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]
                    db.save_ai_plan(schema_name, table_name, clean_plan)
                    st.success(f"✅ Plan for `{table_name}` saved!")

                    all_tables = st.session_state.get('all_tables_list', [])
                    if all_tables:
                        try:
                            current_idx = all_tables.index(table_name)
                            if current_idx + 1 < len(all_tables):
                                next_table = all_tables[current_idx + 1]
                                st.session_state['selected_table_info'] = (next_table, schema_name)
                                for key in ['current_df', 'ai_analysis', 'current_plan', 'last_table_for_plan']:
                                    if key in st.session_state: del st.session_state[key]
                                st.info(f"➡️ Automatski prelazak na: `{next_table}`")
                                time.sleep(1.2)
                                st.rerun()
                            else:
                                st.balloons()
                                st.success("🎯 Sve tabele su obrađene!")
                        except ValueError:
                            st.error("Problem sa identifikacijom redosleda tabela.")


def render_comparison_tab(db):
    st.subheader("🔍 Side-by-Side Comparison")

    if 'current_plan' in st.session_state and 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_plan = st.session_state['current_plan']
        current_salt = st.session_state.get('salt_input', 'default_salt')
        current_locale = st.session_state.get('selected_locale', 'de') # NOVO

        # Uzimamo uzorak za poređenje
        raw_sample = db.read_table(
            table_name,
            schema_name,
            limit=st.session_state.get('last_limit_val', 10),
            where_filter=st.session_state.get('last_where_filter', None)
        )

        if not raw_sample.empty:
            # Primenjujemo anonimizaciju na uzorak sa locale podrškom
            anon_sample = db.apply_anonymization_rules(
                raw_sample,
                current_plan,
                salt=current_salt
            )
            notes = [] # Opet, prazna lista da ostatak UI-ja ne pukne


            if notes:
                for n in set(notes): st.info(n)

            c1, c2 = st.columns(2)
            with c1:
                st.write("**📄 Original Data**")
                st.dataframe(raw_sample, use_container_width=True)
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, use_container_width=True)

            # Export Sekcija
            st.divider()
            if st.button("Prepare Full Download (CSV)"):
                with st.spinner("Generating CSV..."):
                    # NOVI KOD
                    full_df = db.read_table(table_name, schema_name)
                    # Primetite da ovde nema '_', jer metoda vraća samo jedan objekat
                    full_anon = db.apply_anonymization_rules(
                        full_df,
                        current_plan,
                        salt=current_salt
                    )
                    csv = full_anon.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Click to Download CSV",
                        data=csv,
                        file_name=f"anon_{table_name}.csv",
                        mime="text/csv",
                    )
        else:
            st.warning("No records to compare.")

def render_tabs(db):
    tab_list = ["📊 Explorer", "🛠️ Plan", "🔍 Comparison", "📜 Audit"]
    tabs = st.tabs(tab_list)

    with tabs[0]:
        render_explorer_tab(db)

    with tabs[1]:
        render_planner_tab(db)

    with tabs[2]:
        render_comparison_tab(db)

    with tabs[3]:
        # Audit log fetch
        log_query = "SELECT * FROM metadata.audit_log ORDER BY execution_time DESC LIMIT 50"
        try:
            log_df = pd.read_sql(log_query, db.engine)
            st.dataframe(log_df, use_container_width=True)
        except:
            st.info("No audit logs found yet.")