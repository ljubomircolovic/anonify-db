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
    if 'ai_analysis' in st.session_state:
        st.subheader("🛠️ Review & Finalize Plan")

        # Uzimamo osnovne informacije o tabeli
        table_name, schema_name = st.session_state['selected_table_info']

        # --- FIX ZA BUG 2: Osiguranje da podaci odgovaraju selektovanoj tabeli ---
        if 'current_df' in st.session_state:
            # Proveravamo kolone u učitanom DataFrame-u naspram onoga što baza kaže za trenutnu tabelu
            actual_db_cols = db.get_columns(table_name, schema_name)
            current_df_cols = st.session_state['current_df'].columns.tolist()

            # Ako se kolone ne podudaraju, stopiramo render dok korisnik ne klikne Load Table Data
            if not all(col in actual_db_cols for col in current_df_cols[:3]):
                st.warning(f"🔄 Data mismatch detected. Please click 'Load Table Data' for `{table_name}` to refresh.")
                st.stop()

        # 1. Unifikacija podataka (Pydantic vs Dict)
        analysis_data = st.session_state['ai_analysis']
        if hasattr(analysis_data, 'plan'):
            plan_list = [p.model_dump() for p in analysis_data.plan]
        elif isinstance(analysis_data, dict) and 'plan' in analysis_data:
            plan_list = analysis_data['plan']
        else:
            plan_list = []

        # --- FIX ZA BUG 1: Redosled kolona (Column First) ---
        plan_df = pd.DataFrame(plan_list)
        if not plan_df.empty:
            # Definišemo željeni redosled: 'column' ide na početak
            desired_order = ['column', 'is_pii', 'strategy', 'reason']
            # Ređamo samo one koje postoje u DF-u
            existing_cols = [c for c in desired_order if c in plan_df.columns]
            plan_df = plan_df[existing_cols]

        # --- NOVO: Detekcija Foreign Key kolona (Za upozorenje) ---
        fk_relations = db.get_foreign_key_relations_postgres(schema_name)
        current_table_fks = fk_relations[fk_relations['table_name'] == table_name]['column_name'].tolist()
        fks_in_plan = [col for col in plan_list if col['column'] in current_table_fks]

        if fks_in_plan:
            with st.warning("⚠️ **Foreign Key Columns Detected**"):
                cols_str = ", ".join([f"`{c['column']}`" for c in fks_in_plan])
                st.write(f"Ove kolone povezuju tabele: {cols_str}")
                st.info("💡 Za ove kolone koristite **HASH** ili **KEEP**.")
                if st.button("❓ Saznaj više o FK anonimizaciji"):
                    render_fk_explanation()

        # 2. Data Editor (sada sa ispravnim redosledom kolona)
        editor_key = f"plan_editor_{table_name}"
        edited_plan_df = st.data_editor(
            plan_df,
            column_config={
                "column": st.column_config.TextColumn("Database Column", disabled=True),
                "is_pii": st.column_config.CheckboxColumn("PII", disabled=True),
                "strategy": st.column_config.SelectboxColumn(
                    "Strategy",
                    options=["keep", "hash", "mask", "mapping", "noise", "date_shift"],
                    required=True
                ),
                "reason": st.column_config.TextColumn("AI Reasoning", disabled=True)
            },
            hide_index=True,
            use_container_width=True,
            key=editor_key
        )

        plan_data = edited_plan_df.to_dict('records')
        st.session_state['current_plan'] = plan_data

        # 3. Privacy Score
        total_cols = len(plan_data)
        score_points = sum(100 if str(col['strategy']).lower() in ['mapping', 'hash'] else 50 if str(col['strategy']).lower() in ['mask', 'noise', 'date_shift'] else 0 for col in plan_data)
        privacy_score = int(score_points / total_cols) if total_cols > 0 else 0

        st.write(f"**Current Privacy Score: {privacy_score}%**")
        st.progress(privacy_score / 100)

        if privacy_score < 40: st.error("🔴 Low Protection")
        elif privacy_score < 75: st.warning("🟡 Balanced Protection")
        else: st.success("🟢 High Protection")

        st.divider()

        # 4. Akcije
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 Run Anonymization Preview", use_container_width=True):
                with st.spinner("Processing data..."):
                    current_salt = st.session_state.get('salt_input', 'default_salt')
                    full_df = db.read_table(table_name, schema_name)
                    anon_df = db.apply_anonymization_rules(full_df, plan_data, salt=current_salt)
                    db.save_anonymized_table(anon_df, table_name, target_schema='anon')
                    db.log_action(st.session_state.get('user_name', 'Admin'), schema_name, table_name, privacy_score, current_salt)
                    st.success(f"✅ Processed and saved to 'anon.{table_name}'")

        with col2:
            if st.button("💾 Save Plan in DB", type="primary", use_container_width=True):
                db.save_ai_plan(schema_name, table_name, plan_data)
                st.success("Plan saved successfully!")
                st.rerun()

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