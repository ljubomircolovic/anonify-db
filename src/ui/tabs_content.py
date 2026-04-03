# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import time

# --- 1. OVDE STAVI FUNKCIJU (Vrh fajla) ---
def save_and_move_to_next(db, table_name, schema_name, plan_data):
    """Pomocna funkcija za cuvanje plana i navigaciju kroz tabele"""
    # Ciscenje UI kolona pre snimanja
    clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]

    # Snimanje u bazu
    db.save_ai_plan(schema_name, table_name, clean_plan)

    # Logika za prelazak na sledecu tabelu
    all_tables = st.session_state.get('all_tables_list', [])
    if all_tables:
        try:
            current_idx = all_tables.index(table_name)
            if current_idx + 1 < len(all_tables):
                next_table = all_tables[current_idx + 1]
                # Postavljamo novu tabelu kao aktivnu
                st.session_state['selected_table_info'] = (next_table, schema_name)

                # Brisanje state-a da bi nova tabela pocela sveza
                for key in ['current_df', 'ai_analysis', 'current_plan', 'last_table_for_plan']:
                    if key in st.session_state:
                        del st.session_state[key]

                st.success(f"✅ Plan saved. Moving to `{next_table}`...")
                time.sleep(1)
                st.rerun()
            else:
                st.balloons()
                st.success("🎯 All tables processed!")
        except ValueError:
            st.error("Table sequence error.")


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


        # --- LOGIKA ZA ISTORIJU ---
        current_nav = (table_name, schema_name)
        if not st.session_state['navigation_history'] or st.session_state['navigation_history'][st.session_state['history_pointer']] != current_nav:
            st.session_state['navigation_history'] = st.session_state['navigation_history'][:st.session_state['history_pointer'] + 1]
            st.session_state['navigation_history'].append(current_nav)
            st.session_state['history_pointer'] = len(st.session_state['navigation_history']) - 1

        # --- PRIKAZ NAVIGACIJE (⬅️ Back / Next ➡️) ---
        n_col1, n_col2, n_empty = st.columns([1, 1, 8])
        with n_col1:
            if st.button("⬅️ Back", disabled=(st.session_state['history_pointer'] <= 0), use_container_width=True):
                st.session_state['history_pointer'] -= 1
                st.session_state['selected_table_info'] = st.session_state['navigation_history'][st.session_state['history_pointer']]
                st.rerun()
        with n_col2:
            if st.button("Next ➡️", disabled=(st.session_state['history_pointer'] >= len(st.session_state['navigation_history']) - 1), use_container_width=True):
                st.session_state['history_pointer'] += 1
                st.session_state['selected_table_info'] = st.session_state['navigation_history'][st.session_state['history_pointer']]
                st.rerun()
        st.divider()


        # --- NOVO: Dohvatanje stvarnih PK kolona iz baze ---
        if f"pk_{table_name}" not in st.session_state:
            st.session_state[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)

        real_pks = st.session_state[f"pk_{table_name}"]

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
# --- ACTION BAR ---
        col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1, 1, 1, 1.5])

        with col_btn1:
            if st.button("🤖 AI Scan", use_container_width=True, type="secondary"):
                with st.spinner("Consulting AI..."):
                    raw_df = db.read_table(table_name, schema_name, limit=10)
                    agent = st.session_state.get('agent')
                    st.session_state['ai_analysis'] = db.analyze_table_structure(raw_df, agent, schema_name=schema_name)
                    # KLJUČNO: AI Scan resetuje origin jer je ovo novi predlog
                    st.session_state['plan_origin'] = 'new'
                    if 'plan_snapshot' in st.session_state: del st.session_state['plan_snapshot']
                    if 'current_plan' in st.session_state: del st.session_state['current_plan']
                    st.rerun()

        with col_btn2:
            if st.button("📂 Load Saved", use_container_width=True):
                # Dohvatamo plan iz baze
                saved_plan = db.get_saved_plan(schema_name, table_name)
                if saved_plan:
                    # POSTAVLJANJE SNAPSHOTA:
                    st.session_state['ai_analysis'] = saved_plan
                    st.session_state['plan_origin'] = 'saved'
                    st.session_state['plan_snapshot'] = saved_plan # Ovo služi za poređenje u Koraku 3

                    if 'current_plan' in st.session_state: del st.session_state['current_plan']
                    st.success("Plan loaded from database!")
                    st.rerun()
                else:
                    st.warning("No saved plan found for this table.")

        with col_btn3:
            if st.button("✍️ Manual", use_container_width=True):
                columns = db.get_columns(table_name, schema_name)
                st.session_state['ai_analysis'] = {
                    "plan": [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
                }
                st.session_state['plan_origin'] = 'new'
                if 'current_plan' in st.session_state: del st.session_state['current_plan']
                st.rerun()

        with col_btn4:
            if st.button("👁️ Refresh Preview", use_container_width=True):
                df = db.read_table(table_name, schema_name, limit=100)
                st.session_state['current_df'] = df
                st.success("Data refreshed!")

        st.divider()

# --- 1. ACTION BAR (MORA BITI IZNAD PROVERE) ---
        # Ovde idu tvoji st.columns i dugmići (AI Scan, Load Saved, Manual, Refresh)
        # ... (ovaj deo koda već imaš, on puni 'ai_analysis' ili 'current_plan') ...

        st.divider()

        # --- 2. PAMETNA PROVERA IZVORA PODATAKA ---
        # Proveravamo da li za TRENUTNU tabelu imamo bilo šta u state-u
        is_data_ready = (
            ('ai_analysis' in st.session_state) or 
            ('current_plan' in st.session_state and st.session_state.get('last_table_for_plan') == table_name)
        )

        if not is_data_ready:
            st.info("👋 **Welcome to the Planner!**")
            st.warning("No plan detected for this table. Please choose an action from the bar above:")
            st.markdown("""
                * 🤖 **AI Scan**: Suggest strategies using AI.
                * 📂 **Load Saved**: Retrieve last confirmed plan from DB.
                * ✍️ **Manual**: Define strategies yourself.
            """)
            return  # Ovde stajemo ako korisnik još ništa nije kliknuo

        # --- 3. ODREĐIVANJE "SOURCE OF TRUTH" ---
        # Ako smo prošli return, znači da podaci postoje. Sada ih pakujemo za editor.
        if 'current_plan' in st.session_state and st.session_state.get('last_table_for_plan') == table_name:
            plan_list = st.session_state['current_plan']
        else:
            analysis_data = st.session_state.get('ai_analysis')
            
            if isinstance(analysis_data, list):
                # Podaci iz baze (get_saved_plan vraća listu)
                plan_list = analysis_data
            elif hasattr(analysis_data, 'plan'):
                # Pydantic objekat od AI Agenta
                plan_list = [p.model_dump() if hasattr(p, 'model_dump') else p for p in analysis_data.plan]
            elif isinstance(analysis_data, dict):
                # Ako je AI vratio direktan rečnik
                plan_list = analysis_data.get('plan', [])
            else:
                plan_list = []
                
            # Sinhronizujemo session_state za editor
            st.session_state['current_plan'] = plan_list
            st.session_state['last_table_for_plan'] = table_name

        # Finalna priprema za DataFrame
        plan_df = pd.DataFrame(plan_list)

        # 4. SINHRONIZACIJA SA EDITOROM
        if editor_key in st.session_state:
            edits = st.session_state[editor_key].get('edited_rows', {})
            for row_idx, changes in edits.items():
                for col_name, new_val in changes.items():
                    if row_idx < len(plan_df):
                        plan_df.at[row_idx, col_name] = new_val
            st.session_state['current_plan'] = plan_df.to_dict('records')

        # 5. STATUS I REORGANIZACIJA (Definišemo strategije koje editor koristi)
        if f"pks_{table_name}" not in st.session_state:
            st.session_state[f"pks_{table_name}"] = db.get_primary_keys(schema_name, table_name)

        real_pks = st.session_state[f"pks_{table_name}"]

        valid_strategies = ["keep", "hash", "mask", "mapping", "noise", "date_shift"]
        pk_strategies = ["keep", "hash"]

        def check_row_status(row):
            col_name = str(row['column'])
            strategy = str(row['strategy']).lower().strip()

            # Provera na osnovu meta-podataka iz baze (real_pks)
            is_primary = col_name in real_pks

            if is_primary:
                # Ako je PK, dozvoljavamo samo keep ili hash
                if strategy in pk_strategies:
                    return "🔑 PK: OK"
                else:
                    return "❌ PK: MUST BE KEEP/HASH"

            # Za ostale kolone standardna provera
            return "✅ OK" if strategy in valid_strategies else "❌ MISSING"

        # Primenjujemo validaciju na svaku vrstu u DataFrame-u
        plan_df['status'] = plan_df.apply(check_row_status, axis=1)

        # Reorganizacija kolona radi preglednosti
        desired_order = ['status', 'column', 'is_pii', 'strategy', 'reason']
        plan_df = plan_df[[c for c in desired_order if c in plan_df.columns]]

        # 6. DATA EDITOR
        edited_plan_df = st.data_editor(
            plan_df,
            column_config={
                "status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                "column": st.column_config.TextColumn("Database Column", disabled=True),
                "is_pii": st.column_config.CheckboxColumn("PII", disabled=True),
                "strategy": st.column_config.SelectboxColumn("Strategy", options=valid_strategies, required=True),
                "reason": st.column_config.TextColumn("AI Reasoning", disabled=True)
            },
            hide_index=True,
            use_container_width=True,
            key=editor_key
        )

        # --- KLJUČNI FIX: Ovde definišemo plan_data ---
        plan_data = edited_plan_df.to_dict('records')
        st.session_state['current_plan'] = plan_data

        # ==========================================
        # KORAK 3: DINAMIČKI DUGMIĆI & NAVIGACIJA
        # ==========================================
        st.write("") 
        c1, c2 = st.columns([6, 4]) 

        with c2:
            # 1. Osnovne definicije
            real_pks = st.session_state.get(f"pks_{table_name}", [])
            pk_strategies = ["keep", "hash"]
            origin = st.session_state.get('plan_origin', 'new')
            
            # 2. DETEKCIJA PROMENA
            has_changes = False
            if origin == 'saved' and 'plan_snapshot' in st.session_state:
                # Čistimo privremene UI kolone pre poređenja
                clean_current = [{k: v for k, v in r.items() if k != 'status'} for r in plan_data]
                clean_snapshot = [{k: v for k, v in r.items() if k != 'status'} for r in st.session_state['plan_snapshot']]
                
                if clean_current != clean_snapshot:
                    has_changes = True

            # 3. DINAMIČKA LABELA
            if origin == 'saved' and not has_changes:
                button_label = "✅ Confirm & Next Table"
            elif origin == 'saved' and has_changes:
                button_label = "💾 Save Changes & Next Table"
            else:
                button_label = "💾 Save Plan & Next Table"

            # 4. RENDER DUGMIĆA (SA UNIKATNIM KLJUČEVIMA)
            btn_clicked = False
            main_btn_key = f"main_action_btn_{table_name}"

            if has_changes:
                sc1, sc2 = st.columns([3, 1])
                with sc1:
                    btn_clicked = st.button(button_label, type="primary", use_container_width=True, key=main_btn_key)
                with sc2:
                    if st.button("✖️", help="Revert to saved plan", use_container_width=True, key=f"cancel_btn_{table_name}"):
                        st.session_state['ai_analysis'] = st.session_state['plan_snapshot']
                        if 'current_plan' in st.session_state: del st.session_state['current_plan']
                        st.rerun()
            else:
                btn_clicked = st.button(button_label, type="primary", use_container_width=True, key=main_btn_key)

            # 5. LOGIKA NAKON KLIKA
            if btn_clicked:
                # Validacije
                missing = [item.get('column') for item in plan_data if str(item.get('strategy', '')).lower().strip() not in valid_strategies]
                invalid_pk_list = [item.get('column') for item in plan_data if item.get('column') in real_pks and str(item.get('strategy', '')).lower().strip() not in pk_strategies]

                if invalid_pk_list:
                    st.error(f"❌ PK ERROR: `{invalid_pk_list[0]}` must be keep/hash.")
                elif missing:
                    st.error(f"❌ Missing strategies for: {missing}")
                else:
                    # Check for 'keep' on PK for security warning
                    is_any_pk_keep = any(item.get('column') in real_pks and item.get('strategy') == 'keep' for item in plan_data)
                    if is_any_pk_keep:
                        st.session_state['confirm_pk_move'] = True
                        st.rerun()
                    else:
                        # AKO JE SVE OK -> SNIMI I IDI DALJE
                        save_and_move_to_next(db, table_name, schema_name, plan_data)

        # --- DIJALOG ZA POTVRDU (Mora biti van 'with c2' kolone) ---
        if st.session_state.get('confirm_pk_move', False):
            st.divider()
            st.warning(f"⚠️ **Security Warning:** You selected **'keep'** for Primary Key(s) in `{table_name}`. Move to next?")
            conf_c1, conf_c2 = st.columns(2)
            if conf_c1.button("✅ Yes, save and move", type="primary", use_container_width=True, key=f"conf_y_{table_name}"):
                st.session_state['confirm_pk_move'] = False
                save_and_move_to_next(db, table_name, schema_name, plan_data)
            if conf_c2.button("🔙 No, let me change", use_container_width=True, key=f"conf_n_{table_name}"):
                st.session_state['confirm_pk_move'] = False
                st.rerun()

        st.divider()

        # --- DODATNI INFO: PRIVACY SCORE & PREVIEW ---
        inf_col1, inf_col2 = st.columns([6, 4])
        with inf_col1:
            if plan_data:
                score_points = sum(100 if str(col.get('strategy','')).lower() in ['mapping','hash'] else 50 if str(col.get('strategy','')).lower() in ['mask','noise','date_shift'] else 0 for col in plan_data)
                privacy_score = int(score_points / len(plan_data))
                st.write(f"**Privacy Score: {privacy_score}%**")
                st.progress(privacy_score / 100)
        
        with inf_col2:
            if st.button("🚀 Run Anonymization Preview", use_container_width=True, key=f"pre_btn_{table_name}"):
                current_salt = st.session_state.get('salt_input', 'default_salt')
                clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]
                with st.spinner("Processing preview..."):
                    raw_table = db.read_table(table_name, schema_name)
                    anon_df = db.apply_anonymization_rules(raw_table, clean_plan, salt=current_salt)
                    db.save_anonymized_table(anon_df, table_name, target_schema='anon')
                    st.success(f"✅ Preview saved to 'anon.{table_name}'")


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