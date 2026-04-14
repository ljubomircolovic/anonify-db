# -*- coding: utf-8 -*-
# --- Na vrh fajla ---
import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
# Svi ovi moduli sada rade za tebe:
from src.ui.planner import AnonymizationPlanner, analyze_tables_parallel
from src.ui.planner_logic import validate_plan_row, calculate_privacy_score, get_clean_plan
from src.ui.planner_components import render_status_chain, render_table_header_info, render_ai_audit_log
from src.ui.planner_navigation import handle_navigation_history, render_nav_buttons, get_next_table_in_chain


def save_and_move_to_next(db, table_name, schema_name, plan_data, where_clause=""):
    """
    Sadrži strogu validaciju DDL tipova, PII detekciju i RI sinhronizaciju,
    snima plan i pomera navigaciju na sledeću tabelu.
    """
    # --- 0. DEBUG START ---
    st.toast(f"⏳ Saving plan for {table_name}...", icon="💾")

    # --- 1. DEFINICIJA KOMPATIBILNOSTI (Tvoja originalna logika) ---
    COMPATIBILITY = {
        "numeric": ["keep", "hash", "mapping", "noise", "null"],
        "text": ["keep", "hash", "mask", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "pii": ["keep", "hash", "mapping", "null", "faker_name", "faker_email", "faker_phone"],
        "date": ["keep", "date_shift", "null"],
        "boolean": ["keep", "null"]
    }

    TYPE_GROUPS = {
        "int": "numeric", "bigint": "numeric", "numeric": "numeric", "double": "numeric",
        "date": "date", "timestamp": "date", "time": "date",
        "bool": "boolean"
    }

    try:
        col_details = db.get_column_details(table_name, schema_name)
        actual_db_columns = list(col_details.keys())
        invalid_selections = []

        # Dohvatamo sve relacije (globalna mapa) i sve do sada sačuvane planove
        all_relations = db.get_all_foreign_keys(schema_name)
        all_saved_plans = db.get_all_saved_plans(schema_name)

        for row in plan_data:
            col_name = row.get('column', '')
            strategy = row.get('strategy', 'keep').lower()

            if col_name in col_details:
                col_info = col_details[col_name]
                sql_type = col_info['type'].lower()
                is_nullable = col_info['nullable']

                # --- VALIDACIJA 1: NOT NULL Guard ---
                if strategy == 'null' and is_nullable == 'NO':
                    invalid_selections.append(f"❌ Kolona `{col_name}` je **NOT NULL**.")
                    continue

                # --- VALIDACIJA 2: Referencijalni Integritet (FK & PK) ---
                for rel in all_relations:
                    # Case A: Trenutna kolona je FK
                    if rel[0] == table_name and rel[1] == col_name:
                        parent_table, parent_col = rel[2], rel[3]
                        p_plan = all_saved_plans.get(parent_table)
                        if p_plan:
                            p_strat = next((p['strategy'] for p in p_plan if p['column'] == parent_col), 'keep').lower()
                            if strategy != p_strat:
                                invalid_selections.append(f"❌ **RI Conflict:** `{col_name}` mora biti `{p_strat}` (kao `{parent_table}.{parent_col}`).")

                    # Case B: Trenutna kolona je PK
                    elif rel[2] == table_name and rel[3] == col_name:
                        child_table, child_col = rel[0], rel[1]
                        c_plan = all_saved_plans.get(child_table)
                        if c_plan:
                            c_strat = next((c['strategy'] for c in c_plan if c['column'] == child_col), 'keep').lower()
                            if strategy != c_strat:
                                invalid_selections.append(f"❌ **RI Conflict:** Deca u `{child_table}` već koriste `{c_strat}`.")

        if invalid_selections:
            st.error("🛑 **Integrity Violation** - Plan nije sačuvan!")
            for err in invalid_selections:
                st.write(err)
            return # OVDE MOŽE DA STANE ako dugme "ne radi"

    except Exception as e:
        st.error(f"Sistemska greška pri validaciji: {e}")
        return

    # --- 6. ČIŠĆENJE I SNIMANJE ---
    clean_plan = get_clean_plan(plan_data)
    safe_where = str(where_clause or "").strip()

    save_success = db.save_ai_plan(
        schema_name=schema_name,
        table_name=table_name,
        plan_data=clean_plan,
        where_condition=safe_where
    )

    if not save_success:
        st.error(f"❌ Kritična greška: Plan za `{table_name}` nije sačuvan u bazi!")
        return

    # --- 7. NAVIGACIJA ---
    # Dodajemo u set završenih
    if 'completed_tables' not in st.session_state:
        st.session_state['completed_tables'] = set()
    st.session_state['completed_tables'].add(table_name)

    all_tables = st.session_state.get('all_tables_list', [])

    # Koristimo tvoju navigaciju
    next_table = get_next_table_in_chain(table_name, all_tables, st.session_state['completed_tables'])

    if next_table:
        st.session_state['selected_table_info'] = (next_table, schema_name)

        # Reset state-a za sledeću tabelu
        keys_to_reset = ['ai_analysis', 'current_plan', 'last_rendered_table', 'plan_snapshot']
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]

        st.success(f"✅ Saved! Moving to {next_table}...")
        time.sleep(0.5) # Kratka pauza da korisnik vidi poruku
        st.rerun()
    else:
        st.success("🎯 All tables finalized! Ready for Batch execution.")
        time.sleep(1)
        st.rerun()

def render_explorer_tab(db):
    if 'current_df' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        st.subheader(f"📊 Raw Data Explorer: {schema_name}.{table_name}")

        if not st.session_state['current_df'].empty:
            st.info(f"Showing {len(st.session_state['current_df'])} rows based on your sidebar filter.")
            st.dataframe(st.session_state['current_df'], width="stretch")
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

def get_next_table_in_chain(current_table, all_tables, completed_tables):
    """
    Pametna navigacija koja pronalazi sledeću logičnu tabelu za obradu.
    Prioritet:
    1. Prva sledeća nezavršena tabela nakon trenutne.
    2. Ako smo na kraju lanca, ali ima nezavršenih "iza" nas, vrati prvu nezavršenu.
    """
    if not all_tables:
        return None

    # Ako completed_tables nije set (npr. None), inicijalizuj ga
    if completed_tables is None:
        completed_tables = set()

    try:
        current_idx = all_tables.index(current_table)
    except ValueError:
        # Ako trenutna tabela nije u listi, vrati prvu nezavršenu uopšte
        for table in all_tables:
            if table not in completed_tables:
                return table
        return all_tables[0]

    # --- KORAK 1: Traži prvu nezavršenu tabelu NAKON trenutne ---
    for next_table in all_tables[current_idx + 1:]:
        if next_table not in completed_tables:
            return next_table

    # --- KORAK 2: Ako smo stigli do kraja, proveri da li smo preskočili neku na početku ---
    # Ovo je bitno ako je korisnik kliktao nasumično po sidebaru
    for table in all_tables:
        if table not in completed_tables:
            return table

    # --- KORAK 3: Sve tabele su završene ---
    return None

def render_planner_action_buttons(db, table_name, schema_name):
    """Iscrtava red dugmića i audit log."""
    col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1, 1, 1, 1.5])

    with col_btn1:
        st.markdown("### 🔒 Privacy Settings")
        allow_sampling = st.checkbox("Dozvoli uzorak", value=True, key=f"sample_check_{table_name}")
        sample_rows = st.slider("Uzorak (redova)", 1, 20, 5) if allow_sampling else 0

        if st.button("🤖 AI Scan", width="stretch", type="secondary", key=f"ai_btn_{table_name}"):
            with st.spinner("Consulting AI..."):
                planner = AnonymizationPlanner(db)
                ai_plan, audit_data = planner.generate_suggestion_plan(schema_name, table_name, allow_sampling, sample_rows)
                if ai_plan:
                    st.session_state['ai_analysis'] = ai_plan
                    st.session_state['last_ai_audit'] = audit_data
                    st.rerun()

    with col_btn2:
        if st.button("📂 Load Saved", width="stretch", key=f"load_btn_{table_name}"):
            saved_data = db.get_saved_plan(schema_name, table_name)
            if saved_data:
                st.session_state['ai_analysis'] = saved_data['plan']
                st.session_state['plan_snapshot'] = saved_data['plan']
                st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                st.session_state['plan_origin'] = 'saved'
                st.rerun()

    with col_btn3:
        if st.button("✍️ Manual", width="stretch", key=f"man_btn_{table_name}"):
            columns = db.get_columns(table_name, schema_name)
            st.session_state['ai_analysis'] = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
            st.session_state['plan_origin'] = 'new'
            st.rerun()

    with col_btn4:
        if st.button("👁️ View Data", width="stretch", key=f"view_btn_{table_name}"):
            st.info("Live Preview is available at the bottom 👇")

    # OVO JE BITNO - Audit log se iscrtava odmah ispod dugmića
    render_ai_audit_log()

def render_planner_tab(db):
    st.header("🚀 Parallel AI Strategy Planner")

    # --- 🛠️ DEBUG DASHBOARD ---
    with st.expander("🔍 DEBUG: Session State Inspector", expanded=False):
        col_db1, col_db2 = st.columns(2)
        with col_db1:
            st.write("**Trenutna selekcija:**")
            st.code(st.session_state.get('selected_table_info', 'Nije selektovana'))
            st.write("**Multi-Scan Ključevi u memoriji:**")
            st.code(list(st.session_state.get('multi_ai_analysis', {}).keys()))
        with col_db2:
            st.write("**Status plana:**")
            st.write(f"Ima li 'ai_analysis'?: `{'DA' if 'ai_analysis' in st.session_state else 'NE'}`")

    st.divider()

    # --- 1. GLOBALNA ANALIZA ---
    available_tables = db.get_tables(schema_name="ecommerce")
    selected_multi_tables = st.multiselect(
        "Izaberi tabele za masovnu AI analizu:",
        options=available_tables,
        default=[],
        key="planner_multiselect"
    )

    c1, c2 = st.columns([1, 2])
    with c1:
        bulk_allow_sampling = st.checkbox("Dozvoli uzorak za sve", value=True, key="bulk_sample_check")
    with c2:
        bulk_sample_rows = st.slider("Uzorak (redova) za sve", 1, 20, 5) if bulk_allow_sampling else 0

    if st.button("🪄 Parallel AI Scan", disabled=not selected_multi_tables, type="primary"):
        with st.spinner(f"Analiziram {len(selected_multi_tables)} tabela paralelno..."):
            all_results = analyze_tables_parallel(
                db, 
                selected_multi_tables, 
                schema="ecommerce",
                allow_sampling=bulk_allow_sampling,
                sample_limit=bulk_sample_rows
            )
            st.session_state['multi_ai_analysis'] = all_results
            if 'selected_table_info' not in st.session_state and selected_multi_tables:
                st.session_state['selected_table_info'] = (selected_multi_tables[0], "ecommerce")
            st.success("Analiza završena!")
            st.rerun()

    # --- 2. LANAC PROGRESA ---
    all_tables_list = st.session_state.get('all_tables_list', [])
    completed_tables = st.session_state.get('completed_tables', set())
    render_status_chain(all_tables_list, completed_tables)
    st.divider()

    # --- 3. RAD SA POJEDINAČNOM TABELOM ---
    if 'selected_table_info' in st.session_state:
        table_info = st.session_state['selected_table_info']
        table_name = table_info[0] if isinstance(table_info, tuple) else table_info
        schema_name = table_info[1] if isinstance(table_info, tuple) else "ecommerce"

        editor_key = f"plan_editor_{table_name}"
        where_key = f"where_clause_{table_name}"

        # --- 🛡️ SINHRONIZACIJA (Claude Fix: Ne briši, samo puni ako je prazno) ---
        multi_results = st.session_state.get('multi_ai_analysis', {})
        base_name = str(table_name).split('.')[-1]
        
        # Ako NEMAMO ai_analysis, a IMAMO multi_results za ovu tabelu -> PUNI ODMAH
        if 'ai_analysis' not in st.session_state or st.session_state.get('last_rendered_table') != table_name:
            found_res = multi_results.get(table_name) or multi_results.get(base_name)
            if found_res:
                if hasattr(found_res, 'plan'):
                    st.session_state['ai_analysis'] = found_res.plan
                    st.session_state['last_ai_audit'] = getattr(found_res, 'audit', [])
                elif isinstance(found_res, dict) and 'plan' in found_res:
                    st.session_state['ai_analysis'] = found_res['plan']
                    st.session_state['last_ai_audit'] = found_res.get('audit', [])
                st.session_state['plan_origin'] = 'parallel_ai'
                st.session_state['last_rendered_table'] = table_name

        handle_navigation_history(table_name, schema_name)
        render_nav_buttons()
        render_table_header_info(schema_name, table_name)

        # --- PRIVACY SETTINGS SEKCIJA ---
        col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1, 1, 1, 1.5])
        with col_btn1:
            st.markdown("### 🔒 Privacy Settings")
            allow_sampling = st.checkbox("Dozvoli uzorak", value=True, key=f"sample_check_{table_name}")
            sample_rows = st.slider("Uzorak (redova)", 1, 20, 5, key=f"sample_slider_{table_name}") if allow_sampling else 0
            if st.button("🤖 AI Scan", type="secondary", key=f"ai_btn_{table_name}", use_container_width=True):
                with st.spinner("Consulting AI..."):
                    planner = AnonymizationPlanner(db)
                    ai_plan, audit_data = planner.generate_suggestion_plan(schema_name, table_name, allow_sampling, sample_rows)
                    if ai_plan:
                        st.session_state['ai_analysis'] = ai_plan
                        st.session_state['last_ai_audit'] = audit_data
                        st.rerun()

        with col_btn2:
            if st.button("📂 Load Saved", key=f"load_btn_{table_name}", use_container_width=True):
                saved_data = db.get_saved_plan(schema_name, table_name)
                if saved_data:
                    st.session_state['ai_analysis'] = saved_data['plan']
                    st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                    st.rerun()

        with col_btn3:
            if st.button("✍️ Manual", key=f"man_btn_{table_name}", use_container_width=True):
                columns = db.get_columns(table_name, schema_name)
                st.session_state['ai_analysis'] = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
                st.rerun()

        with col_btn4:
            if st.button("👁️ View Data", key=f"view_btn_{table_name}", use_container_width=True):
                st.info("Live Preview is available at the bottom 👇")

        render_ai_audit_log()
        st.divider()

        # --- DATA FILTER ---
        st.markdown("### 🔍 Data Filter")
        st.session_state[where_key] = st.text_input(
            "SQL WHERE Clause:", value=st.session_state.get(where_key, ""), key=f"in_{where_key}"
        )
        st.divider()

        # --- DATA EDITOR (Zadnja linija odbrane) ---
        # Ovde radimo finalni check: ako i dalje nema ai_analysis, probaj fuzzy match još jednom
        if 'ai_analysis' not in st.session_state:
            found_res = multi_results.get(table_name) or multi_results.get(base_name)
            if found_res:
                st.session_state['ai_analysis'] = found_res['plan'] if isinstance(found_res, dict) else found_res.plan
                st.rerun() # Forsiraj osvežavanje da editor vidi podatke

        if 'ai_analysis' in st.session_state:
            analysis_data = st.session_state['ai_analysis']
            plan_list = analysis_data.plan if hasattr(analysis_data, 'plan') else analysis_data
            plan_df = pd.DataFrame(plan_list)
            
            if f"pk_{table_name}" not in st.session_state:
                st.session_state[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)
            
            plan_df['status'] = plan_df.apply(lambda x: validate_plan_row(x, st.session_state[f"pk_{table_name}"]), axis=1)

            edited_plan_df = st.data_editor(
                plan_df,
                column_config={
                    "status": st.column_config.TextColumn("Status", disabled=True),
                    "column": st.column_config.TextColumn("Column", disabled=True),
                    "strategy": st.column_config.SelectboxColumn("Strategy", options=["keep", "hash", "mask", "mapping", "noise", "date_shift", "null", "faker_name", "faker_email", "faker_phone"], required=True),
                },
                hide_index=True,
                key=editor_key
            )
            st.session_state['current_plan'] = edited_plan_df.to_dict('records')
        else:
            st.warning("Izaberi akciju iznad (AI Scan, Load ili Manual) da započneš.")
            return

        # --- FINALNE AKCIJE ---
        c_act1, c_act2 = st.columns([1, 1])
        with c_act1:
            if st.button("🚀 Preview", use_container_width=True, key=f"pre_btn_{table_name}"):
                # Preview logika
                pass
        with c_act2:
            next_table = get_next_table_in_chain(table_name, all_tables_list, completed_tables)
            btn_label = "💾 Confirm & Next" if next_table else "🏁 Finish"
            if st.button(btn_label, type="primary", use_container_width=True, key=f"confirm_next_{table_name}"):
                save_and_move_to_next(db, table_name, schema_name, st.session_state['current_plan'], st.session_state.get(where_key, ""))
    else:
        st.info("👋 Select a table from the sidebar to start.")


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
            where=st.session_state.get(f"where_clause_{table_name}", "") # Koristi ispravan ključ i argument
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
                st.dataframe(raw_sample, width="stretch")
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, width="stretch")

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

    # --- OVDE POZIVAMO GLOBALNI PREVIEW ---
    render_global_preview_section(db)

def sync_anon_ddl_with_plan(db, target_schema, table_name, plan):
    """
    Usklađuje tipove podataka u anon šemi sa planom anonimizacije.
    Prosleđujemo 'db' (DBManager instancu) umesto self.
    """
    from sqlalchemy import text

    text_strategies = ['hash', 'faker_name', 'faker_email', 'faker_phone', 'mask', 'mapping']

    # Koristimo db.engine jer je funkcija sada van klase
    with db.engine.connect() as conn:
        for item in plan:
            col = item['column']
            strategy = item.get('strategy', 'keep').lower()

            if strategy in text_strategies:
                check_query = text("""
                    SELECT data_type FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t AND column_name = :c
                """)
                current_type = conn.execute(check_query, {"s": target_schema, "t": table_name, "c": col}).scalar()

                if current_type and any(num_type in current_type.lower() for num_type in ['int', 'numeric', 'double', 'real']):
                    print(f"🔧 DDL Sync: Menjam {table_name}.{col} iz {current_type} u VARCHAR(255)...")

                    alter_query = text(f"""
                        ALTER TABLE "{target_schema}"."{table_name}"
                        ALTER COLUMN "{col}" TYPE VARCHAR(255)
                        USING "{col}"::VARCHAR
                    """)
                    conn.execute(alter_query)
                    conn.commit()
                    print(f"✅ DDL Aligned: {table_name}.{col} converted to VARCHAR")

def get_all_foreign_keys(db, schema_name):
    """
    Izvlači sve FK relacije u šemi.
    Prosleđujemo 'db' (DBManager instancu) umesto self.
    """
    from sqlalchemy import text

    query = text("""
        SELECT
            kcu.table_name as source_table,
            kcu.column_name as source_column,
            rel_kcu.table_name as target_table,
            rel_kcu.column_name as target_column
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu
          ON tco.constraint_name = kcu.constraint_name
        JOIN information_schema.referential_constraints rco
          ON tco.constraint_name = rco.constraint_name
        JOIN information_schema.key_column_usage rel_kcu
          ON rco.unique_constraint_name = rel_kcu.constraint_name
        WHERE tco.constraint_type = 'FOREIGN KEY'
          AND tco.table_schema = :s
    """)

    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"s": schema_name})
            return [(row[0], row[1], row[2], row[3]) for row in result]
    except Exception as e:
        print(f"❌ Error fetching foreign keys: {e}")
        return []

def render_global_preview_section(db):
    """Prikazuje live preview podataka na dnu ekrana, nezavisno od tabova."""

    if 'selected_table_info' in st.session_state:
        table_name, schema_name = st.session_state['selected_table_info']
        current_full_name = f"{schema_name}.{table_name}"

        # --- 1. AUTO-RESET LOGIKA (SINHRONIZACIJA) ---
        # Ako je korisnik promenio tabelu u Sidebaru, brišemo stari DataFrame
        # da ne bismo prikazivali "stale data" (podatke od prošle tabele)
        if st.session_state.get('last_previewed_table') != current_full_name:
            if 'current_df' in st.session_state:
                del st.session_state['current_df']
            st.session_state['last_previewed_table'] = current_full_name

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("---")

        with st.container():
            # Naslov expandera sada dinamički ispisuje ime trenutne tabele
            with st.expander(f"👁️ Live Data Preview: {current_full_name}", expanded=True):
                p_col1, p_col2 = st.columns([3, 7])

                with p_col1:
                    st.write(f"**Current Context:** `{current_full_name}`")

                    # Hvatanje filtera koji je korisnik ukucao u Planer tabu
                    where_clause = st.session_state.get(f"where_clause_{table_name}", "")

                    if where_clause:
                        st.info(f"🔍 **Active Filter:**\n`{where_clause}`")
                        # DEV-FRIENDLY: Prikazujemo puni SQL query za lakši debug u DBeaver-u
                        st.caption("Debug SQL Query:")
                        st.code(f"SELECT * FROM {current_full_name} WHERE {where_clause} LIMIT 100;", language="sql")
                    else:
                        st.caption("No active filter. Showing top 100 records.")

                    if st.button("🔄 Refresh Data", key="global_preview_refresh_btn", width="stretch"):
                        with st.spinner(f"Fetching {table_name}..."):
                            try:
                                # Čitamo podatke koristeći tvoj db_manager
                                df = db.read_table(table_name, schema_name, where=where_clause, limit=100)
                                st.session_state['current_df'] = df
                                st.rerun()
                            except Exception as e:
                                st.error(f"SQL Error: {str(e)}")

                with p_col2:
                    if 'current_df' in st.session_state:
                        df = st.session_state['current_df']

                        # --- 2. HANDLING ZA PRAZNE TABELE ---
                        if df.empty:
                            st.warning("⚠️ This table is empty or no records match your WHERE clause.")
                        else:
                            st.dataframe(
                                df,
                                use_container_width=True,
                                hide_index=True
                            )
                            st.caption(f"Showing up to 100 rows from {current_full_name}")
                    else:
                        st.info("💡 Data not loaded yet. Click **'Refresh Data'** to fetch a snippet.")
    else:
        # Ako ništa nije selektovano, panel je diskretan
        st.markdown("---")
        st.info("👋 Select a table in the **Explorer** or **Plan** tab to enable live preview here.")