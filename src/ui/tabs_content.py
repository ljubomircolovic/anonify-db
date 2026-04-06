# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from src.ui.batch_processor import handle_batch_execution

# --- 1. OVDE STAVI FUNKCIJU (Vrh fajla) ---
def save_and_move_to_next(db, table_name, schema_name, plan_data):
    """Pomocna funkcija sa STROGOM validacijom DDL tipova, PII detekcijom i RI sinhronizacijom"""

    # --- 1. DEFINICIJA KOMPATIBILNOSTI ---
    COMPATIBILITY = {
        "numeric": ["keep", "mapping", "noise", "null"],
        "text": ["keep", "hash", "mask", "mapping", "null"],
        "pii": ["keep", "hash", "mapping", "null"],
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

                # --- 1. NOT NULL GUARD ---
                if strategy == 'null' and is_nullable == 'NO':
                    invalid_selections.append(
                        f"❌ Kolona `{col_name}` je **NOT NULL**. Strategija `null` nije dozvoljena."
                    )
                    continue

                # --- 2. REFERENCIJALNI INTEGRITET (FK & PK Guard) ---
                for rel in all_relations:
                    # Case A: Trenutna kolona je FK (Sluga), proveravamo Roditelja
                    if rel[0] == table_name and rel[1] == col_name:
                        parent_table, parent_col = rel[2], rel[3]
                        p_plan = all_saved_plans.get(parent_table)
                        if p_plan:
                            p_strat = next((p['strategy'] for p in p_plan if p['column'] == parent_col), 'keep').lower()
                            if strategy != p_strat:
                                invalid_selections.append(
                                    f"❌ **RI Conflict (FK):** `{col_name}` referencira `{parent_table}.{parent_col}` (Strategija: `{p_strat}`). "
                                    f"Moraš uskladiti strategiju."
                                )

                    # Case B: Trenutna kolona je PK (Roditelj), proveravamo Decu koja su već sačuvana
                    elif rel[2] == table_name and rel[3] == col_name:
                        child_table, child_col = rel[0], rel[1]
                        c_plan = all_saved_plans.get(child_table)
                        if c_plan:
                            c_strat = next((c['strategy'] for c in c_plan if c['column'] == child_col), 'keep').lower()
                            if strategy != c_strat:
                                invalid_selections.append(
                                    f"❌ **RI Conflict (PK):** Kolona je ključ za `{child_table}.{child_col}` koji je već sačuvan kao `{c_strat}`. "
                                    f"Promeni ovde u `{c_strat}` ili izmeni plan za tabelu `{child_table}`."
                                )

                # --- 3. ODREĐIVANJE KATEGORIJE ---
                category = None
                for base_type, group in TYPE_GROUPS.items():
                    if base_type in sql_type:
                        category = group
                        break

                category = category or "text"
                if category == "text":
                    pii_keywords = ['name', 'email', 'phone', 'surname', 'mail', 'address']
                    if any(key in col_name.lower() for key in pii_keywords):
                        category = "pii"

                # --- 4. PROVERA KOMPATIBILNOSTI + DDL BYPASS ---
                allowed = COMPATIBILITY.get(category, [])

                # Proveravamo da li je kolona deo bilo kakve relacije (FK ili PK)
                is_in_relation = any(
                    (r[0] == table_name and r[1] == col_name) or (r[2] == table_name and r[3] == col_name)
                    for r in all_relations
                )

                # KLJUČNA PROMENA:
                # Ako je kolona deo relacije, dozvoljavamo bilo koju strategiju
                # jer pretpostavljamo da će naš DDL Sync uskladiti tipove u _anon bazi.
                if is_in_relation:
                    # Dozvoljavamo sve strategije koje su RI sinhronizovane
                    pass
                elif strategy not in allowed:
                    invalid_selections.append(
                        f"❌ Kolona `{col_name}` ({sql_type}) ne podržava `{strategy}`. "
                        f"Dozvoljeno: {', '.join(allowed)}"
                    )

        if invalid_selections:
            st.error("🛑 **Integrity Violation**")
            for err in invalid_selections:
                st.write(err)
            return

        # --- 5. FINALNI SANITY CHECK ---
        plan_columns = [row.get('column') for row in plan_data if 'column' in row]
        if plan_columns and plan_columns[0] not in actual_db_columns:
            st.error(f"🛑 **Critical Error:** Data mismatch za tabelu `{table_name}`.")
            return

    except Exception as e:
        st.error(f"Sistemska greška pri validaciji: {e}")
        return

    # --- 6. ČIŠĆENJE I SNIMANJE (Samo ako je sve Type-Safe) ---
    clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]
    db.save_ai_plan(schema_name, table_name, clean_plan)

    # --- 7. LOGIKA ZA PRELAZAK NA SLEDEĆU TABELU ---
    all_tables = st.session_state.get('all_tables_list', [])

    if all_tables:
        try:
            current_idx = all_tables.index(table_name)

            # 1. MARKIRANJE TABELE KAO ZAVRŠENE (Za tvoju listu sa ✅)
            if 'completed_tables' not in st.session_state:
                st.session_state['completed_tables'] = set()
            st.session_state['completed_tables'].add(table_name)

            if current_idx + 1 < len(all_tables):
                # --- IMA JOŠ TABELA ---
                next_table = all_tables[current_idx + 1]
                st.session_state['selected_table_info'] = (next_table, schema_name)

                # Resetujemo SAMO podatke o trenutnom editoru
                keys_to_clear = [
                    'current_df', 'ai_analysis', 'current_plan',
                    'last_table_for_plan', 'last_rendered_table',
                    'plan_snapshot', 'plan_origin'
                ]
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]

                # Brišemo state editora samo za tabelu koju smo upravo završili
                for key in list(st.session_state.keys()):
                    if key.startswith(f"plan_editor_{table_name}"):
                        del st.session_state[key]

                st.success(f"✅ Plan za `{table_name}` je sačuvan. Prebacujem na `{next_table}`...")
                time.sleep(1)
                st.rerun()
            else:
                # --- ZADNJA TABELA DOSTIGNUTA ---
                # Ovde NE RADIMO st.rerun() odmah, nego osiguravamo da Batch ostane vidljiv
                st.session_state['all_plans_saved'] = True # Flag koji ti drži Batch sekciju upaljenom

                st.success(f"🎯 Sve tabele su procesuirane! Batch Execution je spreman.")

                # Opciono: Forsiramo jedan rerun da bi se osvežila lista sa kvačicama
                # ali bez brisanja selected_table_info
                time.sleep(1)
                st.rerun()

        except ValueError:
            st.error("Greška u sekvenci tabela.")

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

def render_planner_tab(db):

    # --- 1. DOHVATANJE GLOBALNIH PODATAKA (Uvek dostupni) ---
    all_tables = st.session_state.get('all_tables_list', [])
    completed = st.session_state.get('completed_tables', set())

    # --- 2. PRIKAZ LANCA PROGRESA (ZAKUCANO NA VRH) ---
    if all_tables:
        steps = []
        for t in all_tables:
            # Ako je tabela u setu completed, dobija ✅, inače ⏳
            icon = "✅" if t in completed else "⏳"
            steps.append(f"{icon} {t}")

        # Prikazujemo info bar sa lancem koji je uvek tu
        st.info(f"⛓️ **Execution Order:** {' ➔ '.join(steps)}")

    st.divider()

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
            if st.button("⬅️ Back", disabled=(st.session_state['history_pointer'] <= 0), width="stretch"):
                st.session_state['history_pointer'] -= 1
                st.session_state['selected_table_info'] = st.session_state['navigation_history'][st.session_state['history_pointer']]
                st.rerun()
        with n_col2:
            if st.button("Next ➡️", disabled=(st.session_state['history_pointer'] >= len(st.session_state['navigation_history']) - 1), width="stretch"):
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
            if st.button("🤖 AI Scan", width="stretch", type="secondary"):
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
            if st.button("📂 Load Saved", width="stretch"):
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
            if st.button("✍️ Manual", width="stretch"):
                columns = db.get_columns(table_name, schema_name)
                st.session_state['ai_analysis'] = {
                    "plan": [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
                }
                st.session_state['plan_origin'] = 'new'
                if 'current_plan' in st.session_state: del st.session_state['current_plan']
                st.rerun()

        with col_btn4:
            if st.button("👁️ Refresh Preview", width="stretch"):
                df = db.read_table(table_name, schema_name, limit=100)
                st.session_state['current_df'] = df
                st.success("Data refreshed!")

        st.divider()

# --- 1. ACTION BAR (MORA BITI IZNAD PROVERE) ---
        # Ovde idu tvoji st.columns i dugmići (AI Scan, Load Saved, Manual, Refresh)
        # ... (ovaj deo koda već imaš, on puni 'ai_analysis' ili 'current_plan') ...


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

        valid_strategies = ["keep", "hash", "mask", "mapping", "noise", "date_shift", "null"]
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
            width="stretch",
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
            main_btn_key = f"main_action_btn_{table_name}_{origin}" # Dodat origin u key radi stabilnosti

            if has_changes:
                sc1, sc2 = st.columns([3, 1])
                with sc1:
                    btn_clicked = st.button(button_label, type="primary", width="stretch", key=main_btn_key)
                with sc2:
                    if st.button("✖️", help="Revert to saved plan", width="stretch", key=f"cancel_btn_{table_name}"):
                        st.session_state['ai_analysis'] = st.session_state['plan_snapshot']
                        if 'current_plan' in st.session_state: del st.session_state['current_plan']
                        st.rerun()
            else:
                # Ako nema promena, dugme je i dalje tu ali služi kao potvrda
                btn_clicked = st.button(button_label, type="primary", width="stretch", key=main_btn_key)

            # 5. LOGIKA NAKON KLIKA
            if btn_clicked:
                # --- PRVO: Validacija strategija ---
                missing = [item.get('column') for item in plan_data if str(item.get('strategy', '')).lower().strip() not in valid_strategies]
                invalid_pk_list = [item.get('column') for item in plan_data if item.get('column') in real_pks and str(item.get('strategy', '')).lower().strip() not in pk_strategies]

                if invalid_pk_list:
                    st.error(f"❌ PK ERROR: `{invalid_pk_list[0]}` must be keep/hash.")
                elif missing:
                    st.error(f"❌ Missing strategies for: {missing}")
                else:
                    # --- DRUGO: Markiranje tabele kao završene (ZA LANAC SA ✅) ---
                    if 'completed_tables' not in st.session_state:
                        st.session_state['completed_tables'] = set()
                    st.session_state['completed_tables'].add(table_name)

                    # --- TREĆE: Check for 'keep' on PK for security warning ---
                    is_any_pk_keep = any(item.get('column') in real_pks and item.get('strategy') == 'keep' for item in plan_data)

                    if is_any_pk_keep:
                        # Ako ide na dijalog, dijalog će pozvati save_and_move_to_next
                        st.session_state['confirm_pk_move'] = True
                        st.rerun()
                    else:
                        # DIREKTNO SNIMANJE I PRELAZAK
                        save_and_move_to_next(db, table_name, schema_name, plan_data)


        # --- DIJALOG ZA POTVRDU (Mora biti van 'with c2' kolone) ---
        if st.session_state.get('confirm_pk_move', False):
            st.divider()
            st.warning(f"⚠️ **Security Warning:** You selected **'keep'** for Primary Key(s) in `{table_name}`. Move to next?")
            conf_c1, conf_c2 = st.columns(2)

            # U delu koda gde je DIJALOG ZA POTVRDU
            if conf_c1.button("✅ Yes, save and move", type="primary", width="stretch", key=f"conf_y_{table_name}"):
                if 'completed_tables' not in st.session_state:
                    st.session_state['completed_tables'] = set()
                st.session_state['completed_tables'].add(table_name) # OBAVEZNO DODAJ OVO
                st.session_state['confirm_pk_move'] = False
                save_and_move_to_next(db, table_name, schema_name, plan_data)

            if conf_c2.button("🔙 No, let me change", width="stretch", key=f"conf_n_{table_name}"):
                st.session_state['confirm_pk_move'] = False
                st.rerun()

        st.divider()

        # --- DODATNI INFO: PRIVACY SCORE & PREVIEW ---
        inf_col1, inf_col2 = st.columns([6, 4])
        with inf_col1:
            if plan_data:
                # Definišemo težine za svaku strategiju
                # 100 poena: null (najsigurnije), mapping i hash (jaka anonimizacija)
                # 50 poena: mask, noise, date_shift (parcijalna zaštita)
                # 0 poena: keep (nema zaštite)

                score_points = sum(
                    100 if str(col.get('strategy','')).lower() in ['mapping', 'hash', 'null']
                    else 50 if str(col.get('strategy','')).lower() in ['mask', 'noise', 'date_shift']
                    else 0
                    for col in plan_data
                )

                privacy_score = int(score_points / len(plan_data))

                # Ograničavamo score na max 100% (u slučaju da len(plan_data) napravi anomaliju)
                privacy_score = min(privacy_score, 100)

                st.write(f"**Privacy Score: {privacy_score}%**")
                st.progress(privacy_score / 100)

        with inf_col2:
            if st.button("🚀 Run Anonymization Preview", width="stretch", key=f"pre_btn_{table_name}"):
                current_salt = st.session_state.get('salt_input', 'default_salt')
                clean_plan = [{k: v for k, v in row.items() if k != 'status'} for row in plan_data]
                with st.spinner("Processing preview..."):
                    raw_table = db.read_table(table_name, schema_name)
                    anon_df = db.apply_anonymization_rules(raw_table, clean_plan, salt=current_salt)
                    db.save_anonymized_table(anon_df, table_name, target_schema='anon')
                    st.success(f"✅ Preview saved to 'anon.{table_name}'")
    else:
        st.info("👋 **Welcome!** Please select a table from the list or sidebar to start mapping.")


    # Na kraju render_planner_tab u tabs_content.py
    st.markdown("---")
# Izvlačimo šemu iz session_state-a jer nam treba i van 'if selected_table_info'
    current_selected_schema = st.session_state.get('selected_schema')

    if not current_selected_schema and 'selected_table_info' in st.session_state:
        # Fallback ako selected_schema nije setovan, uzmi iz trenutno selektovane tabele
        _, current_selected_schema = st.session_state['selected_table_info']

    if st.session_state.get('all_tables_list'):
        st.subheader("🔥 Batch Execution")
        
        # Definišemo target_schema sigurno
        target_schema = st.session_state.get('target_schema', f"{current_selected_schema}_anon")
        
        # Dohvatamo listu selektovanih tabela iz multiselect-a u sidebaru
        batch_tables = st.session_state.get('batch_table_selector', [])
        
        # POZIVAMO METODU - SADA JE 100% ZAKUCANA NA DNU
        handle_batch_execution(
            db=db,
            ordered_tables=st.session_state['all_tables_list'],
            selected_schema=current_selected_schema,
            target_schema=target_schema,
            selected_tables=batch_tables,
            instance_id="main_batch_footer"
        )



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
            st.dataframe(log_df, width="stretch")
        except:
            st.info("No audit logs found yet.")

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