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

# Pomocna funkcija za dinamičko kvotovanje na osnovu DDL tipa
def _get_quoted_value(column_name, col_details, value):
    if column_name in col_details:
        col_type = col_details[column_name]['type'].lower()
        if any(t in col_type for t in ['char', 'text', 'uuid', 'date', 'time', 'varchar']):
            return f"'{value}'"
    return str(value)



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

    # Ensure 'selected_tables' is updated to reflect the new completed table
    if 'selected_tables' in st.session_state and table_name not in st.session_state['selected_tables']:
        st.session_state['selected_tables'].append(table_name)

    all_tables = st.session_state.get('all_tables_list', [])

    # Koristimo tvoju navigaciju
    next_table = get_next_table_in_chain(table_name, all_tables, st.session_state['completed_tables'])

    if next_table:
        # Check if the next table has a saved plan to determine if 'plan_active' should remain True
        next_table_plan = db.get_saved_plan(schema_name, next_table)
        if next_table_plan:
            st.session_state['plan_active'] = True
            # Also load the plan for the next table immediately
            st.session_state['ai_analysis'] = next_table_plan['plan']
            st.session_state['plan_snapshot'] = next_table_plan['plan']
            st.session_state[f"where_clause_{next_table}"] = next_table_plan['where']
            st.session_state['plan_origin'] = 'saved'
        else:
            st.session_state['plan_active'] = False

        st.session_state['selected_table_info'] = (next_table, schema_name)

        # Reset state-a za sledeću tabelu
        keys_to_reset = ['ai_analysis', 'current_plan', 'last_rendered_table', 'plan_snapshot', 'plan_active']
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]

        st.success(f"✅ Saved! Moving to {next_table}...")
        time.sleep(0.5) # Kratka pauza da korisnik vidi poruku
        st.rerun()
    else:
        st.success("🎯 All tables finalized! Ready for Batch execution.")
        st.session_state['plan_active'] = False # All tables finalized, no active plan
        time.sleep(1)
        st.rerun()

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
                    st.session_state['plan_active'] = True # Set plan active
                    st.rerun()

    with col_btn2:
        if st.button("📂 Load Saved", width="stretch", key=f"load_btn_{table_name}"):
            saved_data = db.get_saved_plan(schema_name, table_name)
            if saved_data:
                st.session_state['ai_analysis'] = saved_data['plan']
                st.session_state['plan_snapshot'] = saved_data['plan']
                st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                st.session_state['plan_origin'] = 'saved'
                st.session_state['plan_active'] = True # Set plan active
                st.rerun()

    with col_btn3:
        if st.button("✍️ Manual", width="stretch", key=f"man_btn_{table_name}"):
            columns = db.get_columns(table_name, schema_name)
            st.session_state['ai_analysis'] = [{"column": c, "is_pii": False, "strategy": "keep", "reason": "Manual Entry"} for c in columns]
            st.session_state['plan_origin'] = 'new'
            st.session_state['plan_active'] = True # Set plan active
            st.rerun()

    with col_btn4:
        if st.button("👁️ View Data", width="stretch", key=f"view_btn_{table_name}"):
            st.info("Live Preview is available at the bottom 👇")

    # OVO JE BITNO - Audit log se iscrtava odmah ispod dugmića
    render_ai_audit_log()

def _find_multi_result(multi_results, table_name, schema_name):
    """Robust lookup za rezultate skeniranja nezavisno od formata ključa."""
    if not isinstance(multi_results, dict):
        return None

    base_name = str(table_name).split('.')[-1]
    candidates = [
        str(table_name),
        base_name,
        f"{schema_name}.{table_name}",
        f"{schema_name}.{base_name}",
    ]

    seen = set()
    for key in candidates:
        if key in seen:
            continue
        seen.add(key)
        if key in multi_results and multi_results.get(key):
            return multi_results.get(key)

    # Fallback: pokušaj match po "base table name" kada su ključevi različito formatirani
    for result_key, result_value in multi_results.items():
        if not result_value:
            continue
        if str(result_key).split('.')[-1] == base_name:
            return result_value

    return None

def _pick_first_table_by_execution_order(result_keys, execution_order):
    """
    Vraća prvu tabelu iz execution_order koja postoji u rezultatima skena.
    result_keys mogu biti i schema-qualified i base table nazivi.
    """
    normalized_result_keys = {str(k).split('.')[-1] for k in result_keys}
    for ordered_table in execution_order or []:
        if ordered_table in normalized_result_keys:
            return ordered_table
    return None

def render_planner_tab(db):
    st.subheader("Parallel AI Strategy Planner")
    st.caption("Scan selected tables first, then review and refine anonymization plans in dependency order.")
    selected_schema = st.session_state.get('selected_schema', 'public')

    # 1. DOHVATANJE FK RELACIJA
    if "all_schema_fks_by_schema" not in st.session_state:
        st.session_state["all_schema_fks_by_schema"] = {}
    if selected_schema not in st.session_state["all_schema_fks_by_schema"]:
        st.session_state["all_schema_fks_by_schema"][selected_schema] = db.get_all_foreign_keys(selected_schema)
    all_fks = st.session_state["all_schema_fks_by_schema"][selected_schema]

    # Initialize plan_active in session state
    if 'plan_active' not in st.session_state:
        st.session_state['plan_active'] = False

    # 2. GLOBALNI INTEGRITY SETTINGS (Sidebar)
    with st.sidebar:
        st.markdown("### ⛓️ Integrity Lock Settings")
        global_lock = st.checkbox("Force Referential Integrity", value=True, key="global_lock_check")
        global_integrity_val = st.text_input("Global ID Sync:", value="1", key="global_integrity_val")

    # --- 🛠️ DEBUG DASHBOARD ---
    with st.expander("🔍 DEBUG: Session State Inspector", expanded=False):
        col_db1, col_db2 = st.columns(2)
        with col_db1:
            st.write("**Multi-Scan Ključevi:**", list(st.session_state.get('multi_ai_analysis', {}).keys()))
        with col_db2:
            st.write("**Aktivni plan:**", "DA" if st.session_state.get('plan_active', False) else "NE")

    multi_scan_errors = {
        table_key: table_result.get('error')
        for table_key, table_result in st.session_state.get('multi_ai_analysis', {}).items()
        if isinstance(table_result, dict) and table_result.get('error')
    }
    if multi_scan_errors:
        st.warning("⚠️ Some tables failed during Parallel AI Scan.")
        for table_key, err_msg in multi_scan_errors.items():
            st.error(f"`{table_key}`: {err_msg}")

    # --- 1. GLOBALNA ANALIZA ---
    available_tables = db.get_tables(schema_name=selected_schema)

    # Ensure 'completed_tables' is initialized
    if 'completed_tables' not in st.session_state:
        st.session_state['completed_tables'] = set()

    # Populate 'completed_tables' from saved plans on initial load or rerun
    # This ensures tables with saved plans get the green checkmark
    for table_name_check in available_tables:
        if db.get_saved_plan(schema_name=selected_schema, table_name=table_name_check):
            st.session_state['completed_tables'].add(table_name_check)

    # Combine previously selected tables with completed tables for the multiselect default
    current_user_selection = st.session_state.get('selected_tables', [])
    initial_multiselect_default = list(set(current_user_selection) | st.session_state['completed_tables'])
    # Filter out any tables that might no longer exist in available_tables
    initial_multiselect_default = [t for t in initial_multiselect_default if t in available_tables]

    with st.expander("1) 🪄 Scanning", expanded=True):
        st.caption(f"Schema: `{selected_schema}`")
        selected_multi_tables = st.multiselect("Tables for parallel scan:",
                                               options=available_tables,
                                               default=initial_multiselect_default,
                                               key="planner_multiselect")
        st.session_state['selected_tables'] = selected_multi_tables

        c1, c2 = st.columns([1, 2])
        with c1:
            bulk_allow_sampling = st.checkbox("Dozvoli uzorak", value=True, key="bulk_allow_sample")
        with c2:
            bulk_sample_rows = st.slider("Broj redova", 1, 20, 5, key="bulk_sample_rows") if bulk_allow_sampling else 0

        if st.button("🪄 Parallel AI Scan", disabled=not selected_multi_tables, type="primary"):
            with st.status("Running parallel scan...", expanded=True) as scan_status:
                tables_to_scan = st.session_state.get('planner_multiselect', selected_multi_tables)
                scan_status.write(f"Queued tables: {', '.join(tables_to_scan)}")
                scan_status.write("Submitting scan tasks...")
                all_results = analyze_tables_parallel(db, tables_to_scan, schema=selected_schema,
                                                   allow_sampling=bulk_allow_sampling, sample_limit=bulk_sample_rows)
                scan_status.write("Collecting results...")
                st.session_state['multi_ai_analysis'] = all_results
                if all_results:
                    execution_order = st.session_state.get('all_tables_list', [])
                    if not execution_order:
                        execution_order = db.get_execution_order(tables_to_scan, selected_schema)
                        st.session_state['all_tables_list'] = execution_order

                    first_scanned_table = _pick_first_table_by_execution_order(
                        all_results.keys(),
                        execution_order
                    ) or str(next(iter(all_results.keys()))).split('.')[-1]
                    st.session_state['selected_table_info'] = (first_scanned_table, selected_schema)

                    first_result = _find_multi_result(all_results, first_scanned_table, selected_schema)
                    if first_result:
                        first_plan = first_result.get('plan') if isinstance(first_result, dict) else getattr(first_result, 'plan', None)
                        first_audit = first_result.get('audit', []) if isinstance(first_result, dict) else getattr(first_result, 'audit', [])
                        first_error = first_result.get('error') if isinstance(first_result, dict) else None
                        if first_plan:
                            st.session_state['ai_analysis'] = first_plan
                            st.session_state['last_ai_audit'] = first_audit or []
                            st.session_state['last_rendered_table'] = first_scanned_table
                            st.session_state['plan_active'] = True
                        else:
                            st.session_state.pop('ai_analysis', None)
                            st.session_state.pop('last_rendered_table', None)
                            if first_error:
                                st.warning(f"⚠️ First scanned table `{first_scanned_table}` failed: {first_error}")

                    st.session_state.pop('current_plan', None)
                st.success("Analiza završena!")
                st.session_state['plan_active'] = True
                scan_status.update(label="Parallel scan completed", state="complete")
                st.rerun()

        if st.session_state.get('multi_ai_analysis'):
            st.write("**Scan results:**")
            st.json(st.session_state.get('multi_ai_analysis', {}))

    # --- 2. LANAC PROGRESA ---
    all_tables_list = st.session_state.get('all_tables_list', [])
    completed_tables = st.session_state.get('completed_tables', set())

    # --- 3. RAD SA POJEDINAČNOM TABELOM ---
    if 'selected_table_info' not in st.session_state and st.session_state.get('multi_ai_analysis'):
        multi_results = st.session_state['multi_ai_analysis']
        execution_order = st.session_state.get('all_tables_list', [])
        if not execution_order:
            fallback_selected = st.session_state.get('selected_tables', [])
            if fallback_selected:
                execution_order = db.get_execution_order(fallback_selected, selected_schema)
                st.session_state['all_tables_list'] = execution_order

        fallback_table = _pick_first_table_by_execution_order(
            multi_results.keys(),
            execution_order
        )
        if not fallback_table:
            first_result_key = next(iter(multi_results.keys()), None)
            fallback_table = str(first_result_key).split('.')[-1] if first_result_key else None

        if fallback_table:
            st.session_state['selected_table_info'] = (fallback_table, selected_schema)

    with st.expander("2) 📋 Planning", expanded=False):
        if all_tables_list:
            st.info("Execution Order: " + " -> ".join(all_tables_list))
        render_status_chain(all_tables_list, completed_tables)

        selectable_tables = all_tables_list or st.session_state.get('selected_tables', [])
        current_table_selection = st.session_state.get('selected_table_info', (None, selected_schema))[0] if st.session_state.get('selected_table_info') else None
        if selectable_tables:
            default_idx = selectable_tables.index(current_table_selection) if current_table_selection in selectable_tables else 0
            selected_active_table = st.selectbox("Active planning table", selectable_tables, index=default_idx, key="active_planning_table")
            if st.session_state.get('selected_table_info', (None, None))[0] != selected_active_table:
                st.session_state['selected_table_info'] = (selected_active_table, selected_schema)
                st.session_state.pop('ai_analysis', None)
                st.session_state.pop('current_plan', None)
                st.session_state.pop('last_rendered_table', None)

        st.info("ℹ️ ID/PK/FK columns are protected. If AI proposes `mask`, it is automatically forced to `hash`.")

        if 'selected_table_info' in st.session_state:
            table_info = st.session_state['selected_table_info']
            table_name = table_info[0] if isinstance(table_info, tuple) else table_info
            schema_name = table_info[1] if isinstance(table_info, tuple) else selected_schema

            # 🛡️ Inicijalizacija vitalnih varijabli
            violation_found = False
            editor_key = f"plan_editor_{table_name}"
            where_key = f"where_clause_{table_name}"

            # SINHRONIZACIJA (Auto-load)
            multi_results = st.session_state.get('multi_ai_analysis', {})
            if 'ai_analysis' not in st.session_state or st.session_state.get('last_rendered_table') != table_name:
                found_res = _find_multi_result(multi_results, table_name, schema_name)
                found_plan = found_res.get('plan') if isinstance(found_res, dict) else getattr(found_res, 'plan', None) if found_res else None
                found_audit = found_res.get('audit', []) if isinstance(found_res, dict) else getattr(found_res, 'audit', []) if found_res else []
                found_error = found_res.get('error') if isinstance(found_res, dict) else None
                if found_plan:
                    st.session_state['ai_analysis'] = found_plan
                    st.session_state['last_ai_audit'] = found_audit or []
                    st.session_state['last_rendered_table'] = table_name
                    st.session_state['plan_active'] = True # Set plan active
                else:
                    if found_error:
                        st.error(f"❌ Parallel scan failed for `{table_name}`: {found_error}")
                    # Nema plana u multi_ai_analysis, pokušavamo da učitamo iz sačuvanih planova u bazi
                    saved_data = db.get_saved_plan(schema_name, table_name)
                    if saved_data:
                        st.session_state['ai_analysis'] = saved_data['plan']
                        st.session_state['plan_snapshot'] = saved_data['plan'] # Održavamo plan_snapshot konzistentnim
                        st.session_state[f"where_clause_{table_name}"] = saved_data['where']
                        st.session_state['plan_origin'] = 'saved'
                        st.session_state['last_rendered_table'] = table_name
                        st.session_state['plan_active'] = True # Set plan active
                    else:
                        # Ako plan nije pronađen nigde, osiguravamo da je ai_analysis obrisan i plan_active je False
                        if 'ai_analysis' in st.session_state:
                            del st.session_state['ai_analysis']
                        st.session_state['plan_active'] = False

            handle_navigation_history(table_name, schema_name)
            # 🛡️ FK/PK Identifikacija
            current_table_col_details = db.get_column_details(table_name, schema_name)
            current_table_fks = [fk[1] for fk in all_fks if fk[0] == table_name]
            if f"pk_{table_name}" not in st.session_state:
                st.session_state[f"pk_{table_name}"] = db.get_primary_keys(schema_name, table_name)
            real_pks = st.session_state[f"pk_{table_name}"]
            table_columns = list(current_table_col_details.keys())

            left_col, right_col = st.columns([1, 2.2], gap="large")

            with left_col:
                render_table_header_info(schema_name, table_name)
                render_planner_action_buttons(db, table_name, schema_name)

            with right_col:
                # --- DATA EDITOR & VALIDATION ---
                if 'ai_analysis' in st.session_state and st.session_state['ai_analysis']:
                    plan_df = pd.DataFrame(st.session_state['ai_analysis'])

                    # Ako je DataFrame ipak prazan (nema redova)
                    if plan_df.empty:
                        st.warning("⚠️ Plan anonimizacije je prazan. Pokreni skeniranje ponovo.")
                        return

                    # Defanzivna provera kolona
                    available_cols = plan_df.columns.tolist()
                    col_key = next((c for c in available_cols if c.lower() == 'column'), None)

                    if col_key is None:
                        st.error(f"❌ Greška u strukturi podataka. Nađene kolone: {available_cols}")
                        return

                    def get_col_status(col):
                        if col in real_pks: return "🔑 PK (Locked)"
                        if col in current_table_fks: return "🔗 FK (Dependent)"
                        return "✅ Normal"

                    plan_df['status'] = plan_df[col_key].apply(get_col_status)
                    plan_df['guard'] = plan_df[col_key].apply(
                        lambda c: "ℹ️ Mask disabled for ID safety" if any(token in str(c).lower() for token in ["id", "pk", "fk"]) else ""
                    )

                    # Auto-Fix za FK/PK kolone: mask nije dozvoljen za identifikatore
                    locked_mask = plan_df['status'].str.contains("Locked|Dependent", na=False)
                    id_name_mask = plan_df[col_key].astype(str).str.contains(r"(id|pk|fk)", case=False, regex=True)
                    if 'strategy' in plan_df.columns:
                        plan_df.loc[(locked_mask | id_name_mask) & (plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'

                    edited_plan_df = st.data_editor(
                        plan_df,
                        column_config={
                            "status": st.column_config.TextColumn("Status", disabled=True),
                            "guard": st.column_config.TextColumn("Constraint", disabled=True),
                            col_key: st.column_config.TextColumn("Column", disabled=True),
                            "strategy": st.column_config.SelectboxColumn("Strategy", options=["keep", "hash", "mask", "null", "faker_name"], required=True),
                        },
                        hide_index=True, key=editor_key, use_container_width=True
                    )
                    # Hard constraint post-edit: ID-like columns cannot remain on mask.
                    if 'strategy' in edited_plan_df.columns:
                        edited_id_mask = edited_plan_df[col_key].astype(str).str.contains(r"(id|pk|fk)", case=False, regex=True)
                        edited_plan_df.loc[edited_id_mask & (edited_plan_df['strategy'] == 'mask'), 'strategy'] = 'hash'
                    st.session_state['current_plan'] = edited_plan_df.to_dict('records')
                else:
                    st.info("💡 Izaberi tabelu i pokreni analizu da bi video plan anonimizacije.")

                st.write("")
                c_act1, c_act2, c_act3 = st.columns([1, 1, 1])

                with c_act1:
                    if st.button("👁️ Preview (10 rows)", use_container_width=True, disabled=violation_found, key=f"pre_btn_{table_name}"):
                        clean_plan = get_clean_plan(st.session_state.get('current_plan', []))
                        with st.spinner("Generišem preview..."):
                            raw_data = db.read_table(table_name, schema_name, where=st.session_state.get(where_key, ""), limit=10)
                            anon_df = db.apply_anonymization_rules(raw_data, clean_plan)
                            st.dataframe(anon_df, use_container_width=True)

                with c_act2:
                    if st.button("🚀 Run & Save to Anon", use_container_width=True, disabled=violation_found, key=f"run_btn_{table_name}"):
                        clean_plan = get_clean_plan(st.session_state.get('current_plan', []))
                        with st.spinner(f"Snimam u anon.{table_name}..."):
                            try:
                                full_data = db.read_table(table_name, schema_name, where=st.session_state.get(where_key, ""))
                                final_df = db.apply_anonymization_rules(full_data, clean_plan)
                                db.save_anonymized_table(final_df, table_name, target_schema='anon')
                                st.success("✅ Uspešno migrirano!")
                            except Exception as e:
                                st.error(f"Greška: {e}")

                with c_act3:
                    next_t = get_next_table_in_chain(table_name, all_tables_list, completed_tables)
                    btn_label = "💾 Confirm & Next" if next_t else "🏁 Finish"
                    if st.button(btn_label, type="primary", use_container_width=True, disabled=violation_found, key=f"conf_btn_{table_name}"):
                        save_and_move_to_next(db, table_name, schema_name, st.session_state.get('current_plan', []), st.session_state.get(where_key, ""))

            with st.expander("3) 🔒 Privacy Settings", expanded=False):
                p_col1, p_col2, p_col3 = st.columns(3)
                with p_col1:
                    st.session_state['anonify_level'] = st.selectbox(
                        "Anonify Level",
                        options=["Balanced", "Strict", "Maximum"],
                        index=["Balanced", "Strict", "Maximum"].index(st.session_state.get('anonify_level', 'Balanced'))
                    )
                with p_col2:
                    st.session_state['compliance_mode'] = st.selectbox(
                        "Compliance Mode",
                        options=["GDPR", "HIPAA"],
                        index=["GDPR", "HIPAA"].index(st.session_state.get('compliance_mode', 'GDPR'))
                    )
                with p_col3:
                    st.session_state['seed_management'] = st.text_input(
                        "Seed Management",
                        value=st.session_state.get('seed_management', 'default_seed')
                    )
                st.caption("Privacy settings are applied as policy guidance for planning and execution.")

            with st.expander("4) ⚙️ Filter & Consistency", expanded=False):
                is_locked = st.session_state.get('global_lock_check', False)
                g_val = st.session_state.get('global_integrity_val', '1')
                row_limit = st.number_input("Row preview limit", min_value=10, max_value=1000, value=100, step=10, key=f"row_limit_{table_name}")
                st.session_state['last_limit_val'] = int(row_limit)
                integrity_where = None
                if is_locked:
                    if "customer_id" in table_columns and table_name != "order_items":
                        q_val = _get_quoted_value("customer_id", current_table_col_details, g_val)
                        integrity_where = f"customer_id = {q_val}"
                    elif table_name == "customers" and "id" in table_columns:
                        q_val = _get_quoted_value("id", current_table_col_details, g_val)
                        integrity_where = f"id = {q_val}"
                    elif table_name == "order_items" and "order_id" in table_columns:
                        orders_meta = db.get_column_details("orders", schema_name)
                        q_val = _get_quoted_value("customer_id", orders_meta, g_val)
                        integrity_where = f"order_id IN (SELECT order_id FROM {schema_name}.orders WHERE customer_id = {q_val})"

                if is_locked and integrity_where:
                    use_suggestion = st.checkbox(f"Use Deep Integrity Sync", value=True, key=f"suggest_chk_{table_name}")
                    if use_suggestion:
                        if st.session_state.get(where_key) != integrity_where:
                            st.session_state[where_key] = integrity_where
                        st.info(f"⛓️ **Deep RI Lock Active:** `{integrity_where}`")
                        st.text_input("Active SQL Filter:", value=integrity_where, disabled=True, key=f"locked_in_{where_key}")
                    else:
                        st.session_state[where_key] = st.text_input(
                            "SQL WHERE Clause (Custom):",
                            value=st.session_state.get(where_key, ""),
                            key=f"in_custom_{where_key}"
                        )
                else:
                    st.session_state[where_key] = st.text_input(
                        "SQL WHERE Clause (Independent):",
                        value=st.session_state.get(where_key, ""),
                        key=f"in_{where_key}"
                    )

            col1, col2, _ = st.columns([1, 1, 4])
            history = st.session_state.get('navigation_history', [])
            pointer = st.session_state.get('history_pointer', 0)
            with col1:
                if st.button("⬅️ Back", disabled=(pointer <= 0), width="stretch", key=f"flow_back_{table_name}"):
                    st.session_state['history_pointer'] -= 1
                    st.session_state['selected_table_info'] = history[st.session_state['history_pointer']]
                    st.rerun()
            with col2:
                if st.button("Next ➡️", disabled=(pointer >= len(history) - 1), width="stretch", key=f"flow_next_{table_name}"):
                    st.session_state['history_pointer'] += 1
                    st.session_state['selected_table_info'] = history[st.session_state['history_pointer']]
                    st.rerun()
        else:
            st.info("👋 Select a table to start.")

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
                st.dataframe(raw_sample, use_container_width=True)
            with c2:
                st.write(f"**🛡️ Anonymized Preview (Locale: {current_locale.upper()})**")
                st.dataframe(anon_sample, use_container_width=True)

            # Export Sekcija
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
        st.info("👋 Select a table in the **Explorer** or **Plan** tab to enable live preview here.")