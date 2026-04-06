# -*- coding: utf-8 -*-
import streamlit as st
import time
import datetime

def handle_batch_execution(db, ordered_tables, selected_schema, target_schema, selected_tables, instance_id="default"):
    """Handles the UI logic for batch anonymization with overwrite protection and unique keys."""

    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
    # print(f"[{timestamp}] DEBUG: Entering handle_batch_execution")
    # print(f"[{timestamp}] DEBUG: instance_id='{instance_id}', target_schema='{target_schema}'")

    # Provera ključeva koji će biti korišćeni
    main_key = f"{instance_id}_run_main"
    # print(f"[{timestamp}] DEBUG: Main Button Key generated: '{main_key}'")

    # 1. Inicijalizacija session_state-a
    if 'show_overwrite_warning' not in st.session_state:
        st.session_state['show_overwrite_warning'] = False
    if 'start_batch_proc' not in st.session_state:
        st.session_state['start_batch_proc'] = False

    # Unikatan identifikator za ovu sesiju/šemu da bismo izbegli Duplicate ID Error
    safe_key = target_schema.replace(".", "_")

    # 2. Glavno dugme za pokretanje provere
    if st.button("🔥 RUN FULL ANONYMIZATION",
                 type="primary",
                 width="stretch",
                 key=f"{instance_id}_run_main"):

        tables_with_data = []

        # Koristimo progress bar jer provera više tabela može da potraje sekundu-dve
        with st.spinner("Checking target database for existing data..."):
            for t in ordered_tables:
                try:
                    # 1. Provera postojanja tabele (Case-insensitive check)
                    exists = db.table_exists(t, target_schema)

                    if exists:
                        # 2. Ako postoji, proveri broj redova
                        count = db.get_row_count(t, target_schema)

                        # Ako ima podataka, dodajemo na listu za upozorenje
                        if count > 0:
                            tables_with_data.append(t)
                except Exception as e:
                    # Logujemo grešku u konzolu, ali ne prekidamo proces
                    print(f"DEBUG: Error checking table {target_schema}.{t}: {e}")
                    continue

        # --- LOGIKA ZA PRIKAZ UPOZORENJA ---
        if tables_with_data:
            # Pronađeni su podaci - palimo crveni alarm
            st.session_state['tables_to_overwrite'] = tables_with_data
            st.session_state['show_overwrite_warning'] = True
            st.rerun()
        else:
            # Ciljna šema je prazna ili ne postoji - letimo odmah!
            st.session_state['start_batch_proc'] = True
            st.rerun()

    # 3. Warning Dijalog (MODIFIKOVAN: Dodata informacija o DDL Resetu)
    if st.session_state.get('show_overwrite_warning'):
        st.markdown("---")
        st.error("🚨 **Critical: Data & Schema Reset Required**")
        st.warning(f"⚠️ **Target schema `{target_schema}` contains data.**")

        st.write(f"""
        Running anonymization now will trigger an **Enterprise Schema Sync**:
        1. **TRUNCATE**: Svi postojeći podaci u ciljnim tabelama biće obrisani (od dece ka roditeljima).
        2. **DDL ALTER**: Tipovi kolona (npr. Integer -> Varchar) biće usklađeni sa planom anonimizacije.
        3. **DATA LOAD**: Ubaciće se novi, anonimizovani podaci.
        """)

        with st.expander("Pregled tabela koje će biti resetovane"):
            st.code(", ".join(st.session_state['tables_to_overwrite']))

        st.write("Da li ste sigurni da želite da nastavite? Ova operacija je nepovratna.")

        cw1, cw2 = st.columns(2)

        # DODATI UNIKATNI KLJUČEVI ZA DIJALOG
        if cw1.button("✅ Yes, Reset and Proceed",
                      width="stretch",
                      type="primary",
                      key=f"{instance_id}_confirm_ok"):
            st.session_state['show_overwrite_warning'] = False
            st.session_state['start_batch_proc'] = True
            st.rerun()

        if cw2.button("🔙 Back / Cancel",
                      width="stretch",
                      key=f"{instance_id}_confirm_back"):
            st.session_state['show_overwrite_warning'] = False
            st.rerun()
        st.markdown("---")

    # 4. Izvršni Pipeline
    if st.session_state.get('start_batch_proc'):
        with st.status("Executing Enterprise Pipeline...", expanded=True) as status:
            try:
                full_plan = {}

                # DEBUG: Provera u konzoli
                print(f"DEBUG: Start Batch Proc for Schema: {selected_schema}")

                # Koristimo selected_tables kao master listu jer nju proveravamo na kraju
                for t in selected_tables:
                    p = db.get_saved_plan(selected_schema, t)
                    if p:
                        full_plan[t] = p
                    else:
                        print(f"DEBUG: Plan NOT FOUND for {selected_schema}.{t}")

                # Provera da li imamo sve planove
                # Koristimo setove za poređenje da izbegnemo greške u redosledu
                missing = [t for t in selected_tables if t not in full_plan]

                # Pre nego što pozoveš execute_anonymization_batch
                st.write(f"DEBUG: Tražim planove za šemu: `{selected_schema}`")
                st.write(f"DEBUG: Tabele u planu: `{list(full_plan.keys())}`")
                st.write(f"DEBUG: Tabele koje sistem očekuje: `{selected_tables}`")


                if missing:
                    st.error(f"❌ Missing plans for: {', '.join(missing)}")
                    # Dodatni info za tebe tokom debaigovanja
                    st.write(f"Proveravam šemu: `{selected_schema}`")
                    st.session_state['start_batch_proc'] = False
                    st.stop()

                # Pokretanje (Ovde prosleđujemo full_plan koji sada garantovano ima sve što treba)
                # Napomena: db.execute_anonymization_batch bi unutra trebalo da koristi
                # svoj mehanizam za sortiranje tabela po FK redosledu
                db.execute_anonymization_batch(selected_schema, target_schema, full_plan)

                status.update(label="✅ Success!", state="complete")
                st.info("Operacija uspešno završena.")

            except Exception as e:
                st.error(f"Error during execution: {str(e)}")
                import traceback
                print(traceback.format_exc()) # Ispisuje ceo stack trace u konzolu
            finally:
                st.session_state['start_batch_proc'] = False