# -*- coding: utf-8 -*-
import streamlit as st
import time
import datetime

def handle_batch_execution(db, ordered_tables, selected_schema, target_schema, selected_tables, instance_id="default"):
    """Handles the UI logic for batch anonymization with overwrite protection and unique keys."""

    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
    print(f"[{timestamp}] DEBUG: Entering handle_batch_execution")
    print(f"[{timestamp}] DEBUG: instance_id='{instance_id}', target_schema='{target_schema}'")

    # Provera ključeva koji će biti korišćeni
    main_key = f"{instance_id}_run_main"
    print(f"[{timestamp}] DEBUG: Main Button Key generated: '{main_key}'")

    # 1. Inicijalizacija session_state-a
    if 'show_overwrite_warning' not in st.session_state:
        st.session_state['show_overwrite_warning'] = False
    if 'start_batch_proc' not in st.session_state:
        st.session_state['start_batch_proc'] = False

    # Unikatan identifikator za ovu sesiju/šemu da bismo izbegli Duplicate ID Error
    # Koristimo target_schema jer se on menja u zavisnosti od selekcije
    safe_key = target_schema.replace(".", "_")

    # 2. Glavno dugme za pokretanje provere
    # DODAT KEY: 'btn_run_main_{safe_key}'

    # 2. Glavno dugme za pokretanje provere
    if st.button("🔥 RUN FULL ANONYMIZATION",
                 type="primary",
                 use_container_width=True,
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

    # 3. Warning Dijalog (Pojavljuje se samo ako ima podataka)
    if st.session_state.get('show_overwrite_warning'):
        st.markdown("---")
        st.warning(f"⚠️ **Data Overwrite Warning!**")
        st.write(f"The following tables in `{target_schema}` already contain data:")
        st.code(", ".join(st.session_state['tables_to_overwrite']))
        st.write("Running anonymization will **potentially change** these values. Proceed?")

        cw1, cw2 = st.columns(2)

        # DODATI UNIKATNI KLJUČEVI ZA DIJALOG
        if cw1.button("✅ OK, Proceed",
                      use_container_width=True,
                      type="primary",
                      key=f"{instance_id}_confirm_ok"):
            st.session_state['show_overwrite_warning'] = False
            st.session_state['start_batch_proc'] = True
            st.rerun()

        if cw2.button("🔙 Back",
                      use_container_width=True,
                      key=f"{instance_id}_confirm_back"):
            st.session_state['show_overwrite_warning'] = False
            st.rerun()
        st.markdown("---")

    # 4. Izvršni Pipeline
    if st.session_state.get('start_batch_proc'):
        with st.status("Executing Enterprise Pipeline...", expanded=True) as status:
            try:
                full_plan = {}
                for t in ordered_tables:
                    p = db.get_saved_plan(selected_schema, t)
                    if p:
                        full_plan[t] = p

                # Provera da li imamo sve planove pre nego što krenemo da gazimo bazu
                if len(full_plan) < len(selected_tables):
                    missing = [t for t in selected_tables if t not in full_plan]
                    st.error(f"❌ Missing plans for: {', '.join(missing)}")
                    st.session_state['start_batch_proc'] = False
                    st.stop()

                # Pokretanje masovne anonimizacije
                db.execute_anonymization_batch(selected_schema, target_schema, full_plan)

                status.update(label="✅ Success!", state="complete")
                #st.balloons()
                st.info("Operacija završena.")
            except Exception as e:
                st.error(f"Error during execution: {str(e)}")
            finally:
                # Obavezno gasimo flag da se proces ne bi ponavljao pri svakom rerun-u
                st.session_state['start_batch_proc'] = False