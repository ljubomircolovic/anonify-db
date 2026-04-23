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
            debug_area = st.empty() 
            with debug_area.container():
                
                # --- KLJUČNI DODATAK: ČIŠĆENJE PRE SVEGA ---
                st.write("🧹 **Korak 1: Resetovanje ciljne šeme (TRUNCATE CASCADE)...**")
                try:
                    # Prosleđujemo ordered_tables da bi brisao ispravnim redosledom
                    db.truncate_anon_tables(target_schema, ordered_tables)
                    st.success("✅ Ciljna šema je očišćena.")
                except Exception as e:
                    st.error(f"❌ Error while cleaning schema: {e}")
                    st.stop() # Zaustavljamo proces ako čišćenje ne uspe
                # ------------------------------------------

                st.write("🔍 **Korak 2: Provera planova i priprema transformacije...**")
                
                try:
                    full_plan = {}
                    for t_name in selected_tables:
                        # ... tvoj postojeći kod za get_saved_plan ...
                        saved = db.get_saved_plan(selected_schema, t_name)
                        
                        if saved:
                            current_plan = saved.get('plan')
                            
                            # Ispisujemo tip podatka direktno u aplikaciji
                            st.write(f"🔹 Tabela: `{t_name}` | Tip plana: `{type(current_plan)}`")
                            
                            # Ako je string, popravljamo i ispisujemo to
                            if isinstance(current_plan, str):
                                st.warning(f"⚠️ Plan za `{t_name}` je stigao kao STRING. Popravljam...")
                                import json
                                current_plan = json.loads(current_plan)
                            
                            # PROVERA STRUKTURE: Da li su unutra rečnici?
                            if isinstance(current_plan, list) and len(current_plan) > 0:
                                first_item_type = type(current_plan[0])
                                st.write(f"   ∟ Prvi element plana je tipa: `{first_item_type}`")
                                if not isinstance(current_plan[0], dict):
                                    st.error(f"❌ ERROR: Element nije DICT! Vrednost: `{current_plan[0]}`")

                            full_plan[t_name] = {
                                "plan": current_plan,
                                "where": saved.get('where', "")
                            }
                        else:
                            st.error(f"❌ Nema plana za tabelu: `{t_name}`")

                    # Konačna provera pre poziva batch-a
                    st.write("🚀 **Pozivam db.execute_anonymization_batch...**")
                    
                    # IZVRŠAVANJE
                    db.execute_anonymization_batch(selected_schema, target_schema, full_plan, ordered_tables)

                    status.update(label="✅ Success!", state="complete")
                    st.success("Operation completed successfully.")

                except Exception as e:
                    import traceback
                    # ISPISUJEMO CEO TRACEBACK NA EKRAN DA GA VIDIŠ
                    st.error(f"💥 KRITIČNA GREŠKA: {str(e)}")
                    st.code(traceback.format_exc(), language="python") # Ovo menja terminal!
                    
