import streamlit as st

def handle_navigation_history(table_name, schema_name):
    """Upravlja istorijom kretanja kroz tabele."""
    current_nav = (table_name, schema_name)
    history = st.session_state.get('navigation_history', [])
    pointer = st.session_state.get('history_pointer', 0)

    if not history or history[pointer] != current_nav:
        st.session_state['navigation_history'] = history[:pointer + 1]
        st.session_state['navigation_history'].append(current_nav)
        st.session_state['history_pointer'] = len(st.session_state['navigation_history']) - 1

def render_nav_buttons():
    """Crta Back i Next dugmad."""
    n_col1, n_col2, _ = st.columns([1, 1, 8])
    history = st.session_state.get('navigation_history', [])
    pointer = st.session_state.get('history_pointer', 0)

    with n_col1:
        if st.button("⬅️ Back", disabled=(pointer <= 0), width="stretch"):
            st.session_state['history_pointer'] -= 1
            st.session_state['selected_table_info'] = history[st.session_state['history_pointer']]
    with n_col2:
        if st.button("Next ➡️", disabled=(pointer >= len(history) - 1), width="stretch"):
            st.session_state['history_pointer'] += 1
            st.session_state['selected_table_info'] = history[st.session_state['history_pointer']]

def get_next_table_in_chain(current_table, all_tables, completed_tables):
    """
    Pronalazi sledeću tabelu iz lanca koja još nije završena.
    Ako su sve posle trenutne završene, vraća prvu sledeću dostupnu.
    """
    if not all_tables:
        return None

    try:
        current_idx = all_tables.index(current_table)
    except ValueError:
        return all_tables[0] # Ako nismo u listi, kreni od početka

    # Gledamo sve tabele nakon trenutne
    for next_table in all_tables[current_idx + 1:]:
        if next_table not in completed_tables:
            return next_table

    # Ako nismo našli nijednu nezavršenu "ispred", proveri celu listu od početka
    for table in all_tables:
        if table not in completed_tables and table != current_table:
            return table

    return None # Sve su završene