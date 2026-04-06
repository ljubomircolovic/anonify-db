# -*- coding: utf-8 -*-
import streamlit as st
import os

def check_login():
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if not st.session_state['authenticated']:
        st.title("🔐 AnonifyDB Login")
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login", width="stretch")

            if submitted:
                if username == os.getenv("APP_ADMIN_USER") and password == os.getenv("APP_ADMIN_PASSWORD"):
                    st.session_state['authenticated'] = True
                    st.session_state['user_name'] = username
                    st.rerun()
                else:
                    st.error("Invalid Username or Password")
        return False
    return True