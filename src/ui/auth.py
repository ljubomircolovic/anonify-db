# -*- coding: utf-8 -*-
import streamlit as st
import os

def check_login():
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if not st.session_state['authenticated']:
        left, center, right = st.columns([1.5, 2, 1.5])
        with center:
            st.title("🛡️ AnonifyDB")
            st.caption("Database Anonymization for AI & Dev")
            st.subheader("Sign In")
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
                        st.warning("Invalid username or password. Please try again.")
        return False
    return True