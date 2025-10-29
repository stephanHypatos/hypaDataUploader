# app.py
import streamlit as st

from helpers import (
    DEFAULT_BASE_URL,
    AUTH_PATH,
    INVOICES_INSERT_PATH,
    ensure_token,
)

from pages.invoices import render_invoices_page
from pages.lookup_tables import render_lookup_tables_page
from pages.suppliers import render_suppliers_page 
from pages.companies import render_companies_page

def main():
    st.set_page_config(page_title="Hypatos Uploader", page_icon="🧾", layout="centered")
    st.title("🧾 Hypatos Uploader")

    # ---- Global API config (shared for all pages) ----
    with st.sidebar:
        st.header("Connection")
        base_url = st.text_input("Base URL", value=DEFAULT_BASE_URL)
        auth_path = st.text_input("Auth token path", value=AUTH_PATH)
        client_id = st.text_input("Client ID")
        client_secret = st.text_input("Client Secret", type="password")

        # Persist config in session so pages can read it
        st.session_state.setdefault("base_url", base_url)
        st.session_state.setdefault("auth_path", auth_path)
        st.session_state.setdefault("client_id", client_id)
        st.session_state.setdefault("client_secret", client_secret)

        st.session_state["base_url"] = base_url
        st.session_state["auth_path"] = auth_path
        st.session_state["client_id"] = client_id
        st.session_state["client_secret"] = client_secret

        # Small auth utility button (not mandatory; pages will auto-refresh token if needed)
        if st.button("🔑 Get/Refresh Token"):
            ok, msg = ensure_token(base_url, client_id, client_secret, auth_path)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

        st.divider()
        page = st.radio(
            "Select page",
            options=[
                "Upload Invoices",
                "Lookup Tables",
                "Ingest Suppliers",
                "Ingest Companies"
            ],
            index=0,
        )

    # ---- Route to page ----
    if page == "Upload Invoices":
        # Pass the insert path for invoices; other pages build their own endpoint
        render_invoices_page(insert_path=ENRICHMENT_INSERT_PATH)
    elif page == "Lookup Tables":
        render_lookup_tables_page()
    elif page == "Ingest Suppliers":
        render_suppliers_page()
    elif page == "Ingest Companies":
        render_companies_page()
    else:
        st.info("Page not implemented yet.")


if __name__ == "__main__":
    main()
