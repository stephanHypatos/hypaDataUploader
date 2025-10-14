# pages/invoices.py
import json
from datetime import datetime

import requests
import streamlit as st

from helpers import (
    make_sample_csv_bytes,
    make_scenarios_csv_bytes,
    read_csv_grouped_by_external_id,
    build_invoice_payload_from_rows,
    bearer_headers,
    ensure_token,
)


def render_invoices_page(insert_path: str):
    st.caption("Upload a CSV of invoice lines → transform → POST to Hypatos Enrichment API")

    # Read shared config from session
    base_url = st.session_state.get("base_url", "")
    auth_path = st.session_state.get("auth_path", "")
    client_id = st.session_state.get("client_id", "")
    client_secret = st.session_state.get("client_secret", "")

    uploaded_csv = st.file_uploader("Upload CSV (invoice lines)", type=["csv"])

    with st.expander("📥 Sample CSV downloads", expanded=False):
        st.download_button(
            label="Download sample (basic, 2 invoices / 3 lines)",
            data=make_sample_csv_bytes(with_gl_cc=False),
            file_name="sample_invoices.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            label="Download sample (with GL & Cost Center)",
            data=make_sample_csv_bytes(with_gl_cc=True),
            file_name="sample_invoices_with_gl_cc.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            label="Download sample (scenario-rich: FI & PO cases)",
            data=make_scenarios_csv_bytes(),
            file_name="demo_invoices_scenarios.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with st.expander("Optional header overrides (used if missing in CSV, or in Test Mode)"):
        override_external_client_id = st.text_input("externalClientId (fallback/test)", value="CLIENT-TEST")
        override_external_company_id = st.text_input("externalCompanyId (fallback/test)", value="COMPANY-TEST")
        override_external_supplier_id = st.text_input("externalSupplierId (fallback/test)", value="SUPPLIER-TEST")
        override_currency = st.text_input("currency (ISO 4217, e.g. EUR)", value="EUR")
        override_document_id = st.text_input("documentId", value="DOC-TEST-001")
        override_external_id = st.text_input("externalId (invoice id)", value="TEST-INVOICE-001")

    header_tax_mode = st.toggle(
        "Provide total tax at header (ignore per-line 'totalTaxAmount')",
        value=False,
        help=(
            "OFF = Read tax on each line and sum to header. "
            "ON = Read 'totalTaxAmount' once from the first row and do NOT include "
            "'totalTaxAmount' on invoice lines in the payload."
        ),
    )

    dry_run = st.toggle("Dry run (do not POST, just preview JSON)", value=True)
    pretty = st.toggle("Pretty-print JSON", value=True)
    test_mode = st.checkbox("Test without CSV (use only overrides & dummy line)", value=False)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔑 Get Access Token"):
            ok, msg = ensure_token(base_url, client_id, client_secret, auth_path)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

    with col2:
        if st.button("🚀 Transform & Send"):
            # Require either a CSV or test mode
            if not uploaded_csv and not test_mode:
                st.error("Please upload a CSV or enable 'Test without CSV'.")
                st.stop()

            # Prepare groups (either from CSV or using a single dummy row)
            if test_mode:
                dummy_row = {
                    "externalId": (override_external_id or "TEST-INVOICE-001").strip() or "TEST-INVOICE-001",
                    "externalClientId": (override_external_client_id or "").strip() or "CLIENT-TEST",
                    "externalCompanyId": (override_external_company_id or "").strip() or "COMPANY-TEST",
                    "externalSupplierId": (override_external_supplier_id or "").strip() or "SUPPLIER-TEST",
                    "currency": (override_currency or "").strip() or "EUR",
                    "documentId": (override_document_id or "").strip() or "DOC-TEST-001",
                    "issuedDate": datetime.utcnow().strftime("%Y-%m-%d"),
                    "line.externalId": "LINE-1",
                    "quantity": "1",
                    "unitPrice": "100.00",
                    "netAmount": "100.00",
                    "totalTaxAmount": "19.00",
                    "grossAmount": "119.00",
                    "itemText": "Dummy service for testing"
                }
                groups = {dummy_row["externalId"]: [dummy_row]}
            else:
                try:
                    groups = read_csv_grouped_by_external_id(uploaded_csv)
                except Exception as e:
                    st.error(f"CSV error: {e}")
                    st.stop()

            # Transform and (optionally) POST
            results = []
            for ext_id, rows in groups.items():
                overrides = {
                    "external_client_id": override_external_client_id or None,
                    "external_company_id": override_external_company_id or None,
                    "external_supplier_id": override_external_supplier_id or None,
                    "currency": override_currency or None,
                    "document_id": override_document_id or None,
                    "external_id": override_external_id or None,
                }
                try:
                    payload = build_invoice_payload_from_rows(rows, overrides, header_tax_mode=header_tax_mode)
                except Exception as e:
                    results.append((ext_id, None, f"Build payload error: {e}", None))
                    continue

                body = json.dumps(payload, indent=2) if pretty else json.dumps(payload)

                # Dry-run? Just show the JSON
                if dry_run:
                    results.append((ext_id, 0, "Dry run: not sent", body))
                    continue

                # POST to enrichment insert
                try:
                    ok, msg = ensure_token(base_url, client_id, client_secret, auth_path)
                    if not ok:
                        raise RuntimeError(msg)
                    url = base_url.rstrip("/") + insert_path
                    headers = bearer_headers(st.session_state["token"])
                    resp = requests.post(url, headers=headers, data=body, timeout=60)
                    try:
                        resp_body = json.dumps(resp.json(), indent=2)
                    except Exception:
                        resp_body = resp.text
                    results.append((ext_id, resp.status_code, None, resp_body))
                except Exception as e:
                    results.append((ext_id, None, f"POST error: {e}", None))

            # Show results
            st.subheader("Results")
            for ext_id, status, err, body in results:
                with st.container(border=True):
                    st.markdown(f"**externalId:** `{ext_id}`")
                    if status is not None and status != 0:
                        st.markdown(f"**HTTP Status:** `{status}`")
                    if err:
                        st.error(err)
                    if body:
                        st.code(body, language="json")
