# pages/suppliers.py
import json
import time
from typing import Dict, List

import pandas as pd
import requests
import streamlit as st

from helpers import (
    load_table,
    bearer_headers,
    ensure_token,
    SUPPLIER_INSERT_PATH,
)

# -----------------------------
# Column resolution helpers
# -----------------------------
def pick(row: Dict, *candidates):
    """Return the first non-empty column from the given row by trying candidate names."""
    for c in candidates:
        if c in row and str(row[c]).strip() != "":
            return str(row[c]).strip()
    return ""

def truthy(val: str) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in ("x", "1", "true", "yes", "y", "ja")

# -----------------------------
# Builder
# -----------------------------
def build_supplier_payloads(lfa1: pd.DataFrame,
                            lfb1: pd.DataFrame | None,
                            lfbk: pd.DataFrame | None,
                            tiban: pd.DataFrame | None) -> List[Dict]:
    """
    Build one supplier payload per vendor (Kreditor/LIFNR).
    Supports both German UI column names and SAP technical names.
    """
    # Normalize columns for easy access (case-sensitive preservation, but compare via a map)
    def cols_map(df: pd.DataFrame) -> Dict[str, str]:
        # map lowercase->actual for flexible lookup
        return {c.lower(): c for c in df.columns}

    lfa1_cols = cols_map(lfa1)
    lfb1_cols = cols_map(lfb1) if lfb1 is not None else {}
    lfbk_cols = cols_map(lfbk) if lfbk is not None else {}
    tiban_cols = cols_map(tiban) if tiban is not None else {}

    # Create indexes/dicts keyed by vendor id
    def vendor_key_from_row(row: Dict) -> str:
        return pick(row,
                    lfa1_cols.get("kreditor", "Kreditor"),
                    lfa1_cols.get("lifnr", "LIFNR"),
                    "Kreditor", "LIFNR")

    # Build subsidiaries map from LFB1
    subs_map: Dict[str, List[Dict]] = {}
    if lfb1 is not None and len(lfb1):
        for _, r in lfb1.iterrows():
            row = r.to_dict()

            lifnr = pick(row,
                         lfb1_cols.get("kreditor", "Kreditor"),
                         lfb1_cols.get("lifnr", "LIFNR"),
                         "Kreditor", "LIFNR")
            if not lifnr:
                continue

            bukr = pick(row, lfb1_cols.get("bukr", "BuKr"), lfb1_cols.get("bukrs", "BUKRS"), "BuKr", "BUKRS")
            zbed = pick(row, lfb1_cols.get("zbed", "Zbed"), lfb1_cols.get("zterm", "ZTERM"), "Zbed", "ZTERM")
            block = pick(row, lfb1_cols.get("s", "S"), lfb1_cols.get("sperr", "SPERR"), "S", "SPERR")
            subsidiary = {
                "externalCompanyId": bukr or None,
                "paymentTerms": {"paymentTermKey": zbed} if zbed else None,
                "blockedForPayment": truthy(block),
            }
            # prune None fields lightly
            if subsidiary["paymentTerms"] is None:
                subsidiary.pop("paymentTerms")
            if not lifnr in subs_map:
                subs_map[lifnr] = []
            subs_map[lifnr].append(subsidiary)

    # Build bank accounts map from LFBK + TIBAN (left join)
    iban_index = {}
    if tiban is not None and len(tiban):
        for _, r in tiban.iterrows():
            row = r.to_dict()
            land = pick(row, tiban_cols.get("land", "Land"), tiban_cols.get("banks", "BANKS"), "Land", "BANKS")
            bank_key = pick(row, tiban_cols.get("bankschlüssel", "Bankschlüssel"), tiban_cols.get("bankl", "BANKL"),
                            "Bankschlüssel", "BANKL")
            acct = pick(row, tiban_cols.get("bankkonto", "Bankkonto"), tiban_cols.get("bankn", "BANKN"),
                        "Bankkonto", "BANKN")
            iban = pick(row, tiban_cols.get("iban", "IBAN"), "IBAN")
            if land and bank_key and acct and iban:
                iban_index[(land, bank_key, acct)] = iban

    bank_map: Dict[str, List[Dict]] = {}
    if lfbk is not None and len(lfbk):
        for _, r in lfbk.iterrows():
            row = r.to_dict()
            lifnr = pick(row,
                         lfbk_cols.get("kreditor", "Kreditor"),
                         lfbk_cols.get("lifnr", "LIFNR"),
                         "Kreditor", "LIFNR")
            if not lifnr:
                continue

            land = pick(row, lfbk_cols.get("land", "Land"), lfbk_cols.get("banks", "BANKS"), "Land", "BANKS")
            bank_key = pick(row, lfbk_cols.get("bankschlüssel", "Bankschlüssel"), lfbk_cols.get("bankl", "BANKL"),
                            "Bankschlüssel", "BANKL")
            acct = pick(row, lfbk_cols.get("bankkonto", "Bankkonto"), lfbk_cols.get("bankn", "BANKN"),
                        "Bankkonto", "BANKN")

            external_id = "".join([land, bank_key, acct]) if (land and bank_key and acct) else None
            iban = iban_index.get((land, bank_key, acct)) if (land and bank_key and acct) else None

            entry = {
                "externalId": external_id,
                "bankAccountNumber": acct or None,
                "iban": iban or None,
            }
            # prune Nones
            entry = {k: v for k, v in entry.items() if v is not None}
            if not lifnr in bank_map:
                bank_map[lifnr] = []
            bank_map[lifnr].append(entry)

    # Build final supplier payloads from LFA1
    payloads: List[Dict] = []
    for _, r in lfa1.iterrows():
        row = r.to_dict()

        lifnr = pick(row, lfa1_cols.get("kreditor", "Kreditor"), lfa1_cols.get("lifnr", "LIFNR"), "Kreditor", "LIFNR")
        if not lifnr:
            # vendor id is required
            continue

        company_name = pick(row, lfa1_cols.get("name 1", "Name 1"), lfa1_cols.get("name1", "NAME1"), "Name 1", "NAME1")
        address = pick(row, lfa1_cols.get("straße", "Straße"), lfa1_cols.get("stras", "STRAS"), "Straße", "STRAS")
        city = pick(row, lfa1_cols.get("ort", "Ort"), lfa1_cols.get("ort01", "ORT01"), "Ort", "ORT01")
        postcode = pick(row, lfa1_cols.get("postleitz.", "Postleitz."), lfa1_cols.get("pstlz", "PSTLZ"),
                        "Postleitz.", "PSTLZ")
        country = pick(row, lfa1_cols.get("lnd", "Lnd"), lfa1_cols.get("land1", "LAND1"), "Lnd", "LAND1")

        tax_no = pick(row, lfa1_cols.get("steuernummer 1", "Steuernummer 1"), lfa1_cols.get("stcd1", "STCD1"),
                      "Steuernummer 1", "STCD1")
        vat_id = pick(row, lfa1_cols.get("umsatzsteuer-id.nr", "Umsatzsteuer-Id.Nr"),
                      lfa1_cols.get("stceg", "STCEG"), "Umsatzsteuer-Id.Nr", "STCEG")

        payload = {
            "vendorId": lifnr,
            "companyName": company_name or None,
            "address": address or None,
            "city": city or None,
            "postcode": postcode or None,
            "country": country or None,
            "taxIds": [tax_no] if tax_no else [],
            "vatIds": [vat_id] if vat_id else [],
            "supplierSubsidiaries": subs_map.get(lifnr, []),
            "supplierBankAccounts": bank_map.get(lifnr, []),
        }
        # prune None/empty
        payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
        payloads.append(payload)

    return payloads

# -----------------------------
# Page UI
# -----------------------------
def render_suppliers_page():
    st.caption("Upload SAP vendor master exports (LFA1, LFB1, LFBK, TIBAN) → map → POST to insert suppliers")

    # Shared config from session
    base_url = st.session_state.get("base_url", "")
    auth_path = st.session_state.get("auth_path", "")
    client_id = st.session_state.get("client_id", "")
    client_secret = st.session_state.get("client_secret", "")

    st.markdown("#### Upload files")
    c1, c2 = st.columns(2)
    with c1:
        lfa1_file = st.file_uploader("LFA1 (required)", type=["xlsx", "xls", "csv"], key="lfa1")
        lfb1_file = st.file_uploader("LFB1 (subsidiaries)", type=["xlsx", "xls", "csv"], key="lfb1")
    with c2:
        lfbk_file = st.file_uploader("LFBK (bank accounts)", type=["xlsx", "xls", "csv"], key="lfbk")
        tiban_file = st.file_uploader("TIBAN (IBAN registry)", type=["xlsx", "xls", "csv"], key="tiban")

    dry_run = st.toggle("Dry run (do not POST, just preview JSON)", value=True)
    throttle_ms = st.slider("Throttle between requests (ms)", 0, 2000, 0, step=50)

    if not lfa1_file:
        st.info("LFA1 is required to build supplier headers.")
        return

    # Load dataframes (read as strings; preserves leading zeros)
    lfa1 = load_table(lfa1_file)
    lfb1 = load_table(lfb1_file) if lfb1_file else None
    lfbk = load_table(lfbk_file) if lfbk_file else None
    tiban = load_table(tiban_file) if tiban_file else None

    st.markdown("#### Previews")
    st.write("LFA1:", lfa1.head(10))
    if lfb1 is not None: st.write("LFB1:", lfb1.head(10))
    if lfbk is not None: st.write("LFBK:", lfbk.head(10))
    if tiban is not None: st.write("TIBAN:", tiban.head(10))

    # Build payloads
    payloads = build_supplier_payloads(lfa1, lfb1, lfbk, tiban)

    st.markdown("#### Payload preview")
    preview_n = min(5, len(payloads))
    st.caption(f"Showing first {preview_n} of {len(payloads)} supplier payload(s).")
    if preview_n:
        st.code(json.dumps(payloads[:preview_n], indent=2, ensure_ascii=False), language="json")
    else:
        st.warning("No supplier payloads could be built from LFA1.")

    endpoint = f"{base_url.rstrip('/')}{SUPPLIER_INSERT_PATH}"
    st.write("Target endpoint:", endpoint)

    if st.button(f"Send {len(payloads)} supplier(s)"):
        if dry_run:
            st.info("Dry run enabled — not sending requests.")
            return

        ok, msg = ensure_token(base_url, client_id, client_secret, auth_path)
        if not ok:
            st.error(msg)
            return

        headers = bearer_headers(st.session_state["token"])
        ok_count = ko_count = 0
        results = []
        progress = st.progress(0, text="Uploading suppliers...")

        for idx, payload in enumerate(payloads, start=1):
            try:
                resp = requests.post(endpoint, headers=headers, json=payload, timeout=60)
                if resp.status_code < 300:
                    ok_count += 1
                    results.append({"row": idx, "status": "OK", "http": resp.status_code})
                else:
                    ko_count += 1
                    results.append({"row": idx, "status": "ERROR", "http": resp.status_code, "body": resp.text[:2000]})
            except Exception as e:
                ko_count += 1
                results.append({"row": idx, "status": "ERROR", "http": "-", "body": str(e)})

            if throttle_ms:
                time.sleep(throttle_ms / 1000.0)

            progress.progress(int(idx * 100 / len(payloads)), text=f"Uploaded {idx}/{len(payloads)}")

        st.success(f"Finished. OK: {ok_count}, Errors: {ko_count}")
        st.dataframe(pd.DataFrame(results), use_container_width=True)
