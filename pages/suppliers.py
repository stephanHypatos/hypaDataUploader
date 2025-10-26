# pages/suppliers.py
import json
import time
from typing import Dict, List, Tuple, Optional

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
def pick(row: Dict, *candidates) -> str:
    """Return the first non-empty value found in row for any of the candidate column names."""
    for c in candidates:
        if c in row:
            v = row[c]
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return ""

def truthy(val: str) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in ("x", "1", "true", "yes", "y", "ja")

def prune_empty(obj):
    if isinstance(obj, dict):
        return {k: prune_empty(v) for k, v in obj.items() if v not in (None, "", [], {}, "nan", "NaN")}
    if isinstance(obj, list):
        return [prune_empty(x) for x in obj if x not in (None, "", [], {}, "nan", "NaN")]
    return obj

# -----------------------------
# ADRC extraction
# -----------------------------
def make_adrc_map(adrc: Optional[pd.DataFrame]) -> Dict[str, Dict]:
    """
    Build a dict keyed by ADRC-ADDRNUMBER with:
      - name2..name4 (uppercased like SAP technical)
      - address fields as possible fallbacks
    """
    if adrc is None or adrc.empty:
        return {}

    # Build a case-insensitive column map
    col = {c.lower(): c for c in adrc.columns}

    k_addr = col.get("addrnumber", "ADDRNUMBER")
    k_name2 = col.get("name2", "NAME2")
    k_name3 = col.get("name3", "NAME3")
    k_name4 = col.get("name4", "NAME4")
    k_city  = col.get("city1", "CITY1")
    k_post  = col.get("post_code1", "POST_CODE1")
    k_street= col.get("street", "STREET")
    k_ctry  = col.get("country", "COUNTRY")
    k_name1 = col.get("name1", "NAME1")  # sometimes name1 in ADRC is also populated

    out: Dict[str, Dict] = {}
    for _, r in adrc.iterrows():
        rd = r.to_dict()
        addrnum = pick(rd, k_addr)
        if not addrnum:
            continue
        out[addrnum] = {
            "NAME1": pick(rd, k_name1),
            "NAME2": pick(rd, k_name2),
            "NAME3": pick(rd, k_name3),
            "NAME4": pick(rd, k_name4),
            "STREET": pick(rd, k_street),
            "CITY1": pick(rd, k_city),
            "POST_CODE1": pick(rd, k_post),
            "COUNTRY": pick(rd, k_ctry),
        }
    return out

# -----------------------------
# Core builder
# -----------------------------
def build_supplier_payloads(
    lfa1: pd.DataFrame,
    lfb1: Optional[pd.DataFrame],
    lfbk: Optional[pd.DataFrame],
    tiban: Optional[pd.DataFrame],
    adrc: Optional[pd.DataFrame],
    alt_name_source: str = "LFA1_FIRST",  # "LFA1_FIRST" or "ADRC_FIRST"
) -> List[Dict]:
    """
    Build one supplier payload per vendor (LIFNR). Accepts SAP technical columns.
    alt_name_source: choose whether alternative names come primarily from LFA1 or ADRC.
    """
    # column maps (case-insensitive)
    def cols_map(df: Optional[pd.DataFrame]) -> Dict[str, str]:
        return {} if df is None else {c.lower(): c for c in df.columns}

    c_lfa1 = cols_map(lfa1)
    c_lfb1 = cols_map(lfb1)
    c_lfbk = cols_map(lfbk)
    c_tiban = cols_map(tiban)

    # --- subsidiaries from LFB1 ---
    subs_map: Dict[str, List[Dict]] = {}
    if lfb1 is not None and not lfb1.empty:
        for _, r in lfb1.iterrows():
            rd = r.to_dict()
            lifnr = pick(rd, c_lfb1.get("lifnr", "LIFNR"))
            if not lifnr:
                continue
            bukrs = pick(rd, c_lfb1.get("bukrs", "BUKRS"))
            zterm = pick(rd, c_lfb1.get("zterm", "ZTERM"))
            sperr = pick(rd, c_lfb1.get("sperr", "SPERR"))
            item = {
                "externalCompanyId": bukrs or None,
                "blockedForPayment": truthy(sperr),
            }
            if zterm:
                item["paymentTerms"] = {"paymentTermKey": zterm}
            item = prune_empty(item)
            subs_map.setdefault(lifnr, []).append(item)

    # --- TIBAN lookup (BANKS,BANKL,BANKN -> IBAN) ---
    iban_index: Dict[Tuple[str, str, str], str] = {}
    if tiban is not None and not tiban.empty:
        for _, r in tiban.iterrows():
            rd = r.to_dict()
            banks = pick(rd, c_tiban.get("banks", "BANKS"))
            bankl = pick(rd, c_tiban.get("bankl", "BANKL"))
            bankn = pick(rd, c_tiban.get("bankn", "BANKN"))
            iban  = pick(rd, c_tiban.get("iban",  "IBAN"))
            if banks and bankl and bankn and iban:
                iban_index[(banks, bankl, bankn)] = iban

    # --- bank accounts from LFBK (+ TIBAN join) ---
    bank_map: Dict[str, List[Dict]] = {}
    if lfbk is not None and not lfbk.empty:
        for _, r in lfbk.iterrows():
            rd = r.to_dict()
            lifnr = pick(rd, c_lfbk.get("lifnr", "LIFNR"))
            if not lifnr:
                continue
            banks = pick(rd, c_lfbk.get("banks", "BANKS"))
            bankl = pick(rd, c_lfbk.get("bankl", "BANKL"))
            bankn = pick(rd, c_lfbk.get("bankn", "BANKN"))
            external_id = f"{banks}{bankl}{bankn}" if banks and bankl and bankn else None
            iban = iban_index.get((banks, bankl, bankn))
            entry = {
                "externalId": external_id,
                "bankAccountNumber": bankn or None,
                "iban": iban or None,
            }
            entry = prune_empty(entry)
            bank_map.setdefault(lifnr, []).append(entry)

    # --- ADRC map (optional) ---
    adrc_map = make_adrc_map(adrc)

    # --- final suppliers from LFA1 ---
    payloads: List[Dict] = []
    for _, r in lfa1.iterrows():
        rd = r.to_dict()
        lifnr   = pick(rd, c_lfa1.get("lifnr", "LIFNR"))
        if not lifnr:
            continue

        # Base names/addr from LFA1
        name1  = pick(rd, c_lfa1.get("name1", "NAME1"))
        name2  = pick(rd, c_lfa1.get("name2", "NAME2"))
        name3  = pick(rd, c_lfa1.get("name3", "NAME3"))
        name4  = pick(rd, c_lfa1.get("name4", "NAME4"))
        stras  = pick(rd, c_lfa1.get("stras", "STRAS"))
        city   = pick(rd, c_lfa1.get("ort01", "ORT01"))
        post   = pick(rd, c_lfa1.get("pstlz", "PSTLZ"))
        ctry   = pick(rd, c_lfa1.get("land1", "LAND1"))
        stcd1  = pick(rd, c_lfa1.get("stcd1", "STCD1"))
        stceg  = pick(rd, c_lfa1.get("stceg", "STCEG"))
        adrnr  = pick(rd, c_lfa1.get("adrnr", "ADRNR"))

        # ADRC fallbacks / alternates
        a = adrc_map.get(adrnr) if adrnr else None
        if alt_name_source == "ADRC_FIRST" and a:
            alt1 = a.get("NAME2") or name2
            alt2 = a.get("NAME3") or name3
            alt3 = a.get("NAME4") or name4
        else:
            # LFA1 first, fallback ADRC
            alt1 = name2 or (a.get("NAME2") if a else "")
            alt2 = name3 or (a.get("NAME3") if a else "")
            alt3 = name4 or (a.get("NAME4") if a else "")

        # Address fallback from ADRC if LFA1 is empty
        if a:
            stras = stras or a.get("STREET")
            city  = city  or a.get("CITY1")
            post  = post  or a.get("POST_CODE1")
            ctry  = ctry  or a.get("COUNTRY")
            # also sometimes ADRC.NAME1 is "nicer" – do not override LFA1.NAME1 unless empty
            name1 = name1 or a.get("NAME1")

        payload = {
            "vendorId": lifnr,
            "companyName": name1 or None,
            "nameAlternative1": alt1 or None,
            "nameAlternative2": alt2 or None,
            "nameAlternative3": alt3 or None,
            # you can add nameAlternative4 from another source if needed
            "address": stras or None,
            "city": city or None,
            "postcode": post or None,
            "country": ctry or None,
            "taxIds": [stcd1] if stcd1 else [],
            "vatIds": [stceg] if stceg else [],
            "supplierSubsidiaries": subs_map.get(lifnr, []),
            "supplierBankAccounts": bank_map.get(lifnr, []),
        }

        payloads.append(prune_empty(payload))

    return payloads

# -----------------------------
# Page UI
# -----------------------------
def render_suppliers_page():
    st.caption("Upload SAP vendor master exports (LFA1, LFB1, LFBK, TIBAN, ADRC) → map → POST to insert suppliers")

    # Shared config from session
    base_url = st.session_state.get("base_url", "")
    auth_path = st.session_state.get("auth_path", "")
    client_id = st.session_state.get("client_id", "")
    client_secret = st.session_state.get("client_secret", "")

    st.markdown("#### Upload files")
    c1, c2, c3 = st.columns(3)
    with c1:
        lfa1_file = st.file_uploader("LFA1 (required)", type=["xlsx", "xls", "csv"], key="lfa1")
        lfb1_file = st.file_uploader("LFB1 (subsidiaries)", type=["xlsx", "xls", "csv"], key="lfb1")
    with c2:
        lfbk_file = st.file_uploader("LFBK (bank accounts)", type=["xlsx", "xls", "csv"], key="lfbk")
        tiban_file = st.file_uploader("TIBAN (IBAN registry)", type=["xlsx", "xls", "csv"], key="tiban")
    with c3:
        adrc_file = st.file_uploader("ADRC (address/alt names)", type=["xlsx", "xls", "csv"], key="adrc")

    st.markdown("#### Options")
    alt_source = st.radio(
        "Alternative name source priority",
        options=["LFA1 first (fallback ADRC)", "ADRC first (fallback LFA1)"],
        index=0,
        help="Controls which table provides NAME2/NAME3/NAME4 first for nameAlternative1..3.",
    )
    alt_source_key = "LFA1_FIRST" if alt_source.startswith("LFA1") else "ADRC_FIRST"

    dry_run = st.toggle("Dry run (do not POST, just preview JSON)", value=True)
    throttle_ms = st.slider("Throttle between requests (ms)", 0, 2000, 0, step=50)

    if not lfa1_file:
        st.info("LFA1 is required to build supplier headers.")
        return

    # Load tables (read as strings; preserves leading zeros)
    lfa1 = load_table(lfa1_file)
    lfb1 = load_table(lfb1_file) if lfb1_file else None
    lfbk = load_table(lfbk_file) if lfbk_file else None
    tiban = load_table(tiban_file) if tiban_file else None
    adrc  = load_table(adrc_file)  if adrc_file  else None

    st.markdown("#### Previews")
    st.write("LFA1:", lfa1.head(10))
    if lfb1 is not None: st.write("LFB1:", lfb1.head(10))
    if lfbk is not None: st.write("LFBK:", lfbk.head(10))
    if tiban is not None: st.write("TIBAN:", tiban.head(10))
    if adrc  is not None: st.write("ADRC:", adrc.head(10))

    # Build payloads
    payloads = build_supplier_payloads(lfa1, lfb1, lfbk, tiban, adrc, alt_name_source=alt_source_key)

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
