# pages/companies.py
import json
import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

from helpers import (
    load_table,
    bearer_headers,
    ensure_token,
    COMPANY_INSERT_PATH,
    make_company_samples_technical,
)

# -----------------------------
# Small helpers (reuse style from suppliers)
# -----------------------------
def pick(row: Dict, *candidates) -> str:
    for c in candidates:
        if c in row:
            v = row[c]
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return ""

def prune_empty(obj):
    if isinstance(obj, dict):
        return {k: prune_empty(v) for k, v in obj.items() if v not in (None, "", [], {}, "nan", "NaN")}
    if isinstance(obj, list):
        return [prune_empty(x) for x in obj if x not in (None, "", [], {}, "nan", "NaN")]
    return obj

# ADRC map (optional)
def make_adrc_map(adrc: Optional[pd.DataFrame]) -> Dict[str, Dict]:
    if adrc is None or adrc.empty:
        return {}
    col = {c.lower(): c for c in adrc.columns}
    k_addr = col.get("addrnumber", "ADDRNUMBER")
    out: Dict[str, Dict] = {}
    for _, r in adrc.iterrows():
        rd = r.to_dict()
        addrnum = pick(rd, k_addr)
        if not addrnum:
            continue
        out[addrnum] = {
            "NAME1": pick(rd, col.get("name1","NAME1")),
            "NAME2": pick(rd, col.get("name2","NAME2")),
            "NAME3": pick(rd, col.get("name3","NAME3")),
            "NAME4": pick(rd, col.get("name4","NAME4")),
            "STREET": pick(rd, col.get("street","STREET")),
            "CITY1": pick(rd, col.get("city1","CITY1")),
            "POST_CODE1": pick(rd, col.get("post_code1","POST_CODE1")),
            "COUNTRY": pick(rd, col.get("country","COUNTRY")),
        }
    return out

# Collect multiple IDs (VAT / TAX) from T001
_SPLIT_RE = re.compile(r"[,\;\|\s]+")

def _split_multi(val: str) -> list[str]:
    if not val:
        return []
    return [p.strip() for p in _SPLIT_RE.split(str(val)) if p and p.strip()]

def _collect_ids_from_row(row: dict, colmap: dict[str, str], patterns: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for lower_name, actual in colmap.items():
        if any(re.fullmatch(pat, lower_name) for pat in patterns):
            for v in _split_multi(row.get(actual, "")):
                if v not in seen:
                    seen.add(v)
                    out.append(v)
    return out

# -----------------------------
# Builder: one payload per company
# -----------------------------
def build_company_payloads(
    t001: pd.DataFrame,
    adrc: Optional[pd.DataFrame],
    alt_name_source: str = "T001_FIRST",
    external_client_id: Optional[str] = None,
) -> List[Dict]:
    """
    Build company payloads for Insert Company endpoint.
    externalId <- BUKRS, companyName <- BUTXT.
    Address from T001 (basic) and optionally ADRC via T001-ADRNR.
    Multiple VAT/TAX IDs supported.
    """
    def cols_map(df: Optional[pd.DataFrame]) -> Dict[str, str]:
        return {} if df is None else {c.lower(): c for c in df.columns}

    c_t001 = cols_map(t001)
    adrc_map = make_adrc_map(adrc)

    # patterns for multi IDs in T001 (if present / custom)
    vat_patterns = [r"stceg(_?\d+)?", r"vat[_\-]?id", r"vat[_\-]?number", r"vatno"]
    tax_patterns = [r"stcd(_?\d+)?", r"tax[_\-]?id", r"tax[_\-]?number", r"taxno"]

    payloads: List[Dict] = []
    for _, r in t001.iterrows():
        rd = r.to_dict()
        bukrs = pick(rd, c_t001.get("bukrs","BUKRS"))
        if not bukrs:
            continue

        name  = pick(rd, c_t001.get("butxt","BUTXT"))
        city  = pick(rd, c_t001.get("ort01","ORT01"))
        ctry  = pick(rd, c_t001.get("land1","LAND1"))
        adrnr = pick(rd, c_t001.get("adrnr","ADRNR"))
        # optional in T001 exports
        post  = pick(rd, c_t001.get("pstlz","PSTLZ"))
        street= pick(rd, c_t001.get("stras","STRAS"))

        # collect IDs
        vat_ids = _collect_ids_from_row(rd, c_t001, vat_patterns)
        tax_ids = _collect_ids_from_row(rd, c_t001, tax_patterns)

        a = adrc_map.get(adrnr) if adrnr else None

        # alternative names (if you want to expose in payload)
        if a and alt_name_source == "ADRC_FIRST":
            alt1 = a.get("NAME2")
            alt2 = a.get("NAME3")
            alt3 = a.get("NAME4")
        else:
            alt1 = None
            alt2 = None
            alt3 = None
            if a and not name:
                name = a.get("NAME1") or name  # do not override if T001 has a name

        # address fallback from ADRC if T001 basic fields are not present
        if a:
            street = street or a.get("STREET")
            city   = city   or a.get("CITY1")
            post   = post   or a.get("POST_CODE1")
            ctry   = ctry   or a.get("COUNTRY")

        payload = {
            "externalId": bukrs,
            "externalClientId": (external_client_id or None),
            "companyName": name or None,
            "nameAlternative1": alt1 or None,
            "nameAlternative2": alt2 or None,
            "nameAlternative3": alt3 or None,
            "address": street or None,
            "city": city or None,
            "postcode": post or None,
            "country": ctry or None,
            # IMPORTANT: API expects arrays of OBJECTS, not strings
            "taxIds": [{"taxId": t} for t in vat_ids and [] or []],   # placeholder, will fix below
            "vatIds": [{"vatId": v} for v in vat_ids] if vat_ids else [],
        }

        # fix taxIds mapping (typo guard)
        tax_objs = [{"taxId": t} for t in tax_ids] if tax_ids else []
        payload["taxIds"] = tax_objs

        payloads.append(prune_empty(payload))

    return payloads

# -----------------------------
# Page UI
# -----------------------------
def render_companies_page():
    st.caption("Upload SAP company master (T001) → map → POST to Insert Company API. Optional ADRC join for address/names.")
    # SAP tables shown for guidance
    with st.expander("ℹ️ SAP sources we expect", expanded=False):
        st.markdown(
            "- **T001** (Company Code): `BUKRS` (ID), `BUTXT` (name), `LAND1` (country), `ORT01` (city), `WAERS` (currency), `ADRNR` (address ref), optional `STCD1..` and `STCEG`.\n"
            "- **ADRC** (optional): `ADDRNUMBER` join via `T001-ADRNR`, address fields `STREET`, `CITY1`, `POST_CODE1`, `COUNTRY`, names `NAME1..4`."
        )

    # Shared config
    base_url = st.session_state.get("base_url", "")
    auth_path = st.session_state.get("auth_path", "")
    client_id = st.session_state.get("client_id", "")
    client_secret = st.session_state.get("client_secret", "")

    # Samples
    with st.expander("📥 Download company sample files", expanded=False):
        samples = make_company_samples_technical()
        st.download_button(
            "Download all (ZIP)",
            data=samples["company_technical_samples.zip"],
            file_name="company_technical_samples.zip",
            mime="application/zip",
            use_container_width=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "T001 (XLSX)",
                data=samples["T001_technical_sample.xlsx"],
                file_name="T001_technical_sample.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "ADRC (XLSX)",
                data=samples["ADRC_technical_sample.xlsx"],
                file_name="ADRC_technical_sample.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    st.markdown("#### Upload files")
    c1, c2 = st.columns(2)
    with c1:
        t001_file = st.file_uploader("T001 (required)", type=["xlsx","xls","csv"], key="t001")
    with c2:
        adrc_file = st.file_uploader("ADRC (optional)", type=["xlsx","xls","csv"], key="adrc_company")

    st.markdown("#### Options")
    alt_source = st.radio(
        "Alternative name source priority",
        options=["T001 first (fallback ADRC)", "ADRC first (fallback T001)"],
        index=0,
    )
    alt_source_key = "T001_FIRST" if alt_source.startswith("T001") else "ADRC_FIRST"

    st.markdown("#### External Client ID")
    external_client_id = st.text_input(
        "Global externalClientId (optional)",
        placeholder="EXTERNAL_CLIENT_ID",
    )

    dry_run = st.toggle("Dry run (do not POST, just preview JSON)", value=True)
    throttle_ms = st.slider("Throttle between requests (ms)", 0, 2000, 0, step=50)

    if not t001_file:
        st.info("T001 is required.")
        return

    # Load tables
    t001 = load_table(t001_file)
    adrc = load_table(adrc_file) if adrc_file else None

    st.markdown("#### Previews")
    st.write("T001:", t001.head(10))
    if adrc is not None: st.write("ADRC:", adrc.head(10))

    # Build payloads
    payloads = build_company_payloads(
        t001=t001,
        adrc=adrc,
        alt_name_source=alt_source_key,
        external_client_id=external_client_id or None,
    )

    st.markdown("#### Payload preview")
    preview_n = min(5, len(payloads))
    st.caption(f"Showing first {preview_n} of {len(payloads)} company payload(s).")
    if preview_n:
        st.code(json.dumps(payloads[:preview_n], indent=2, ensure_ascii=False), language="json")
    else:
        st.warning("No company payloads could be built from T001.")

    endpoint = f"{base_url.rstrip('/')}{COMPANY_INSERT_PATH}"
    st.write("Target endpoint:", endpoint)

    if st.button(f"Send {len(payloads)} compan{'y' if len(payloads)==1 else 'ies'}"):
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
        progress = st.progress(0, text="Uploading companies...")

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
