import time
import pandas as pd
import requests
import streamlit as st

from helpers import (
    load_table,
    normalize_snake,
    bearer_headers,
    ensure_token,
)

def _extract_external_ids(df: pd.DataFrame) -> list[str]:
    # Accept common column names
    candidates = [c for c in df.columns]
    lower_map = {c.lower(): c for c in candidates}

    col = None
    for want in ["externalid", "externalids", "external_id", "external_ids", "id", "ids"]:
        if want in lower_map:
            col = lower_map[want]
            break

    if col is None:
        return []

    ids = (
        df[col]
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "NULL": "", "null": ""})
        .tolist()
    )
    # drop empties + dedupe while preserving order
    seen = set()
    out = []
    for x in ids:
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def render_delete_records_page():
    st.title("Delete records (by externalId)")
    st.caption("Upload an Excel/CSV with externalId(s), preview, then confirm before sending DELETE requests.")

    # --- Target configuration ---
    st.subheader("Target endpoint")

    # You can adapt defaults to your real delete endpoint:
    # Example template patterns (choose one that matches your API):
    # 1) /v2/enrichment/lookup-tables/{type}/{externalId}
    # 2) /v2/enrichment/lookup-tables/{type}?externalId={externalId}
    # 3) /v2/enrichment/invoices/{externalId}
    endpoint_template = st.text_input(
        "DELETE path template (relative to Base URL)",
        value="/v2/enrichment/lookup-tables/{type}/{externalId}",
        help="Use placeholders: {type} and {externalId}. If you don't need type, remove it.",
    )

    type_mode = "{type}" in endpoint_template
    table_type = None
    
    if type_mode:
        st.subheader("Lookup table type")

        TYPE_OPTIONS = {
            "Payment terms": "payment_terms",
            "Tax codes": "tax_codes",
            "Custom (free input)": "__custom__",
        }
    
        selected_label = st.selectbox(
            "Select type",
            options=list(TYPE_OPTIONS.keys()),
            index=0,
            help="Choose a known lookup table type or enter a custom one",
        )
    
        selected_value = TYPE_OPTIONS[selected_label]
    
        if selected_value == "__custom__":
            table_type = st.text_input(
                "Custom type",
                placeholder="e.g. withholding_tax, gl_accounts, cost_centers",
            )
        else:
            table_type = selected_value
    
        # final normalization (safe even if helpers.normalize_snake is missing)
        table_type = normalize_snake(table_type)
    
        if not table_type:
            st.error("Table type must not be empty.")
            st.stop()
    else:
        table_type = ""  # so .format(type=...) won't break if someone removed {type}

    throttle_ms = st.number_input("Throttle (ms) between DELETE calls", min_value=0, value=0, step=50)
    dry_run = st.checkbox("Dry run (do not call DELETE)", value=False)

    st.divider()

    # --- File upload ---
    st.subheader("Upload IDs")
    file = st.file_uploader("Excel or CSV", type=["xlsx", "xls", "csv"])

    if not file:
        st.info("Upload a file to continue.")
        return

    df = load_table(file)  # your helper already reads as strings & preserves leading zeros (per README)
    st.write("Preview:")
    st.dataframe(df.head(25), use_container_width=True)

    external_ids = _extract_external_ids(df)
    if not external_ids:
        st.error("Could not find an externalId(s) column. Add a column named externalId (recommended).")
        return

    st.success(f"Found **{len(external_ids)}** unique externalId(s).")
    st.code("\n".join(external_ids[:20]) + ("\n..." if len(external_ids) > 20 else ""))

    st.divider()

    # --- Safety confirmation ---
    st.subheader("Confirm deletion")
    st.warning("This action is destructive. Make sure your endpoint template is correct.")

    confirm_checkbox = st.checkbox("I understand this will DELETE records", value=False)
    confirm_text = st.text_input('Type "DELETE" to confirm', value="")

    can_run = confirm_checkbox and (confirm_text.strip().upper() == "DELETE")

    # --- Execute ---
    if st.button(f"Delete {len(external_ids)} record(s)", disabled=not can_run):
        if dry_run:
            st.info("Dry run enabled — no DELETE calls sent.")
            return

        ensure_token()  # your existing auth refresh
        headers = bearer_headers()

        results = []
        base_url = st.session_state.get("base_url", "").rstrip("/")

        for i, ext_id in enumerate(external_ids, start=1):
            path = endpoint_template.format(type=table_type or "", externalId=ext_id)
            url = f"{base_url}{path}"

            try:
                r = requests.delete(url, headers=headers, timeout=60)
                ok = 200 <= r.status_code < 300
                results.append({
                    "externalId": ext_id,
                    "url": url,
                    "status": r.status_code,
                    "ok": ok,
                    "response": (r.text or "")[:5000],
                })
            except Exception as e:
                results.append({
                    "externalId": ext_id,
                    "url": url,
                    "status": None,
                    "ok": False,
                    "response": str(e),
                })

            if throttle_ms and i < len(external_ids):
                time.sleep(throttle_ms / 1000.0)

        out_df = pd.DataFrame(results)
        st.subheader("Results")
        st.dataframe(out_df, use_container_width=True)

        failed = out_df[~out_df["ok"]]
        if len(failed):
            st.error(f"{len(failed)} deletion(s) failed.")
        else:
            st.success("All deletions returned 2xx.")
