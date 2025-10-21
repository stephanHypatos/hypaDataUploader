# pages/lookup_tables.py
import json
import time

import pandas as pd
import requests
import streamlit as st

from helpers import (
    load_table,
    build_payloads,
    slugify_type,
    bearer_headers,
    ensure_token,
)


def render_lookup_tables_page():
    st.subheader("Insert Lookup Table Rows")

    # Shared config from session
    base_url = st.session_state.get("base_url", "")
    auth_path = st.session_state.get("auth_path", "")
    client_id = st.session_state.get("client_id", "")
    client_secret = st.session_state.get("client_secret", "")

    # Type select with custom
    type_choice = st.selectbox(
        "Lookup table type (path param `{type}`)",
        options=["tax_codes", "central_bank_indicator", "payment_terms", "custom"],
        index=0,
        help="Choose a predefined type or 'custom' to provide your own. Naming convention: table_name (i.e. bank_numbers)",
    )

    custom_type = ""
    if type_choice == "custom":
        raw_custom = st.text_input(
            "Custom lookup table type",
            placeholder="my_table",
            help="Allowed: letters, numbers, underscore. Spaces will become underscores.",
        )
        custom_type = slugify_type(raw_custom)
        if raw_custom and not custom_type:
            st.error("Invalid custom type. Use only letters, numbers, or underscore.")
    lookup_type = custom_type if type_choice == "custom" else type_choice

    uploaded = st.file_uploader("Excel/CSV with rows to insert", type=["xlsx", "xls", "csv"])

    col_req, col_opt = st.columns(2)
    with col_req:
        st.caption("**Recommended columns**")
        st.code("externalId\nkey\ndescription", language="text")
    with col_opt:
        st.caption("**Optional columns**")
        st.text("Any others you add will be included as string fields.")

    validate_cols = st.checkbox("Validate recommended columns (`externalId`, `key`, `description`)", value=True)

    if type_choice == "custom" and not lookup_type:
        st.info("Enter a valid custom type to continue.")
        st.stop()

    if not lookup_type or not uploaded:
        st.stop()

    # Load with leading-zero preservation
    df = load_table(uploaded)
    st.write("Preview (as strings, leading zeros preserved):")
    st.dataframe(df.head(20), use_container_width=True)

    required = {"externalId", "key", "description"}
    if validate_cols:
        missing = [c for c in required if c not in df.columns]
        if missing:
            st.error(f"Missing columns: {', '.join(missing)}")
            st.stop()

    payloads = build_payloads(df)

    st.markdown("#### Payload preview")
    preview_count = min(5, len(payloads))
    if preview_count == 0:
        st.warning("No non-empty rows found.")
        st.stop()

    st.caption(f"Showing first {preview_count} of {len(payloads)} payload(s).")
    st.code(json.dumps(payloads[:preview_count], indent=2, ensure_ascii=False), language="json")

    endpoint = f"{base_url.rstrip('/')}/v2/enrichment/lookup-tables/{lookup_type}"
    st.write("Target endpoint:", endpoint)

    throttle_ms = st.slider("Throttle between requests (ms)", min_value=0, max_value=2000, value=0, step=50)
    dry_run = st.toggle("Dry run (do not POST, just preview JSON)", value=True)

    if st.button(f"Send {len(payloads)} request(s) to lookup table"):
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
        progress = st.progress(0, text="Uploading rows...")

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
        import pandas as pd
        st.dataframe(pd.DataFrame(results), use_container_width=True)
