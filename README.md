# Hypatos Uploader (Invoices & Lookup Tables)

A modular Streamlit app to:
- Upload **invoices** from CSV, transform them to the Hypatos enrichment schema, and POST to `/v2/enrichment/invoices`.
- Insert **lookup table rows** from CSV/Excel to `/v2/enrichment/lookup-tables/{type}` (e.g., `payment_terms`, `tax_codes`, `central_bank_indicator`, or a **custom** type).

The app is split into pages so you can easily add more data models in the future.

---

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Run the App](#run-the-app)
- [Usage](#usage)
  - [Connection Settings](#connection-settings)
  - [Upload Invoices](#upload-invoices)
  - [Lookup Tables](#lookup-tables)
- [Extending (Add a New Data Model)](#extending-add-a-new-data-model)
- [CSV/XLSX Notes](#csvxlsx-notes)
- [Auth Details](#auth-details)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

- **Two pages**:
  - **Upload Invoices**: your original, working transformation logic preserved 1:1, with dry-run and sample CSV generators.
  - **Lookup Tables**: upload Excel/CSV and send **one POST per row**; all values sanitized as **strings** (API requires strings).
- **Leading zeros preserved** from Excel/CSV (e.g., `'00001` → `00001`).
- **Dry run** mode to preview JSON without sending.
- **Payload preview** (first rows) before sending.
- **Throttling** option for lookup inserts.
- **Modular** architecture for easy future pages.

---

## Project Structure

uploadpostingdata/
├─ app.py # Main app + page routing
├─ helpers.py # Shared helpers (auth, loaders, builders, utils)
└─ pages/
├─ invoices.py # Invoices page (your existing logic)
└─ lookup_tables.py # Lookup tables page


---

## Requirements

- Python **3.10+** recommended
- See `requirements.txt`

---

## Installation

```bash
# 1) Clone your repo
git clone https://github.com/stephanHypatos/uploadpostingdata.git
cd uploadpostingdata

# 2) Create a venv (recommended)
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate

# 3) Install dependencies
pip install -r requirements.txt
```
## Run the App
`streamlit run app.py`

Streamlit will print a local URL (e.g., http://localhost:8501). Open it in your browser.

## Usage
### Connection Settings

In the sidebar (or the top section, depending on your UI):

- Base URL: default https://api.cloud.hypatos.ai
- Auth token path: default /v2/auth/token
- Client ID & Client Secret: OAuth client-credentials
- Click “Get/Refresh Token” to cache a token in session. Pages will also auto-refresh the token before POSTing.

### Upload Invoices

1. Upload a CSV of invoice lines (headers like externalId, line.externalId, quantity, …).
2. Optionally set header overrides (used when a header value is missing in CSV, or in Test mode).

3. Toggle:

- Header tax mode (use header tax vs. sum of line taxes)

- Dry run (preview JSON only)

- Pretty-print JSON

- Test without CSV (builds a dummy invoice payload from overrides)

4. Click Transform & Send.

5. Results panel shows the per-invoice response (or JSON preview in dry-run).

### Sample CSVs
Open the Sample CSV downloads section and download one of the prebuilt examples to see the expected format.

### Lookup Tables

1. Choose a type: payment_terms, tax_codes, central_bank_indicator, or custom.

- For custom, enter a name – it will be normalized to lowercase_with_underscores.

2. Upload an Excel/CSV with columns like externalId, key, description, and any additional fields you want to send.

3. Review the payload preview (first rows).

4. Optionally set a throttle (ms between requests) and Dry run.

5. Click Send N request(s) → app sends one POST per row to:

`{BaseURL}/v2/enrichment/lookup-tables/{type}`

## Extending (Add a New Data Model)

1. Create a new page file in pages/, e.g. pages/purchase_orders.py with a function:

```
def render_purchase_orders_page():
    # your UI + POST logic
    ...
```

2. Import & route it in app.py:
```
from pages.purchase_orders import render_purchase_orders_page
...
page = st.sidebar.radio("Page", ["Upload Invoices", "Lookup Tables", "Purchase Orders"])
...
elif page == "Purchase Orders":
    render_purchase_orders_page()
```

3. Reuse helpers from helpers.py: ensure_token, bearer_headers, load_table, etc.

## CSV/XLSX Notes

The app reads files with dtype=str and keep_default_na=False, so:

- Leading zeros are preserved (00001 stays "00001").

- Excel’s leading apostrophe ('00001) is safely stripped to 00001.

Empty cells are kept as empty strings and dropped from the outgoing JSON.

## Auth Details

The app uses OAuth client-credentials:

- POST application/x-www-form-urlencoded with grant_type=client_credentials

- HTTP Basic Auth with Client ID and Client Secret

- Expects access_token and expires_in in response

- The token is cached in Streamlit session state and auto-refreshed when needed

If your tenant requires additional headers for auth or API calls, add those to bearer_headers() or wire a small UI field to include them.

## Troubleshooting

### 401 Unauthorized (Auth failed)

- Verify Base URL and Auth token path match your tenant.

- Re-check Client ID / Secret for typos.

- Inspect token response in the UI after clicking “Get/Refresh Token”.

- If your auth flow is different (e.g., different field names), adapt get_access_token() in helpers.py.

### “Additional properties must be strings” (Lookup Tables)

- The page converts all values to strings before sending.

- Ensure your Excel/CSV doesn’t include complex JSON structures in a cell unless the API expects strings.

### Leading zeros lost

- Use the Lookup Tables page (which reads files as strings) or ensure your CSV has those fields treated as text before saving.

### Rate limiting / throttling

- Use the Throttle slider on the Lookup Tables page to add a delay between requests.

## License

MIT (or your preferred license)
