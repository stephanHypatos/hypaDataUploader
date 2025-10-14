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

