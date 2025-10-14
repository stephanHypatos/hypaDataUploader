# helpers.py
import io
import csv
import json
import math
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from typing import Tuple, List, Dict

import pandas as pd
import requests
import streamlit as st

# =========================
# --- Settings (shared)
# =========================
DEFAULT_BASE_URL = "https://api.cloud.hypatos.ai"
AUTH_PATH = "/v2/auth/token"
ENRICHMENT_INSERT_PATH = "/v2/enrichment/invoices"

# =========================
# --- Auth
# =========================
def get_access_token(base_url: str, client_id: str, client_secret: str, auth_path: str = AUTH_PATH):
    """
    OAuth2 client-credentials (x-www-form-urlencoded + HTTP Basic auth).
    Returns (token, expiry_dt_utc, raw_json)
    """
    url = base_url.rstrip("/") + auth_path
    data = {"grant_type": "client_credentials"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers, auth=(client_id.strip(), client_secret.strip()), timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Token request failed: {resp.status_code} {resp.text}")

    payload = resp.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))
    if not access_token:
        raise RuntimeError(f"No access_token in response: {payload}")
    return access_token, datetime.utcnow() + timedelta(seconds=expires_in), payload


def bearer_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def ensure_token(base_url: str, client_id: str, client_secret: str, auth_path: str = AUTH_PATH) -> Tuple[bool, str]:
    """
    Make sure a valid token exists in session state; refresh if needed.
    """
    if not client_id or not client_secret:
        return False, "Client ID and Client Secret are required."

    token = st.session_state.get("token")
    expiry = st.session_state.get("token_expiry")

    if not token or not expiry or datetime.utcnow() > (expiry - timedelta(seconds=30)):
        try:
            token, exp, _ = get_access_token(base_url, client_id, client_secret, auth_path=auth_path)
            st.session_state["token"] = token
            st.session_state["token_expiry"] = exp
            return True, f"Token acquired. Expires in ~{int((exp - datetime.utcnow()).total_seconds())}s."
        except Exception as e:
            return False, str(e)
    return True, "Token is still valid."

# =========================
# --- CSV/Excel loaders (preserve leading zeros & Excel apostrophe)
# =========================
def load_table(uploaded_file: io.BytesIO) -> pd.DataFrame:
    """
    Reads CSV/XLSX strictly as strings to preserve leading zeros and Excel's leading apostrophe.
    """
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, na_filter=False)
    else:
        df = pd.read_excel(
            uploaded_file,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl" if name.endswith("xlsx") else None,
        )

    # normalize headers
    df.columns = [str(c).strip() for c in df.columns]

    def _normalize(val):
        if val is None:
            return ""
        s = str(val).strip()
        # "'00001" → "00001" (Excel text marker) but keep normal words like "'note"
        if s.startswith("'") and s[1:].isdigit():
            return s[1:]
        return s

    return df.applymap(_normalize)


def row_to_string_payload(row: pd.Series) -> dict:
    """
    Build a JSON-ready dict where every value is a string and empties are dropped.
    """
    out = {}
    for k, v in row.items():
        if v is None:
            continue
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            continue
        out[k] = s
    return out


def build_payloads(df: pd.DataFrame) -> List[dict]:
    return [row_to_string_payload(df.iloc[i]) for i in range(len(df))]


def slugify_type(name: str) -> str:
    """
    Lowercase, trim, spaces→underscores, only [a-z0-9_]
    """
    if not name:
        return ""
    s = name.strip().lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_]", "", s)

# =========================
# --- Invoice-specific utils (ported 1:1 from your code)
# =========================
def d(value):
    """Safely convert to Decimal or return None."""
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def to_num(value):
    """Convert Decimal/str to float for JSON."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def clean_date(s):
    """Normalize to YYYY-MM-DD when possible."""
    if not s or not str(s).strip():
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def first_nonempty(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "":
            return v
    return None


def _sample_rows(with_gl_cc: bool = False):
    """(unchanged)"""
    base_common_1 = {
        "externalId": "ext-1",
        "documentId": "686caa631bb57c4804f8a681",
        "supplierInvoiceNumber": "INV-001",
        "invoiceNumber": "1001",
        "externalCompanyId": "COMP-001",
        "externalSupplierId": "SUP-001",
        "currency": "EUR",
        "issuedDate": "2025-08-01",
        "line.externalId": "line-1",
        "quantity": "2",
        "unitPrice": "50.00",
        "netAmount": "100.00",
        "totalTaxAmount": "19.00",
        "grossAmount": "119.00",
        "itemText": "Consulting Service",
    }
    base_common_2_1 = {
        "externalId": "ext-2",
        "documentId": "686caa631bb57c4804f8a682",
        "supplierInvoiceNumber": "INV-002",
        "invoiceNumber": "1002",
        "externalCompanyId": "COMP-002",
        "externalSupplierId": "SUP-002",
        "currency": "USD",
        "issuedDate": "2025-08-05",
        "line.externalId": "line-1",
        "quantity": "5",
        "unitPrice": "20.00",
        "netAmount": "100.00",
        "totalTaxAmount": "10.00",
        "grossAmount": "110.00",
        "itemText": "Office Supplies",
    }
    base_common_2_2 = {
        "externalId": "ext-2",
        "documentId": "686caa631bb57c4804f8a682",
        "supplierInvoiceNumber": "INV-002",
        "invoiceNumber": "1002",
        "externalCompanyId": "COMP-002",
        "externalSupplierId": "SUP-002",
        "currency": "USD",
        "issuedDate": "2025-08-05",
        "line.externalId": "line-2",
        "quantity": "1",
        "unitPrice": "200.00",
        "netAmount": "200.00",
        "totalTaxAmount": "20.00",
        "grossAmount": "220.00",
        "itemText": "Software License",
    }

    if with_gl_cc:
        base_common_1.update({
            "externalGlAccountId": "GL-7000", "glAccountCode": "7000",
            "externalCostCenterId": "CC-100", "costCenterCode": "ADMIN-100",
        })
        base_common_2_1.update({
            "externalGlAccountId": "GL-4000", "glAccountCode": "4000",
            "externalCostCenterId": "CC-200", "costCenterCode": "OPS-200",
        })
        base_common_2_2.update({
            "externalGlAccountId": "GL-6500", "glAccountCode": "6500",
            "externalCostCenterId": "CC-300", "costCenterCode": "IT-300",
        })

    return [base_common_1, base_common_2_1, base_common_2_2]


def make_sample_csv_bytes(with_gl_cc: bool = False) -> bytes:
    header = [
        "externalId","documentId","supplierInvoiceNumber","invoiceNumber",
        "externalCompanyId","externalSupplierId","currency","issuedDate",
        "line.externalId","quantity","unitPrice","netAmount","totalTaxAmount",
        "grossAmount","itemText",
        "externalGlAccountId","glAccountCode","externalCostCenterId","costCenterCode",
    ]
    rows = _sample_rows(with_gl_cc=with_gl_cc)
    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=header, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return sio.getvalue().encode("utf-8")


def make_scenarios_csv_bytes() -> bytes:
    def doc_id(n: int) -> str:
        return f"20250820{n:016d}"[:24]

    common_header = {
        "externalCompanyId": "COMP-DE-01",
        "externalSupplierId": "SUP-DE-01",
        "currency": "EUR",
        "issuedDate": "2025-08-10",
        "receivedDate": "2025-08-11",
        "postingDate": "2025-08-12",
        "isCanceled": "false",
        "isCreditNote": "false",
        "headerText": "Sample invoice for demo",
        "paymentTermKey": "NET30",
        "paymentTermText": "Net 30 days",
        "paymentTermLanguage": "en",
        "unitOfMeasure": "EA",
        "taxCode.code": "DEU_Standard",
        "taxCode.description": "DEU - Standard (19%)",
        "taxJurisdictionCode": "DEU",
    }

    rows = []
    # (unchanged from your code; omitted here for brevity—copy exactly as in your file)
    # -- BEGIN copy of your four scenarios --
    rows.append({
        **common_header,
        "externalId": "ext-fi-1",
        "documentId": doc_id(1),
        "supplierInvoiceNumber": "FINV-001",
        "invoiceNumber": "10001",
        "line.externalId": "line-1",
        "quantity": "2",
        "unitPrice": "50.00",
        "netAmount": "100.00",
        "totalTaxAmount": "19.00",
        "grossAmount": "119.00",
        "itemText": "Consulting Service",
        "externalGlAccountId": "GL-7000",
        "glAccountCode": "7000",
        "externalCostCenterId": "CC-100",
        "costCenterCode": "ADMIN-100",
    })
    rows.append({
        **common_header,
        "externalId": "ext-fi-2",
        "documentId": doc_id(2),
        "supplierInvoiceNumber": "FINV-002",
        "invoiceNumber": "10002",
        "line.externalId": "line-1",
        "quantity": "3",
        "unitPrice": "40.00",
        "netAmount": "120.00",
        "totalTaxAmount": "22.80",
        "grossAmount": "142.80",
        "itemText": "Hardware Components",
        "externalGlAccountId": "GL-4000",
        "glAccountCode": "4000",
        "externalCostCenterId": "CC-200",
        "costCenterCode": "OPS-200",
    })
    rows.append({
        **common_header,
        "externalId": "ext-fi-2",
        "documentId": doc_id(2),
        "supplierInvoiceNumber": "FINV-002",
        "invoiceNumber": "10002",
        "line.externalId": "line-2",
        "quantity": "5",
        "unitPrice": "40.00",
        "netAmount": "200.00",
        "totalTaxAmount": "38.00",
        "grossAmount": "238.00",
        "itemText": "Software Subscription",
        "externalGlAccountId": "GL-6500",
        "glAccountCode": "6500",
        "externalCostCenterId": "CC-300",
        "costCenterCode": "IT-300",
    })
    rows.append({
        **common_header,
        "externalId": "ext-po-1",
        "documentId": doc_id(3),
        "supplierInvoiceNumber": "POINV-001",
        "invoiceNumber": "20001",
        "line.externalId": "line-1",
        "quantity": "3",
        "unitPrice": "50.00",
        "netAmount": "150.00",
        "totalTaxAmount": "28.50",
        "grossAmount": "178.50",
        "itemText": "Maintenance Service",
        "externalPurchaseOrderId": "4500000001",
        "purchaseOrderLineNumber": "00010",
    })
    rows.append({
        **common_header,
        "externalId": "ext-po-2",
        "documentId": doc_id(4),
        "supplierInvoiceNumber": "POINV-002",
        "invoiceNumber": "20002",
        "line.externalId": "line-1",
        "quantity": "4",
        "unitPrice": "20.00",
        "netAmount": "80.00",
        "totalTaxAmount": "15.20",
        "grossAmount": "95.20",
        "itemText": "Packaging Materials",
        "externalPurchaseOrderId": "4500000002",
        "purchaseOrderLineNumber": "00010",
    })
    rows.append({
        **common_header,
        "externalId": "ext-po-2",
        "documentId": doc_id(4),
        "supplierInvoiceNumber": "POINV-002",
        "invoiceNumber": "20002",
        "line.externalId": "line-2",
        "quantity": "10",
        "unitPrice": "23.00",
        "netAmount": "230.00",
        "totalTaxAmount": "43.70",
        "grossAmount": "273.70",
        "itemText": "Transport Service",
        "externalPurchaseOrderId": "4500000002",
        "purchaseOrderLineNumber": "00020",
    })
    # -- END copy --

    header = [
        "externalId","documentId","supplierInvoiceNumber","invoiceNumber",
        "externalCompanyId","externalSupplierId","currency",
        "issuedDate","receivedDate","postingDate","isCanceled","isCreditNote",
        "headerText","paymentTermKey","paymentTermText","paymentTermLanguage",
        "unitOfMeasure","taxCode.code","taxCode.description","taxJurisdictionCode",
        "line.externalId","quantity","unitPrice","netAmount","totalTaxAmount","grossAmount","itemText",
        "externalGlAccountId","glAccountCode","externalCostCenterId","costCenterCode",
        "externalPurchaseOrderId","purchaseOrderLineNumber"
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=header, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return sio.getvalue().encode("utf-8")


def build_invoice_payload_from_rows(rows, overrides, header_tax_mode=False):
    """
    (unchanged logic from your file)
    """
    first = rows[0]

    external_id = first.get("externalId") or overrides.get("external_id")
    if not external_id:
        raise ValueError("externalId is required (CSV column 'externalId' or provided in overrides).")

    payload = {
        "externalId": external_id,
        "externalClientId": first_nonempty(first.get("externalClientId"), overrides.get("external_client_id")),
        "documentId": first_nonempty(first.get("documentId"), overrides.get("document_id")),
        "documents": [],
        "supplierInvoiceNumber": first.get("supplierInvoiceNumber"),
        "invoiceNumber": first.get("invoiceNumber"),
        "externalCompanyId": first_nonempty(first.get("externalCompanyId"), overrides.get("external_company_id")),
        "externalSupplierId": first_nonempty(first.get("externalSupplierId"), overrides.get("external_supplier_id")),
        "externalBankAccountId": first.get("externalBankAccountId"),
        "fiscalYearLabel": first.get("fiscalYearLabel"),
        "issuedDate": clean_date(first.get("issuedDate")),
        "receivedDate": clean_date(first.get("receivedDate")),
        "postingDate": clean_date(first.get("postingDate")),
        "isCanceled": (str(first.get("isCanceled")).lower() == "true") if first.get("isCanceled") else None,
        "isCreditNote": (str(first.get("isCreditNote")).lower() == "true") if first.get("isCreditNote") else None,
        "externalCustomerId": first.get("externalCustomerId"),
        "relatedInvoice": first.get("relatedInvoice"),
        "currency": first_nonempty(first.get("currency"), overrides.get("currency")),
        "totalNetAmount": None,
        "totalFreightCharges": to_num(d(first.get("totalFreightCharges"))),
        "totalOtherCharges": to_num(d(first.get("totalOtherCharges"))),
        "totalTaxAmount": None,
        "totalGrossAmount": None,
        "paymentTerms": None,
        "externalApproverId": first.get("externalApproverId"),
        "customFields": {},
        "customMetadata": None,
        "headerText": first.get("headerText"),
        "type": first.get("type"),
        "invoiceLines": [],
        "withholdingTax": [],
        "documentType": first.get("documentType"),
    }

    if payload["documentId"]:
        payload["documents"] = [{"id": payload["documentId"], "type": "invoice"}]

    pt_key = first.get("paymentTermKey")
    pt_text = first.get("paymentTermText")
    pt_lang = first.get("paymentTermLanguage") or "en"
    if pt_key or pt_text:
        payload["paymentTerms"] = {
            "paymentTermKey": pt_key,
            "descriptions": [{"text": pt_text or "", "language": pt_lang}],
        }

    for k, v in first.items():
        if k.startswith("customFields."):
            payload["customFields"][k.split("customFields.", 1)[1]] = v

    cm = first.get("customMetadata")
    if cm:
        try:
            payload["customMetadata"] = json.loads(cm)
        except json.JSONDecodeError:
            pass

    if first.get("wht.key") or first.get("wht.amount") or first.get("wht.baseAmount"):
        wht = {
            "key": first.get("wht.key"),
            "baseAmount": to_num(d(first.get("wht.baseAmount"))),
            "amount": to_num(d(first.get("wht.amount"))),
            "currency": first_nonempty(first.get("wht.currency"), payload["currency"]),
        }
        payload["withholdingTax"].append(wht)

    sum_net = Decimal("0")
    sum_tax = Decimal("0")
    sum_gross = Decimal("0")

    for r in rows:
        line = {
            "externalId": r.get("line.externalId") or r.get("lineExternalId"),
            "externalCompanyId": first_nonempty(r.get("line.externalCompanyId"), payload["externalCompanyId"]),
            "type": r.get("line.type") or r.get("type"),
            "quantity": to_num(d(r.get("quantity"))),
            "netAmount": to_num(d(r.get("netAmount"))),
            "grossAmount": to_num(d(r.get("grossAmount"))),
            "unitOfMeasure": r.get("unitOfMeasure"),
            "unitPrice": to_num(d(r.get("unitPrice"))),
            "taxCode": None,
            "taxJurisdictionCode": r.get("taxJurisdictionCode"),
            "itemText": r.get("itemText"),
            "externalPurchaseOrderId": r.get("externalPurchaseOrderId"),
            "purchaseOrderLineNumber": r.get("purchaseOrderLineNumber"),
            "centralBankIndicator": r.get("centralBankIndicator"),
            "customFields": {},
            "customMetadata": None,
            "accountAssignments": [],
        }

        tax_code = r.get("taxCode.code") or r.get("taxCodeCode")
        tax_desc = r.get("taxCode.description") or r.get("taxCodeDescription")
        if tax_code or tax_desc:
            line["taxCode"] = {"code": tax_code, "description": tax_desc}

        for k, v in r.items():
            if k.startswith("line.customFields."):
                line["customFields"][k.split("line.customFields.", 1)[1]] = v
        lcm = r.get("line.customMetadata")
        if lcm:
            try:
                line["customMetadata"] = json.loads(lcm)
            except json.JSONDecodeError:
                pass

        aa = {
            "externalGlAccountId": r.get("externalGlAccountId"),
            "externalCostCenterId": r.get("externalCostCenterId"),
            "glAccountCode": r.get("glAccountCode"),
            "costCenterCode": r.get("costCenterCode"),
            "quantity": to_num(d(r.get("aa.quantity"))),
            "externalProjectId": r.get("externalProjectId"),
            "externalOrderId": r.get("externalOrderId"),
            "costElementCode": r.get("costElementCode"),
        }
        if any(v is not None and str(v) != "" for v in aa.values()):
            line["accountAssignments"].append(aa)

        if (nv := d(r.get("netAmount"))) is not None:
            sum_net += nv
        if (gv := d(r.get("grossAmount"))) is not None:
            sum_gross += gv

        if not header_tax_mode:
            line_tax_val = to_num(d(r.get("totalTaxAmount")))
            if line_tax_val is not None:
                line["totalTaxAmount"] = line_tax_val
            if (tv := d(r.get("totalTaxAmount"))) is not None:
                sum_tax += tv

        payload["invoiceLines"].append(line)

    payload["totalNetAmount"] = to_num(sum_net)

    if header_tax_mode:
        header_tax = to_num(d(rows[0].get("totalTaxAmount")))
        payload["totalTaxAmount"] = header_tax
    else:
        payload["totalTaxAmount"] = to_num(sum_tax)

    if payload["totalFreightCharges"] is None:
        payload["totalFreightCharges"] = 0.0
    if payload["totalOtherCharges"] is None:
        payload["totalOtherCharges"] = 0.0

    if sum_gross:
        payload["totalGrossAmount"] = to_num(sum_gross)
    else:
        gross = sum_net + (d(payload["totalTaxAmount"]) or Decimal("0")) \
                + Decimal(str(payload["totalFreightCharges"])) \
                + Decimal(str(payload["totalOtherCharges"]))
        payload["totalGrossAmount"] = to_num(gross)

    def prune(obj):
        if isinstance(obj, dict):
            return {k: prune(v) for k, v in obj.items() if v not in (None, {}, [], "")}
        if isinstance(obj, list):
            return [prune(x) for x in obj if x not in (None, {}, [], "")]
        return obj

    return prune(payload)


def read_csv_grouped_by_external_id(file_like) -> Dict[str, list]:
    text = file_like.read().decode("utf-8")
    reader = csv.DictReader(text.splitlines())
    groups = defaultdict(list)
    for row in reader:
        ext = row.get("externalId") or row.get("invoiceExternalId")
        if not ext:
            raise ValueError("Each row must have 'externalId' (invoice header id).")
        groups[ext].append(row)
    return groups
