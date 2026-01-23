import argparse
import csv
import hashlib
import statistics
import json
import re
import sqlite3
import shutil
import threading
import unicodedata
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
APP_BASE = Path(__file__).stem.split("-", 1)[0]
DB_PATH = Path(__file__).with_name(f"{APP_BASE}.db")

SHEET_SP_ORDERS = "Porud\u017ebine"
SHEET_SP_PAYMENTS = "Knji\u017eenje kupaca"
SHEET_MINIMAX = "Minimax"

COL = {
    "client": "Klijent",
    "tracking": "Kod po\u0161iljke",
    "sp_order_no": "Broj porud\u017ebine",
    "woo_order_no": "Klijent broj porud\u017ebine",
    "location": "Lokacija",
    "customer_code": "\u0160ifra kupca",
    "customer_name": "Ime",
    "city": "Grad",
    "address": "Adresa",
    "postal_code": "Po\u0161tanski broj",
    "phone": "Broj telefona",
    "email": "Broj telefona.1",
    "note": "Napomena",
    "product_code": "\u0160ifra proizvoda",
    "qty": "Koli\u010dina proizvoda",
    "cod_amount": "Otkup proizvoda",
    "advance_amount": "Avansni iznos proizvoda",
    "discount": "Popust proizvoda",
    "discount_type": "Tip popusta proizvoda",
    "addon_cod": "Otkup dodatak",
    "addon_advance": "Avansni iznos dodatka",
    "extra_discount": "Popust",
    "extra_discount_type": "Tip popusta",
    "status": "Status",
    "created_at": "Datum kreiranja",
    "picked_up_at": "Datum preuzimanja",
    "delivered_at": "Datum isporuke",
    # SP-Uplate
    "payment_amount": "Iznos",
    "payment_order_status": "Status porud\u017ebine",
    "payment_client_status": "Status klijenta",
    "payment_customer_name": "Ime kupca",
    # Minimax
    "mm_number": "Broj",
    "mm_customer": "Kupac",
    "mm_country": "Dr\u017eava stranke",
    "mm_date": "Datum",
    "mm_due_date": "Dospe\u0107e",
    "mm_revenue": "Prihod",
    "mm_amount_local": "Iznos u NJ",
    "mm_amount_due": "Iznos za pla\u0107anje",
    "mm_analytics": "Analitika",
    "mm_turnover": "Promet",
    "mm_account": "Ra\u010dun",
    "mm_basis": "Osnova za ra\u010dun",
    "mm_note": "Napomene",
    "mm_payment_amount": "Iznos pla\u0107anja",
    "mm_open_amount": "Otvoreno",
}

SETTINGS_PATH = Path(__file__).with_name("srb_settings.json")
CUSTOMER_KEY_VERSION = "2"

DEFAULT_PREFIX_MAP = {
    "AF-": "Afro rep",
    "RR-": "Ravni rep",
    "OPK-": "Repovi OPK",
    "AR-": "Ariana repovi",
    "KRR-": "Kratki repovi",
    "KRO-": "Kratki repovi",
    "KRA-": "Kratki repovi",
    "TRK-": "Repovi trakica",
    "DR-": "Dugi repovi",
    "U-": "U klipse",
    "BD-": "Blowdry klipse",
    "BDR-": "Blowdry repovi",
    "EKS-": "Ekstenzije",
    "EKSOPK": "Ekstenzije OPK",
    "SIS-": "Siske",
    "P0": "Klasicne perike",
    "PR": "Premium perike",
}

DEFAULT_CUSTOM_SKU_LIST = []
DEFAULT_SKU_CATEGORY_OVERRIDES = {}


def _normalize_sku_list(values):
    result = []
    for v in values or []:
        if isinstance(v, str):
            v = v.strip().upper()
            if v:
                result.append(v)
    return result


def _normalize_prefix_map(values):
    result = {}
    for k, v in (values or {}).items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        key = k.strip().upper()
        val = v.strip()
        if key and val:
            result[key] = val
    return result


def _normalize_overrides(values):
    result = {}
    for k, v in (values or {}).items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        key = k.strip().upper()
        val = v.strip()
        if key and val:
            result[key] = val
    return result


def load_category_settings():
    prefix = DEFAULT_PREFIX_MAP.copy()
    custom_list = list(DEFAULT_CUSTOM_SKU_LIST)
    overrides = DEFAULT_SKU_CATEGORY_OVERRIDES.copy()
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            prefix.update(_normalize_prefix_map(data.get("prefix_map")))
            custom_list = _normalize_sku_list(data.get("custom_skus", custom_list))
            overrides = _normalize_overrides(
                data.get("sku_category_overrides", overrides)
            )
        except Exception:
            pass
    return prefix, custom_list, overrides


def save_category_settings() -> None:
    data = {
        "prefix_map": prefix_map,
        "custom_skus": sorted(CUSTOM_SKU_SET),
        "sku_category_overrides": SKU_CATEGORY_OVERRIDES,
    }
    SETTINGS_PATH.write_text(
        json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8"
    )


def load_app_settings() -> dict:
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}

        def _resolve_setting_path(value: str | None, default: Path | None) -> Path | None:
            if not value:
                return default
            try:
                raw = Path(str(value))
            except Exception:
                return default

            if raw.is_absolute():
                if raw.exists():
                    return raw
                # Folder moved: try same filename/folder name inside current app dir.
                alt = APP_DIR / raw.name
                if alt.exists():
                    return alt
                # Also try last path component under APP_DIR (covers old absolute folder roots).
                try:
                    alt2 = APP_DIR / Path(*raw.parts[-2:])  # e.g. "Kalkulacije_kartice_art/izlaz"
                    if alt2.exists():
                        return alt2
                except Exception:
                    pass
                return default

            rel = APP_DIR / raw
            if rel.exists():
                return rel
            return default or rel

        def _store_setting_path(path: Path | None) -> str | None:
            if path is None:
                return None
            try:
                rel = path.resolve().relative_to(APP_DIR)
                return str(rel).replace("\\", "/")
            except Exception:
                return str(path)

        desired = {
            "db_path": _store_setting_path(_resolve_setting_path(data.get("db_path"), DB_PATH)),
            "kalkulacije_output_dir": _store_setting_path(
                _resolve_setting_path(data.get("kalkulacije_output_dir"), APP_DIR / "Kalkulacije_kartice_art")
            ),
            "kartice_output_dir": _store_setting_path(
                _resolve_setting_path(data.get("kartice_output_dir"), APP_DIR / "Kartice artikala")
            ),
        }
        changed = False
        for k, v in desired.items():
            if v is None:
                continue
            if data.get(k) != v:
                data[k] = v
                changed = True
        if changed:
            try:
                SETTINGS_PATH.write_text(
                    json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8"
                )
            except Exception:
                pass
        return data
    return {}


def save_app_settings(values: dict) -> None:
    data = load_app_settings()

    def _maybe_rel(p: str) -> str:
        try:
            path = Path(p)
        except Exception:
            return p
        try:
            rel = path.resolve().relative_to(APP_DIR)
            return str(rel).replace("\\", "/")
        except Exception:
            return p

    for key, val in list(values.items()):
        if key in {"db_path", "kalkulacije_output_dir", "kartice_output_dir"} and isinstance(val, str):
            values[key] = _maybe_rel(val)
    data.update(values)
    SETTINGS_PATH.write_text(
        json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8"
    )


prefix_map, CUSTOM_SKU_LIST, SKU_CATEGORY_OVERRIDES = load_category_settings()
CUSTOM_SKU_SET = {s.upper() for s in CUSTOM_SKU_LIST}


def sifra_to_prefix(sifra: str) -> str:
    if not isinstance(sifra, str):
        return ""
    sifra = sifra.strip().upper()
    candidates = [p for p in prefix_map.keys() if sifra.startswith(p)]
    if not candidates:
        return ""
    return max(candidates, key=len)


def kategorija_za_sifru(sifra: str, allow_custom: bool = True) -> str:
    if not isinstance(sifra, str):
        return "Ostalo"
    sku = sifra.strip().upper()
    if sku in SKU_CATEGORY_OVERRIDES:
        return SKU_CATEGORY_OVERRIDES[sku]
    if allow_custom and sku in CUSTOM_SKU_SET:
        return "Custom"
    pref = sifra_to_prefix(sifra)
    return prefix_map.get(pref, "Ostalo")


def add_category_prefix(prefix: str, name: str) -> None:
    prefix = (prefix or "").strip().upper()
    name = (name or "").strip()
    if not prefix or not name:
        raise ValueError("Prefix i naziv su obavezni.")
    prefix_map[prefix] = name
    save_category_settings()


def add_sku_category_override(sku: str, category: str) -> None:
    sku = (sku or "").strip().upper()
    category = (category or "").strip()
    if not sku or not category:
        raise ValueError("SKU i kategorija su obavezni.")
    SKU_CATEGORY_OVERRIDES[sku] = category
    save_category_settings()


def add_custom_sku(sku: str) -> None:
    sku = (sku or "").strip().upper()
    if not sku:
        raise ValueError("SKU je obavezan.")
    CUSTOM_SKU_SET.add(sku)
    save_category_settings()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS import_runs (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  filename TEXT NOT NULL,
  file_hash TEXT NOT NULL,
  imported_at TEXT NOT NULL,
  row_count INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_import_runs_file_hash
  ON import_runs(file_hash);

CREATE TABLE IF NOT EXISTS app_state (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY,
  sp_order_no TEXT NOT NULL,
  woo_order_no TEXT,
  client_code TEXT,
  tracking_code TEXT,
  customer_code TEXT,
  customer_name TEXT,
  city TEXT,
  address TEXT,
  postal_code TEXT,
  phone TEXT,
  email TEXT,
  customer_key TEXT,
  note TEXT,
  location TEXT,
  status TEXT,
  created_at TEXT,
  picked_up_at TEXT,
  delivered_at TEXT,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_sp_order_no
  ON orders(sp_order_no);

CREATE INDEX IF NOT EXISTS idx_orders_tracking
  ON orders(tracking_code);

CREATE INDEX IF NOT EXISTS idx_orders_dates
  ON orders(picked_up_at, delivered_at);

CREATE TABLE IF NOT EXISTS order_status_history (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  status_at TEXT NOT NULL,
  source TEXT,
  note TEXT,
  FOREIGN KEY(order_id) REFERENCES orders(id)
);

CREATE INDEX IF NOT EXISTS idx_order_status_order
  ON order_status_history(order_id);

CREATE TABLE IF NOT EXISTS order_items (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_code TEXT,
  qty REAL,
  cod_amount REAL,
  advance_amount REAL,
  discount REAL,
  discount_type TEXT,
  addon_cod REAL,
  addon_advance REAL,
  extra_discount REAL,
  extra_discount_type TEXT,
  FOREIGN KEY(order_id) REFERENCES orders(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_items_unique
  ON order_items(order_id, product_code, qty, cod_amount);

CREATE INDEX IF NOT EXISTS idx_order_items_order
  ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product
  ON order_items(product_code);

CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY,
  sp_order_no TEXT NOT NULL,
  client_code TEXT,
  customer_code TEXT,
  customer_name TEXT,
  amount REAL,
  order_status TEXT,
  client_status TEXT,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_dedupe
  ON payments(sp_order_no, amount, client_status);

CREATE INDEX IF NOT EXISTS idx_payments_sp_order_no
  ON payments(sp_order_no);

CREATE TABLE IF NOT EXISTS returns (
  id INTEGER PRIMARY KEY,
  sp_order_no TEXT,
  tracking_code TEXT,
  customer_name TEXT,
  phone TEXT,
  city TEXT,
  status TEXT,
  created_at TEXT,
  picked_up_at TEXT,
  delivered_at TEXT,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_returns_dedupe
  ON returns(sp_order_no, tracking_code);

CREATE INDEX IF NOT EXISTS idx_returns_tracking
  ON returns(tracking_code);

CREATE TABLE IF NOT EXISTS invoices (
  id INTEGER PRIMARY KEY,
  number TEXT,
  customer_name TEXT,
  country TEXT,
  date TEXT,
  due_date TEXT,
  revenue TEXT,
  amount_local REAL,
  amount_due REAL,
  analytics TEXT,
  turnover TEXT,
  account TEXT,
  basis TEXT,
  note TEXT,
  payment_amount REAL,
  open_amount REAL,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoices_number
  ON invoices(number);

CREATE INDEX IF NOT EXISTS idx_invoices_date_amount
  ON invoices(date, amount_due);

CREATE TABLE IF NOT EXISTS invoice_matches (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  invoice_id INTEGER NOT NULL,
  score INTEGER NOT NULL,
  status TEXT NOT NULL, -- auto, review, needs_invoice
  method TEXT,
  matched_at TEXT NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(id),
  FOREIGN KEY(invoice_id) REFERENCES invoices(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_matches_order
  ON invoice_matches(order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_matches_invoice
  ON invoice_matches(invoice_id);

CREATE TABLE IF NOT EXISTS invoice_candidates (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  invoice_id INTEGER NOT NULL,
  score INTEGER NOT NULL,
  detail TEXT,
  method TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(id),
  FOREIGN KEY(invoice_id) REFERENCES invoices(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_candidates_pair
  ON invoice_candidates(order_id, invoice_id);

CREATE TABLE IF NOT EXISTS order_flags (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  flag TEXT NOT NULL, -- needs_invoice
  note TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_flags_unique
  ON order_flags(order_id, flag);

CREATE TABLE IF NOT EXISTS invoice_storno (
  id INTEGER PRIMARY KEY,
  storno_invoice_id INTEGER NOT NULL,
  original_invoice_id INTEGER NOT NULL,
  storno_amount REAL,
  original_amount REAL,
  remaining_open REAL,
  is_partial INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(storno_invoice_id) REFERENCES invoices(id),
  FOREIGN KEY(original_invoice_id) REFERENCES invoices(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_storno_unique
  ON invoice_storno(storno_invoice_id);

CREATE TABLE IF NOT EXISTS action_log (
  id INTEGER PRIMARY KEY,
  action TEXT NOT NULL,
  ref_type TEXT NOT NULL,
  ref_id INTEGER NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_action_log_ref
  ON action_log(ref_type, ref_id);

CREATE TABLE IF NOT EXISTS minimax_items (
  id INTEGER PRIMARY KEY,
  sku TEXT NOT NULL,
  name TEXT,
  unit TEXT,
  mass_kg REAL,
  stock REAL,
  opening_qty REAL,
  opening_purchase_value REAL,
  opening_sales_value REAL,
  incoming_qty REAL,
  incoming_purchase_value REAL,
  incoming_sales_value REAL,
  outgoing_qty REAL,
  outgoing_purchase_value REAL,
  outgoing_sales_value REAL,
  stock_2 REAL,
  closing_qty REAL,
  closing_purchase_value REAL,
  closing_sales_value REAL,
  updated_at TEXT,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_minimax_items_sku
  ON minimax_items(sku);

CREATE TABLE IF NOT EXISTS bank_transactions (
  id INTEGER PRIMARY KEY,
  fitid TEXT NOT NULL,
  stmt_number TEXT,
  benefit TEXT,
  dtposted TEXT,
  amount REAL,
  purpose TEXT,
  purposecode TEXT,
  payee_name TEXT,
  payee_city TEXT,
  payee_acctid TEXT,
  payee_bankid TEXT,
  payee_bankname TEXT,
  refnumber TEXT,
  payeerefnumber TEXT,
  urgency TEXT,
  fee REAL,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_fitid
  ON bank_transactions(fitid);

CREATE INDEX IF NOT EXISTS idx_bank_dtposted
  ON bank_transactions(dtposted);

CREATE INDEX IF NOT EXISTS idx_bank_payee
  ON bank_transactions(payee_name);

CREATE TABLE IF NOT EXISTS bank_refunds (
  id INTEGER PRIMARY KEY,
  bank_txn_id INTEGER NOT NULL,
  invoice_no TEXT,
  invoice_no_digits TEXT,
  invoice_no_source TEXT,
  reason TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(bank_txn_id) REFERENCES bank_transactions(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_refunds_txn
  ON bank_refunds(bank_txn_id);

CREATE TABLE IF NOT EXISTS bank_matches (
  id INTEGER PRIMARY KEY,
  bank_txn_id INTEGER NOT NULL,
  match_type TEXT NOT NULL, -- sp_payment | storno
  ref_id INTEGER NOT NULL,
  score INTEGER NOT NULL,
  method TEXT,
  matched_at TEXT NOT NULL,
  FOREIGN KEY(bank_txn_id) REFERENCES bank_transactions(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_matches_txn
  ON bank_matches(bank_txn_id);

CREATE TABLE IF NOT EXISTS tracking_events (
  id INTEGER PRIMARY KEY,
  tracking_code TEXT NOT NULL,
  status_time TEXT,
  status_value TEXT,
  fetched_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tracking_events_unique
  ON tracking_events(tracking_code, status_time, status_value);

CREATE TABLE IF NOT EXISTS tracking_summary (
  tracking_code TEXT PRIMARY KEY,
  received_at TEXT,
  first_out_for_delivery_at TEXT,
  delivery_attempts INTEGER,
  failure_reasons TEXT,
  returned_at TEXT,
  days_to_first_attempt REAL,
  has_attempt_before_return INTEGER,
  has_returned INTEGER,
  anomalies TEXT,
  last_status TEXT,
  last_status_at TEXT,
  last_fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sp_prijemi_receipts (
  receipt_key TEXT PRIMARY KEY,
  client_code TEXT,
  created_at TEXT,
  verified_at TEXT,
  status TEXT,
  latest_file_hash TEXT,
  latest_file_name TEXT,
  import_run_id INTEGER,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_sp_prijemi_receipts_verified_at
  ON sp_prijemi_receipts(verified_at);

CREATE INDEX IF NOT EXISTS idx_sp_prijemi_receipts_created_at
  ON sp_prijemi_receipts(created_at);

CREATE TABLE IF NOT EXISTS sp_prijemi_lines (
  id INTEGER PRIMARY KEY,
  receipt_key TEXT NOT NULL,
  sku TEXT NOT NULL,
  product_name TEXT,
  sent_qty REAL,
  arrived_qty REAL,
  import_run_id INTEGER,
  FOREIGN KEY(receipt_key) REFERENCES sp_prijemi_receipts(receipt_key) ON DELETE CASCADE,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sp_prijemi_lines_unique
  ON sp_prijemi_lines(receipt_key, sku);

CREATE INDEX IF NOT EXISTS idx_sp_prijemi_lines_sku
  ON sp_prijemi_lines(sku);

CREATE TABLE IF NOT EXISTS kartice_events (
  id INTEGER PRIMARY KEY,
  event_key TEXT NOT NULL,
  sku TEXT NOT NULL,
  item_name TEXT,
  event_date TEXT NOT NULL,
  broj TEXT,
  tip TEXT,
  smer TEXT,
  opis TEXT,
  referenca TEXT,
  ref_key TEXT NOT NULL,
  prijem_qty REAL,
  prijem_value REAL,
  izdavanje_qty REAL,
  izdavanje_value REAL,
  cena REAL,
  stanje_qty REAL,
  stanje_value REAL,
  delta_qty REAL,
  source_file TEXT,
  import_run_id INTEGER,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_kartice_events_event_key
  ON kartice_events(event_key);

CREATE INDEX IF NOT EXISTS idx_kartice_events_sku_date
  ON kartice_events(sku, event_date);

CREATE TABLE IF NOT EXISTS task_progress (
  task TEXT PRIMARY KEY,
  total INTEGER NOT NULL,
  processed INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
"""


from srb_modules.db import (
    connect_db,
    file_hash,
    get_app_state,
    get_task_progress,
    init_db as _init_db,
    set_app_state,
    set_task_progress,
    update_task_progress,
)
from srb_modules.pipelines import run_regenerate_sku_metrics_process
from srb_modules.ui_context import UIContext
from srb_modules.ui_finansije import build_finansije_tab
from srb_modules.ui_poslovanje import build_poslovanje_tab
from srb_modules.ui_nepreuzete import build_nepreuzete_tab
from srb_modules.ui_povrati import build_povrati_tab
from srb_modules.ui_prodaja import build_prodaja_tab
from srb_modules.ui_prodaja_logic import ProdajaLogicDeps, init_prodaja_logic
from srb_modules.ui_troskovi import build_troskovi_tab
from srb_modules.import_sp import (
    import_sp_orders as _import_sp_orders,
    import_sp_payments as _import_sp_payments,
    import_sp_returns as _import_sp_returns,
)
from srb_modules.import_minimax import (
    import_minimax as _import_minimax,
    import_minimax_items as _import_minimax_items,
)
from srb_modules.import_bank_xml import import_bank_xml as _import_bank_xml
from srb_modules.import_sp_prijemi import (
    import_sp_prijem as _import_sp_prijem,
    import_sp_prijemi_folder as _import_sp_prijemi_folder,
)
from srb_modules.import_kartice_events import (
    import_kartice_events_csv as _import_kartice_events_csv,
)
from srb_modules.reset_sources import RESET_SPECS, reset_source as _reset_source
from srb_modules.queries import (
    build_refund_item_totals,
    date_filter_clause,
    get_expense_summary,
    get_finansije_monthly,
    get_kpis,
    get_neto_breakdown_by_orders,
    get_needs_invoice_orders,
    get_pending_sp_orders_details,
    get_pending_sp_orders_summary,
    get_refund_total_amount,
    get_refund_top_categories,
    get_refund_top_customers,
    get_refund_top_items,
    get_unpaid_sp_orders_details,
    get_unpaid_sp_orders_summary,
    report_refund_items_category,
    get_sp_bank_monthly,
    get_top_customers,
    get_top_products,
    get_unmatched_orders_list,
    get_unpicked_category_totals,
    get_unpicked_customer_groups,
    get_unpicked_orders_list,
    get_unpicked_rows,
    get_unpicked_stats,
    get_unpicked_top_items,
)


def init_db(conn: sqlite3.Connection) -> None:
    _init_db(conn, SCHEMA_SQL)


def hash_password(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


_HTTP_OPENER = None
_HTTP_COOKIE_JAR = None


def _http_headers(extra: dict | None = None) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "bs-BA,bs;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }
    if extra:
        headers.update(extra)
    return headers


def _get_http_opener():
    global _HTTP_OPENER, _HTTP_COOKIE_JAR
    if _HTTP_OPENER is not None:
        return _HTTP_OPENER
    import http.cookiejar
    import urllib.request

    _HTTP_COOKIE_JAR = http.cookiejar.CookieJar()
    _HTTP_OPENER = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(_HTTP_COOKIE_JAR)
    )
    return _HTTP_OPENER


def http_request(
    url: str,
    method: str = "GET",
    data: bytes | None = None,
    headers: dict | None = None,
    timeout: int = 20,
):
    import urllib.request

    req = urllib.request.Request(
        url, data=data, method=method, headers=_http_headers(headers)
    )
    opener = _get_http_opener()
    return opener.open(req, timeout=timeout)


def fetch_cbbh_rsd_rate(
    url: str = "https://www.cbbh.ba/CurrencyExchange/",
) -> float | None:
    rate, _ = fetch_cbbh_rsd_rate_debug(url)
    return rate


def fetch_cbbh_rsd_rate_debug(
    url: str = "https://www.cbbh.ba/CurrencyExchange/",
) -> tuple[float | None, str | None]:
    try:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        return None, f"HTTP greska: {exc}"

    rows = re.findall(r"<tr[^>]*>.*?</tr>", html, flags=re.S | re.I)
    row = None
    for candidate in rows:
        if 'currcircle">RSD</div>' in candidate and "<td" in candidate:
            row = candidate
            break
    if not row:
        return None, "RSD red nije pronadjen u HTML-u."
    middle_match = re.search(r"middle-column[^>]*>\s*([0-9.,]+)\s*<", row)
    units_match = re.findall(r"tbl-smaller[^>]*tbl-center[^>]*>\s*(\d+)\s*<", row)
    if not middle_match:
        return None, "Nedostaje middle vrijednost u RSD redu."
    if not units_match:
        return None, "Nedostaje units vrijednost u RSD redu."
    try:
        units = float(units_match[0])
        middle = float(middle_match.group(1).replace(",", "."))
        if units <= 0:
            return None, "Units je 0 ili negativan."
        return middle / units, None
    except (TypeError, ValueError) as exc:
        return None, f"Parse greska: {exc}"


def start_import(
    conn: sqlite3.Connection, source: str, path: Path, row_count: int
) -> int | None:
    digest = file_hash(path)
    existing = conn.execute(
        "SELECT id FROM import_runs WHERE file_hash = ?",
        (digest,),
    ).fetchone()
    if existing:
        return None
    cur = conn.execute(
        "INSERT INTO import_runs (source, filename, file_hash, imported_at, row_count) "
        "VALUES (?, ?, ?, datetime('now'), ?)",
        (source, str(path), digest, row_count),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_or_create_order(
    conn: sqlite3.Connection, sp_order_no: str, values: dict
) -> tuple[int, bool]:
    row = conn.execute(
        "SELECT id FROM orders WHERE sp_order_no = ?",
        (sp_order_no,),
    ).fetchone()
    if row:
        return int(row[0]), False
    cols = ", ".join(values.keys())
    placeholders = ", ".join("?" for _ in values)
    cur = conn.execute(
        f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
        tuple(values.values()),
    )
    return int(cur.lastrowid), True


def add_status_history(
    conn: sqlite3.Connection, order_id: int, status: str, status_at: str, source: str
) -> None:
    conn.execute(
        "INSERT INTO order_status_history (order_id, status, status_at, source) "
        "VALUES (?, ?, ?, ?)",
        (order_id, status, status_at, source),
    )


def append_reject(
    rejects: list | None,
    source: str,
    file_name: str,
    row_index: int | None,
    reason: str,
    details: str,
) -> None:
    if rejects is None:
        return
    rejects.append(
        {
            "source": source,
            "file": file_name,
            "row_index": row_index,
            "reason": reason,
            "details": details,
        }
    )


def maybe_mark_delivered_from_payment(
    conn: sqlite3.Connection, sp_order_no: str
) -> None:
    row = conn.execute(
        "SELECT id, status FROM orders WHERE sp_order_no = ?",
        (sp_order_no,),
    ).fetchone()
    if not row:
        return
    order_id, status = int(row[0]), str(row[1] or "")
    if status.lower() in {"poslato", "poslano"}:
        conn.execute(
            "UPDATE orders SET status = ? WHERE id = ?",
            ("Isporu\u010deno", order_id),
        )
        add_status_history(conn, order_id, "Isporu\u010deno", "", "SP-Uplate")


def import_sp_orders(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_sp_orders(
        conn,
        path,
        rejects,
        col=COL,
        sheet_orders=SHEET_SP_ORDERS,
        file_hash=file_hash,
        compute_customer_key=compute_customer_key,
        set_app_state=set_app_state,
    )


def import_sp_payments(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_sp_payments(
        conn,
        path,
        rejects,
        col=COL,
        sheet_payments=SHEET_SP_PAYMENTS,
        file_hash=file_hash,
    )


def import_sp_returns(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_sp_returns(
        conn,
        path,
        rejects,
        col=COL,
        sheet_orders=SHEET_SP_ORDERS,
        file_hash=file_hash,
    )


def import_minimax(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_minimax(
        conn,
        path,
        rejects,
        col=COL,
        sheet_minimax=SHEET_MINIMAX,
        file_hash=file_hash,
        apply_storno=apply_storno,
    )


def import_minimax_items(conn: sqlite3.Connection, path: Path) -> None:
    _import_minimax_items(conn, path, sheet_minimax=SHEET_MINIMAX, file_hash=file_hash)


def import_sp_prijemi(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    if path.is_dir():
        _import_sp_prijemi_folder(conn, path, rejects, file_hash=file_hash)
        return
    _import_sp_prijem(conn, path, rejects, file_hash=file_hash)


def import_kartice_events(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_kartice_events_csv(conn, path, rejects, file_hash=file_hash)


def normalize_date(value) -> date | None:
    if value is None or value == "":
        return None
    text = str(value)
    dayfirst = "." in text and "-" not in text
    ts = pd.to_datetime(value, errors="coerce", dayfirst=dayfirst)
    if pd.isna(ts):
        return None
    return ts.date()


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return "".join(
        ch.lower() for ch in str(value) if ch.isalnum() or ch.isspace()
    ).strip()


def normalize_text_loose(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch for ch in text if ch.isalnum() or ch.isspace()).strip()


def _levenshtein_leq_n(a: str, b: str, max_dist: int) -> bool:
    if a == b:
        return True
    if abs(len(a) - len(b)) > max_dist:
        return False
    if not a or not b:
        return False
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        min_row = i
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            val = min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            )
            curr.append(val)
            if val < min_row:
                min_row = val
        if min_row > max_dist:
            return False
        prev = curr
    return prev[-1] <= max_dist


def fuzzy_contains(text: str, pattern: str, max_dist: int = 3) -> bool:
    if not text or not pattern:
        return False
    if pattern in text:
        return True
    t_len = len(text)
    p_len = len(pattern)
    min_len = max(1, p_len - max_dist)
    max_len = p_len + max_dist
    for win_len in range(min_len, max_len + 1):
        if win_len > t_len:
            break
        for i in range(0, t_len - win_len + 1):
            chunk = text[i : i + win_len]
            if _levenshtein_leq_n(chunk, pattern, max_dist):
                return True
    return False


def extract_invoice_no_from_text(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    match = re.search(r"\bSP-MM-\d+\b", text, flags=re.I)
    if match:
        val = match.group(0)
        digits = re.sub(r"\D", "", val)
        return val, digits or None
    match = re.search(r"\b\d{8,12}\b", text)
    if match:
        val = match.group(0)
        return val, val
    return None, None


def classify_refund_reason(purpose: str | None) -> str | None:
    text = normalize_text_loose(purpose)
    if not text:
        return None
    patterns = [
        ("reklamirana roba povrat sredstava", "reklamirana_roba_povrat_sredstava"),
        ("reklamacija robe povrat sredstava", "reklamacija_robe_povrat_sredstava"),
        (
            "povrat kupljene robe povrat sredstava",
            "povrat_kupljene_robe_povrat_sredstava",
        ),
        ("povrat robe storno racuna", "povrat_robe_storno_racuna"),
        ("povrat robe storno", "povrat_robe_storno"),
        ("storno racuna", "storno_racuna"),
        ("povrat robe", "povrat_robe"),
    ]
    for pattern, reason in patterns:
        if fuzzy_contains(text, pattern, max_dist=3):
            return reason
    return None


def invoice_digits(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\D", "", str(value))


def amount_exact_strict(a, b) -> bool:
    if a is None or b is None:
        return False
    try:
        return round(float(a), 2) == round(float(b), 2)
    except (TypeError, ValueError):
        return False


def name_exact_strict(a: str | None, b: str | None) -> bool:
    return normalize_text(a) == normalize_text(b)


def _levenshtein_leq_one(a: str, b: str) -> bool:
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        mismatches = 0
        for ch1, ch2 in zip(a, b):
            if ch1 != ch2:
                mismatches += 1
                if mismatches > 1:
                    return False
        return True
    if len(a) > len(b):
        a, b = b, a
    i = 0
    j = 0
    skips = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            skips += 1
            if skips > 1:
                return False
            j += 1
    return True


def name_distance_ok(a: str | None, b: str | None, max_distance: int = 1) -> bool:
    if max_distance != 1:
        raise ValueError("Only max_distance=1 is supported.")
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if not a_norm or not b_norm:
        return False
    a_parts = [p for p in a_norm.split() if p]
    b_parts = [p for p in b_norm.split() if p]
    if not a_parts or not b_parts:
        return False
    a_first = a_parts[0]
    a_last = a_parts[-1] if len(a_parts) > 1 else a_parts[0]
    b_first = b_parts[0]
    b_last = b_parts[-1] if len(b_parts) > 1 else b_parts[0]
    return _levenshtein_leq_one(a_first, b_first) and _levenshtein_leq_one(
        a_last, b_last
    )


def is_cancelled_status(status: str | None) -> bool:
    text = normalize_text(status)
    return "otkazan" in text


def is_in_progress_status(status: str | None) -> bool:
    text = normalize_text(status)
    return "u obradi" in text


def is_unpicked_status(status: str | None) -> bool:
    text = normalize_text_loose(status)
    return text.startswith("vrac")


def normalize_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return ""
    if digits.startswith("3810"):
        digits = "0" + digits[4:]
    elif digits.startswith("381"):
        digits = "0" + digits[3:]
    if digits.startswith("0"):
        return digits
    if digits.startswith("6") and 8 <= len(digits) <= 10:
        return "0" + digits
    return digits


def compute_customer_key(
    phone: str | None, email: str | None, name: str | None, city: str | None
) -> str:
    phone_key = normalize_phone(phone)
    if not phone_key and email:
        email_text = str(email)
        if "@" not in email_text:
            phone_key = normalize_phone(email_text)
    if phone_key:
        return f"phone:{phone_key}"
    email_key = normalize_text(email) if email and "@" in str(email) else ""
    if email_key:
        return f"email:{email_key}"
    name_key = normalize_text(name)
    city_key = normalize_text(city)
    if name_key or city_key:
        return f"name:{name_key}|city:{city_key}"
    return ""


def parse_tracking_time(value: str | None) -> str | None:
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _status_has(status: str | None, needle: str) -> bool:
    if not status or not needle:
        return False
    return needle in normalize_text_loose(status)


def analyze_tracking_history(
    history: list[dict],
) -> tuple[list[tuple[str | None, str | None]], dict]:
    events = []
    for entry in history:
        status_time = parse_tracking_time(entry.get("statusTime"))
        status_value = entry.get("statusValue")
        events.append((status_time, status_value))
    events_sorted = sorted(
        events,
        key=lambda x: (x[0] or "9999-12-31 23:59:59"),
    )

    received_at = None
    first_out_for_delivery_at = None
    returned_at = None
    delivery_attempts = 0
    reasons = []
    last_status = None
    last_status_at = None
    for status_time, status_value in events_sorted:
        text = normalize_text_loose(status_value)
        if status_value:
            last_status = status_value
            last_status_at = status_time
        if not received_at and _status_has(status_value, "preuzeta od posiljaoca"):
            received_at = status_time
        if _status_has(status_value, "zaduzena za isporuku"):
            delivery_attempts += 1
            if not first_out_for_delivery_at:
                first_out_for_delivery_at = status_time
        if "pokusaj isporuke" in text or "ponovni pokusaj" in text:
            delivery_attempts += 1
        if _status_has(status_value, "vracena posiljaocu") or _status_has(
            status_value, "vraca se posiljaocu"
        ):
            returned_at = status_time
        if any(
            marker in text
            for marker in [
                "telefon",
                "netacan",
                "nema nikoga",
                "nema nikog",
                "nema na adresi",
                "odbij",
                "odbio",
                "nepoznat",
                "pogresna adresa",
                "adresa",
                "neuspes",
                "bezuspes",
                "nije dostup",
                "ne moze",
                "nepostojec",
                "neispravan",
            ]
        ):
            if status_value and status_value not in reasons:
                reasons.append(status_value)

    days_to_first_attempt = None
    if received_at and first_out_for_delivery_at:
        try:
            dt_received = pd.to_datetime(received_at)
            dt_first = pd.to_datetime(first_out_for_delivery_at)
            days_to_first_attempt = (dt_first - dt_received).total_seconds() / 86400.0
        except Exception:
            days_to_first_attempt = None

    has_attempt_before_return = 0
    if returned_at:
        try:
            dt_returned = pd.to_datetime(returned_at)
            for status_time, status_value in events_sorted:
                if not status_time:
                    continue
                if _status_has(status_value, "zaduzena za isporuku") or _status_has(
                    status_value, "pokusaj isporuke"
                ):
                    dt = pd.to_datetime(status_time)
                    if dt <= dt_returned:
                        has_attempt_before_return = 1
                        break
        except Exception:
            has_attempt_before_return = 0

    anomalies = []
    text_reasons = normalize_text_loose(" ".join(reasons))
    if "telefon" in text_reasons and "nema nikoga" in text_reasons:
        anomalies.append("Nelogican slijed: telefon netacan + nema nikoga")
    if returned_at and not has_attempt_before_return:
        anomalies.append("Vracena bez pokusaja isporuke")
    if not returned_at:
        anomalies.append("Nema statusa vracena posiljaocu")

    summary = {
        "received_at": received_at,
        "first_out_for_delivery_at": first_out_for_delivery_at,
        "delivery_attempts": delivery_attempts,
        "failure_reasons": "; ".join(reasons),
        "returned_at": returned_at,
        "days_to_first_attempt": days_to_first_attempt,
        "has_attempt_before_return": has_attempt_before_return,
        "has_returned": 1 if returned_at else 0,
        "anomalies": "; ".join(anomalies),
        "last_status": last_status,
        "last_status_at": last_status_at,
    }
    return events_sorted, summary


def fetch_dexpress_tracking(tracking_code: str) -> tuple[dict | None, int | None]:
    if not tracking_code:
        return None, None
    import urllib.parse

    url = "https://www.dexpress.rs/rs/pracenje-posiljaka"
    form_data = {
        "ajax": "yes",
        "task": "search",
        "data[package_tracking_search]": tracking_code,
    }
    encoded = urllib.parse.urlencode(form_data).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.dexpress.rs",
        "Referer": f"https://www.dexpress.rs/rs/pracenje-posiljaka/{tracking_code}",
    }
    with http_request(
        url, method="POST", data=encoded, headers=headers, timeout=20
    ) as resp:
        status_code = getattr(resp, "status", None)
        payload = resp.read().decode("utf-8", errors="ignore")
    try:
        return json.loads(payload), status_code
    except json.JSONDecodeError:
        return None, status_code


def fetch_slanjepaketa_tracking(tracking_code: str) -> tuple[dict | None, int | None]:
    if not tracking_code:
        return None, None
    url = f"https://softver.slanjepaketa.rs/api/v1/product-orders/status-info/{tracking_code}"
    headers = {
        "Authorization": "External 7b028ded-ebe4-4d74-ae79-ff516a64a851",
        "Referer": f"https://www.slanjepaketa.rs/pracenje-posiljaka/{tracking_code}",
        "Origin": "https://www.slanjepaketa.rs",
    }
    with http_request(url, method="GET", headers=headers, timeout=20) as resp:
        status_code = getattr(resp, "status", None)
        payload = resp.read().decode("utf-8", errors="ignore")
    try:
        return json.loads(payload), status_code
    except json.JSONDecodeError:
        return None, status_code


def tracking_public_url(tracking_code: str) -> str:
    if tracking_code.upper().startswith("SPF"):
        return f"https://www.slanjepaketa.rs/pracenje-posiljaka/{tracking_code}"
    return f"https://www.dexpress.rs/rs/pracenje-posiljaka/{tracking_code}"


def save_tracking_result(
    conn: sqlite3.Connection,
    tracking_code: str,
    events: list[tuple[str | None, str | None]],
    summary: dict,
) -> None:
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        "INSERT OR IGNORE INTO tracking_events (tracking_code, status_time, status_value, fetched_at) "
        "VALUES (?, ?, ?, ?)",
        [(tracking_code, t, v, fetched_at) for t, v in events],
    )
    conn.execute(
        "INSERT INTO tracking_summary "
        "(tracking_code, received_at, first_out_for_delivery_at, delivery_attempts, "
        "failure_reasons, returned_at, days_to_first_attempt, has_attempt_before_return, "
        "has_returned, anomalies, last_status, last_status_at, last_fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(tracking_code) DO UPDATE SET "
        "received_at=excluded.received_at, "
        "first_out_for_delivery_at=excluded.first_out_for_delivery_at, "
        "delivery_attempts=excluded.delivery_attempts, "
        "failure_reasons=excluded.failure_reasons, "
        "returned_at=excluded.returned_at, "
        "days_to_first_attempt=excluded.days_to_first_attempt, "
        "has_attempt_before_return=excluded.has_attempt_before_return, "
        "has_returned=excluded.has_returned, "
        "anomalies=excluded.anomalies, "
        "last_status=excluded.last_status, "
        "last_status_at=excluded.last_status_at, "
        "last_fetched_at=excluded.last_fetched_at",
        (
            tracking_code,
            summary.get("received_at"),
            summary.get("first_out_for_delivery_at"),
            summary.get("delivery_attempts"),
            summary.get("failure_reasons"),
            summary.get("returned_at"),
            summary.get("days_to_first_attempt"),
            summary.get("has_attempt_before_return"),
            summary.get("has_returned"),
            summary.get("anomalies"),
            summary.get("last_status"),
            summary.get("last_status_at"),
            fetched_at,
        ),
    )
    conn.commit()


def log_tracking_request(
    tracking_code: str,
    url: str,
    status_code: int | None,
    latency_ms: int | None,
    result: str,
    error: str | None = None,
) -> None:
    log_dir = Path("exports")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "tracking-log.csv"
    file_exists = log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            writer.writerow(
                [
                    "timestamp",
                    "tracking_code",
                    "url",
                    "status_code",
                    "latency_ms",
                    "result",
                    "error",
                ]
            )
        writer.writerow(
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                tracking_code,
                url,
                status_code if status_code is not None else "",
                latency_ms if latency_ms is not None else "",
                result,
                error or "",
            ]
        )


_APP_LOG_LOCK = threading.Lock()


def log_app_event(source: str, message: str, **fields) -> None:
    log_dir = APP_DIR / "exports"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app-run.log"
    payload = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "message": message,
        **fields,
    }
    line = json.dumps(payload, ensure_ascii=False)
    with _APP_LOG_LOCK:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def log_app_error(source: str, message: str) -> None:
    log_dir = APP_DIR / "exports"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app-errors.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _APP_LOG_LOCK:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] {source}: {message}\n")
    try:
        log_app_event(source, message, level="error")
    except Exception:
        pass


def get_latest_task_progress(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT task, total, processed, updated_at "
        "FROM task_progress "
        "ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {
        "task": row[0],
        "total": int(row[1]),
        "processed": int(row[2]),
        "updated_at": row[3],
    }


def load_review_samples(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT m.id, m.score, o.id, o.sp_order_no, o.customer_name, o.picked_up_at, "
        "i.id, i.number, i.customer_name, i.date, i.amount_due "
        "FROM invoice_matches m "
        "JOIN orders o ON o.id = m.order_id "
        "JOIN invoices i ON i.id = m.invoice_id "
        "WHERE m.status = 'review' "
        "ORDER BY m.score DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        return []
    order_ids = [int(r[2]) for r in rows]
    totals = build_order_net_map(conn, order_ids)
    result = []
    for (
        match_id,
        score,
        order_id,
        sp_order_no,
        order_name,
        order_date,
        invoice_id,
        invoice_no,
        invoice_name,
        invoice_date,
        amount_due,
    ) in rows:
        result.append(
            {
                "match_id": match_id,
                "score": score,
                "order_id": order_id,
                "sp_order_no": sp_order_no,
                "order_name": order_name,
                "order_date": order_date,
                "order_amount": totals.get(int(order_id), 0.0),
                "invoice_id": invoice_id,
                "invoice_no": invoice_no,
                "invoice_name": invoice_name,
                "invoice_date": invoice_date,
                "invoice_amount": float(amount_due or 0),
            }
        )
    return result


def load_all_match_samples(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT m.id, m.score, o.id, o.sp_order_no, o.customer_name, o.picked_up_at, "
        "i.id, i.number, i.customer_name, i.date, i.amount_due "
        "FROM invoice_matches m "
        "JOIN orders o ON o.id = m.order_id "
        "JOIN invoices i ON i.id = m.invoice_id "
        "ORDER BY m.score DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        return []
    order_ids = [int(r[2]) for r in rows]
    totals = build_order_net_map(conn, order_ids)
    result = []
    for (
        match_id,
        score,
        order_id,
        sp_order_no,
        order_name,
        order_date,
        invoice_id,
        invoice_no,
        invoice_name,
        invoice_date,
        amount_due,
    ) in rows:
        result.append(
            {
                "match_id": match_id,
                "score": score,
                "order_id": order_id,
                "sp_order_no": sp_order_no,
                "order_name": order_name,
                "order_date": order_date,
                "order_amount": totals.get(int(order_id), 0.0),
                "invoice_id": invoice_id,
                "invoice_no": invoice_no,
                "invoice_name": invoice_name,
                "invoice_date": invoice_date,
                "invoice_amount": float(amount_due or 0),
            }
        )
    return result


def _should_skip_tracking(
    conn: sqlite3.Connection,
    tracking_code: str,
    fast_hours: int = 6,
    slow_hours: int = 24,
) -> bool:
    row = conn.execute(
        "SELECT last_fetched_at, has_returned, delivery_attempts, last_status "
        "FROM tracking_summary WHERE tracking_code = ?",
        (tracking_code,),
    ).fetchone()
    if not row:
        return False
    last_fetched_at, has_returned, delivery_attempts, last_status = row
    if has_returned:
        return True
    if last_status and _status_has(last_status, "isporucena"):
        return True
    if not last_fetched_at:
        return False
    try:
        last_dt = pd.to_datetime(last_fetched_at)
    except Exception:
        return False
    if pd.isna(last_dt):
        return False
    delta_hours = (datetime.now() - last_dt).total_seconds() / 3600.0
    if delivery_attempts and delivery_attempts > 0:
        return delta_hours < fast_hours
    return delta_hours < slow_hours


def update_unpicked_tracking(
    conn: sqlite3.Connection,
    batch_size: int = 20,
    min_delay: int = 8,
    max_delay: int = 12,
    batch_pause_min: int = 180,
    batch_pause_max: int = 300,
    backoff_min: int = 120,
    backoff_max: int = 600,
    progress_task: str | None = None,
    force_refresh: bool = False,
) -> int:
    import random
    import time
    import urllib.error

    rows = get_unpicked_rows(conn)
    tracking_codes = sorted({str(r[9]).strip() for r in rows if len(r) > 9 and r[9]})
    if not tracking_codes:
        return 0
    total = len(tracking_codes)
    scanned = 0
    if progress_task:
        set_task_progress(conn, progress_task, total)
        update_task_progress(conn, progress_task, 0)
    processed = 0
    for idx, code in enumerate(tracking_codes, start=1):
        url = tracking_public_url(code)
        if not force_refresh and _should_skip_tracking(conn, code):
            log_tracking_request(code, url, None, None, "skipped_cache", None)
            scanned += 1
            if progress_task:
                update_task_progress(conn, progress_task, scanned)
            continue
        attempts = 0
        max_retries = 2
        while True:
            start_time = time.time()
            try:
                if code.upper().startswith("SPF"):
                    data, status_code = fetch_slanjepaketa_tracking(code)
                    if not data or "notes" not in data:
                        latency_ms = int((time.time() - start_time) * 1000)
                        log_tracking_request(
                            code, url, status_code, latency_ms, "no_data", None
                        )
                        break
                    history = [
                        {
                            "statusTime": note.get("date"),
                            "statusValue": note.get("note"),
                        }
                        for note in (data.get("notes") or [])
                    ]
                else:
                    data, status_code = fetch_dexpress_tracking(code)
                    if not data or not data.get("flag"):
                        latency_ms = int((time.time() - start_time) * 1000)
                        log_tracking_request(
                            code, url, status_code, latency_ms, "no_data", None
                        )
                        break
                    history = data.get("historyStatuses") or []
                events, summary = analyze_tracking_history(history)
                save_tracking_result(conn, code, events, summary)
                processed += 1
                latency_ms = int((time.time() - start_time) * 1000)
                log_tracking_request(code, url, status_code, latency_ms, "ok", None)
                break
            except urllib.error.HTTPError as exc:
                latency_ms = int((time.time() - start_time) * 1000)
                log_tracking_request(
                    code, url, exc.code, latency_ms, "http_error", str(exc)
                )
                if exc.code in (403, 429):
                    time.sleep(random.uniform(backoff_min, backoff_max))
                    attempts += 1
                    if attempts <= max_retries:
                        continue
                    break
                raise
            except urllib.error.URLError as exc:
                latency_ms = int((time.time() - start_time) * 1000)
                log_tracking_request(code, url, None, latency_ms, "timeout", str(exc))
                time.sleep(random.uniform(backoff_min, backoff_max))
                attempts += 1
                if attempts <= max_retries:
                    continue
                break
            except Exception as exc:
                latency_ms = int((time.time() - start_time) * 1000)
                log_tracking_request(code, url, None, latency_ms, "error", str(exc))
                break

        scanned += 1
        if progress_task:
            update_task_progress(conn, progress_task, scanned)

        if idx % batch_size == 0:
            time.sleep(random.uniform(batch_pause_min, batch_pause_max))
        else:
            time.sleep(random.uniform(min_delay, max_delay))
    if progress_task:
        update_task_progress(conn, progress_task, total)
    return processed


def recompute_customer_keys(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        "SELECT id, phone, email, customer_name, city FROM orders"
    ).fetchall()
    updates = []
    for order_id, phone, email, name, city in rows:
        key = compute_customer_key(phone, email, name, city) or None
        updates.append((key, order_id))
    if updates:
        conn.executemany("UPDATE orders SET customer_key = ? WHERE id = ?", updates)
        conn.commit()
    return len(updates)


def ensure_customer_keys(conn: sqlite3.Connection) -> int:
    version = get_app_state(conn, "customer_key_version")
    if version == CUSTOMER_KEY_VERSION:
        return 0
    updated = recompute_customer_keys(conn)
    set_app_state(conn, "customer_key_version", CUSTOMER_KEY_VERSION)
    return updated


def extract_sp_order_no(note: str) -> str | None:
    if not note:
        return None
    match = re.search(r"\b(\d{4,})\b", note)
    if not match:
        return None
    return match.group(1)


def compute_order_component(max_val, min_val, sum_val) -> float | None:
    if max_val is None:
        return None
    if min_val is None:
        return float(max_val)
    try:
        max_f = float(max_val)
        min_f = float(min_val)
        sum_f = float(sum_val) if sum_val is not None else None
        if sum_f is not None and sum_f > max_f + 0.01:
            return sum_f
        if abs(max_f - min_f) < 0.01:
            return max_f
        return sum_f if sum_f is not None else max_f
    except (TypeError, ValueError):
        return None


def compute_order_amount(cod, addon, advance, addon_advance) -> float | None:
    if cod is None and addon is None and advance is None and addon_advance is None:
        return None
    base = float(cod or 0) + float(addon or 0)
    paid = float(advance or 0) + float(addon_advance or 0)
    return base - paid


def apply_percent(value, percent) -> float:
    val = to_float(value) or 0.0
    pct = to_float(percent)
    if pct is None:
        return val
    if pct < 0 or pct > 100:
        return val
    return val * (1 - pct / 100.0)


def apply_percent_chain(value, percents: list) -> float:
    val = to_float(value) or 0.0
    for pct_val in percents or []:
        pct = to_float(pct_val)
        if pct is None:
            continue
        if pct < 0 or pct > 100:
            continue
        val *= 1 - pct / 100.0
    return val


def build_order_net_map(
    conn: sqlite3.Connection, order_ids: list[int]
) -> dict[int, float]:
    if not order_ids:
        return {}
    net_map: dict[int, float] = {int(oid): 0.0 for oid in order_ids}
    chunk_size = 900
    for i in range(0, len(order_ids), chunk_size):
        chunk = order_ids[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            "SELECT order_id, qty, cod_amount, addon_cod, advance_amount, addon_advance, "
            "discount, extra_discount "
            f"FROM order_items WHERE order_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            order_id = int(row[0])
            qty = to_float(row[1]) or 0.0
            order_discount = row[7]
            item_discount = row[6]
            cod_unit = apply_percent_chain(row[2], [order_discount, item_discount])
            addon_unit = apply_percent_chain(row[3], [order_discount])
            cod = qty * cod_unit
            addon = qty * addon_unit
            advance = qty * (to_float(row[4]) or 0.0)
            addon_advance = qty * (to_float(row[5]) or 0.0)
            net_map[order_id] = (
                net_map.get(order_id, 0.0) + cod + addon - advance - addon_advance
            )
    return net_map


def is_no_value_order(cod, addon, advance, addon_advance) -> bool:
    vals = [cod, addon, advance, addon_advance]
    total = 0.0
    for val in vals:
        if val is None:
            continue
        try:
            total += abs(float(val))
        except (TypeError, ValueError):
            continue
    return total == 0.0


def score_match(order, invoice) -> int:
    score, _ = score_match_with_reasons(order, invoice)
    return score


def score_match_with_reasons(order, invoice) -> tuple[int, list[str]]:
    score = 0
    reasons = []
    order_date = normalize_date(order["picked_up_at"])
    inv_date = normalize_date(invoice["turnover"])
    if order_date and inv_date:
        delta = (inv_date - order_date).days
        if -10 <= delta <= 10:
            score += 30
            reasons.append("date-10+10")
    order_amount = order["amount"]
    inv_amount = invoice["amount_due"]
    if amount_exact_strict(order_amount, inv_amount):
        score += 40
        reasons.append("amount_exact")
    order_name = order.get("customer_name")
    invoice_name = invoice.get("customer_name")
    if name_exact_strict(order_name, invoice_name):
        score += 30
        reasons.append("name_exact")
    elif name_distance_ok(order_name, invoice_name):
        score += 30
        reasons.append("name_close")
    return score, reasons


def extract_invoice_number_from_basis(basis: str | None) -> str | None:
    if not basis:
        return None
    match = re.search(r"\b([A-Z]{2}-[A-Z]{2}-\d+)\b", basis)
    if not match:
        return None
    return match.group(1)


def extract_invoice_number_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\bSP-MM-\d+\b", text)
    if not match:
        return None
    return match.group(0)


def to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_text(elem, path: str) -> str:
    if elem is None:
        return ""
    child = elem.find(path)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def import_bank_xml(
    conn: sqlite3.Connection, path: Path, rejects: list | None = None
) -> None:
    _import_bank_xml(conn, path, rejects, file_hash=file_hash, to_float=to_float)


def apply_storno(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, amount_due, revenue, basis FROM invoices"
    ).fetchall()
    for row in rows:
        _, amount_due, revenue, basis = row
        amount_due_val = to_float(amount_due)
        revenue_val = to_float(revenue)
        is_storno = (amount_due_val is not None and amount_due_val < 0) or (
            revenue_val is not None and revenue_val < 0
        )
        if not is_storno:
            continue
        basis_no = extract_invoice_number_from_basis(str(basis or ""))
        if not basis_no:
            continue
        orig = conn.execute(
            "SELECT id, amount_due FROM invoices WHERE number = ?",
            (basis_no,),
        ).fetchone()
        if not orig:
            continue
        orig_id, orig_amount = int(orig[0]), to_float(orig[1])
        if orig_amount is None:
            continue
        storno_val = abs(amount_due_val or revenue_val or 0)
        if abs(orig_amount - storno_val) <= 2.0:
            new_open = 0.0
        else:
            new_open = max(0.0, orig_amount - storno_val)
        conn.execute(
            "UPDATE invoices SET open_amount = ? WHERE id = ?",
            (new_open, orig_id),
        )
        conn.execute(
            "INSERT OR REPLACE INTO invoice_storno ("
            "storno_invoice_id, original_invoice_id, storno_amount, "
            "original_amount, remaining_open, is_partial, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                row[0],
                orig_id,
                storno_val,
                orig_amount,
                new_open,
                1 if new_open > 0 else 0,
            ),
        )
    conn.commit()


def match_minimax(
    conn: sqlite3.Connection,
    auto_threshold: int = 70,
    review_threshold: int = 50,
    progress_task: str | None = None,
) -> None:
    conn.execute("DELETE FROM order_flags WHERE flag = 'needs_invoice'")
    orders = []
    rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.picked_up_at, o.created_at, "
        "o.phone, o.address, o.city, o.status, "
        "MAX(oi.cod_amount), MIN(oi.cod_amount), SUM(oi.cod_amount), "
        "MAX(oi.addon_cod), MIN(oi.addon_cod), SUM(oi.addon_cod), "
        "MAX(oi.advance_amount), MIN(oi.advance_amount), SUM(oi.advance_amount), "
        "MAX(oi.addon_advance), MIN(oi.addon_advance), SUM(oi.addon_advance), "
        "MAX(oi.discount), MIN(oi.discount), COUNT(oi.id) "
        "FROM orders o LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "GROUP BY o.id"
    ).fetchall()
    order_ids = [int(row[0]) for row in rows]
    net_map = build_order_net_map(conn, order_ids)

    def is_all_zero_values(
        max_cod, max_addon, max_adv, max_addon_adv, item_count: int
    ) -> bool:
        if item_count == 0:
            return True
        vals = [max_cod, max_addon, max_adv, max_addon_adv]
        for val in vals:
            try:
                if val is not None and abs(float(val)) > 0.0:
                    return False
            except (TypeError, ValueError):
                continue
        return True

    def is_all_discount_100(min_discount, max_discount, item_count: int) -> bool:
        if item_count == 0:
            return False
        try:
            return float(min_discount) == 100.0 and float(max_discount) == 100.0
        except (TypeError, ValueError):
            return False

    for row in rows:
        status = row[8]
        if is_cancelled_status(status) or is_in_progress_status(status):
            continue
        order_id = int(row[0])
        order_date = normalize_date(row[3] or row[4])
        amount = net_map.get(order_id)
        max_cod = row[9]
        max_addon = row[12]
        max_adv = row[15]
        max_addon_adv = row[18]
        max_discount = row[21]
        min_discount = row[22]
        item_count = int(row[23] or 0)
        if is_all_zero_values(max_cod, max_addon, max_adv, max_addon_adv, item_count):
            continue
        if is_all_discount_100(min_discount, max_discount, item_count):
            continue
        if amount is None:
            continue
        orders.append(
            {
                "id": order_id,
                "sp_order_no": str(row[1]),
                "customer_name": row[2],
                "picked_up_at": row[3] or row[4],
                "phone": row[5],
                "address": row[6],
                "city": row[7],
                "amount": amount,
                "picked_up_date": order_date,
            }
        )

    invoices = []
    inv_rows = conn.execute(
        "SELECT id, number, customer_name, turnover, amount_due, note, analytics, account "
        "FROM invoices "
        "WHERE id NOT IN (SELECT invoice_id FROM invoice_matches)"
    ).fetchall()
    for row in inv_rows:
        invoices.append(
            {
                "id": int(row[0]),
                "number": row[1],
                "customer_name": row[2],
                "turnover": row[3],
                "amount_due": row[4],
                "note": row[5],
                "analytics": row[6],
                "account": row[7],
                "turnover_date": normalize_date(row[3]),
            }
        )

    total_steps = len(invoices) + len(orders) * 2 + 2
    processed_steps = 0
    last_progress = 0
    progress_every = 10
    if progress_task:
        set_task_progress(conn, progress_task, total_steps)

    def maybe_update_progress():
        nonlocal last_progress
        if not progress_task:
            return
        if (
            processed_steps - last_progress >= progress_every
            or processed_steps >= total_steps
        ):
            update_task_progress(conn, progress_task, processed_steps)
            last_progress = processed_steps

    processed_steps += 1
    maybe_update_progress()

    def amount_exact(a, b) -> bool:
        return amount_exact_strict(a, b)

    def name_exact(a, b) -> bool:
        return name_exact_strict(a, b)

    def name_close(a, b) -> bool:
        return name_distance_ok(a, b, max_distance=1)

    def date_in_window(d1, d2, days_back: int = 10, days_forward: int = 10) -> bool:
        if not d1 or not d2:
            return False
        delta = (d2 - d1).days
        return -days_back <= delta <= days_forward

    orders_by_date = {}
    orders_no_date = []
    for order in orders:
        od = order.get("picked_up_date")
        if od:
            orders_by_date.setdefault(od, []).append(order)
        else:
            orders_no_date.append(order)

    invoices_by_date = {}
    invoices_no_date = []
    for inv in invoices:
        idate = inv.get("turnover_date")
        if idate:
            invoices_by_date.setdefault(idate, []).append(inv)
        else:
            invoices_no_date.append(inv)

    def candidate_orders_for_invoice(inv):
        idate = inv.get("turnover_date")
        if idate:
            date_candidates = []
            for offset in range(-10, 11):
                date_candidates.extend(
                    orders_by_date.get(idate + timedelta(days=offset), [])
                )
            date_candidates.extend(orders_no_date)
        else:
            date_candidates = orders
        filtered = [
            o
            for o in date_candidates
            if amount_exact(o.get("amount"), inv.get("amount_due"))
            and date_in_window(o.get("picked_up_date"), idate, 7)
        ]
        if filtered:
            return filtered
        return date_candidates

    def candidate_invoices_for_order(order, pool):
        odate = order.get("picked_up_date")
        if odate:
            date_candidates = []
            for offset in range(-10, 11):
                date_candidates.extend(
                    invoices_by_date.get(odate + timedelta(days=offset), [])
                )
            date_candidates.extend(invoices_no_date)
        else:
            date_candidates = pool
        filtered = [
            inv
            for inv in date_candidates
            if amount_exact(order.get("amount"), inv.get("amount_due"))
            and date_in_window(odate, inv.get("turnover_date"), 7)
        ]
        if filtered:
            return filtered
        return date_candidates

    used_orders = set()
    matched_invoice_ids = {
        int(row[0])
        for row in conn.execute("SELECT invoice_id FROM invoice_matches").fetchall()
    }

    def select_best_invoice(order, candidates):
        odate = order.get("picked_up_date")
        best = None
        best_key = None
        for inv in candidates:
            idate = inv.get("turnover_date")
            if odate and idate:
                delta = abs((idate - odate).days)
            else:
                delta = 9999
            key = (delta, inv["id"])
            if best_key is None or key < best_key:
                best_key = key
                best = inv
        return best

    def find_candidates(order, name_check):
        odate = order.get("picked_up_date")
        if odate:
            date_candidates = []
            for offset in range(-10, 11):
                date_candidates.extend(
                    invoices_by_date.get(odate + timedelta(days=offset), [])
                )
            date_candidates.extend(invoices_no_date)
        else:
            date_candidates = invoices
        candidates = [
            inv
            for inv in date_candidates
            if inv["id"] not in matched_invoice_ids
            and name_check(order.get("customer_name"), inv.get("customer_name"))
            and amount_exact(order.get("amount"), inv.get("amount_due"))
            and (
                not order.get("picked_up_date")
                or date_in_window(order.get("picked_up_date"), inv.get("turnover_date"))
            )
        ]
        return candidates

    # Step 1: exact name + exact amount (+/-10 days).
    for order in orders:
        if order["id"] in used_orders:
            continue
        processed_steps += 1
        maybe_update_progress()
        candidates = find_candidates(order, name_exact)
        if not candidates:
            continue
        if order.get("picked_up_date") is None and len(candidates) != 1:
            continue
        inv = select_best_invoice(order, candidates)
        if not inv:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO invoice_matches "
            "(order_id, invoice_id, score, status, method, matched_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (order["id"], inv["id"], 100, "auto", "exact"),
        )
        used_orders.add(order["id"])
        matched_invoice_ids.add(inv["id"])

    # Step 2: name within 1 char + exact amount (+/-10 days).
    for order in orders:
        if order["id"] in used_orders:
            continue
        processed_steps += 1
        maybe_update_progress()
        candidates = find_candidates(order, name_close)
        if not candidates:
            continue
        if order.get("picked_up_date") is None and len(candidates) != 1:
            continue
        inv = select_best_invoice(order, candidates)
        if not inv:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO invoice_matches "
            "(order_id, invoice_id, score, status, method, matched_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (order["id"], inv["id"], 90, "auto", "close-name"),
        )
        used_orders.add(order["id"])
        matched_invoice_ids.add(inv["id"])

    # Store top candidates for unmatched orders.
    processed_steps += 1
    maybe_update_progress()

    remaining_invoices = [
        inv for inv in invoices if inv["id"] not in matched_invoice_ids
    ]
    for order in orders:
        if order["id"] in used_orders:
            continue
        processed_steps += 1
        maybe_update_progress()
        candidates = []
        for inv in candidate_invoices_for_order(order, remaining_invoices):
            score, reasons = score_match_with_reasons(order, inv)
            if score > 0:
                candidates.append((score, inv["id"], ",".join(reasons)))
        candidates.sort(reverse=True)
        top = candidates[:3]
        if not top:
            conn.execute(
                "INSERT OR IGNORE INTO order_flags (order_id, flag, note, created_at) "
                "VALUES (?, 'needs_invoice', NULL, datetime('now'))",
                (order["id"],),
            )
            continue
        conn.execute(
            "DELETE FROM order_flags WHERE order_id = ? AND flag = 'needs_invoice'",
            (order["id"],),
        )
        conn.execute(
            "DELETE FROM invoice_candidates WHERE order_id = ?", (order["id"],)
        )
        for score, inv_id, detail in top:
            conn.execute(
                "INSERT OR IGNORE INTO invoice_candidates "
                "(order_id, invoice_id, score, detail, method, created_at) "
                "VALUES (?, ?, ?, ?, ?, datetime('now'))",
                (order["id"], inv_id, score, detail, "fuzzy"),
            )

    if progress_task:
        update_task_progress(conn, progress_task, total_steps)
    conn.commit()


def list_review_matches(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT m.id, m.score, o.sp_order_no, o.customer_name, o.picked_up_at, "
        "i.number, i.customer_name, i.turnover, i.amount_due "
        "FROM invoice_matches m "
        "JOIN orders o ON o.id = m.order_id "
        "JOIN invoices i ON i.id = m.invoice_id "
        "WHERE m.status = 'review' "
        "ORDER BY m.score DESC"
    ).fetchall()
    if not rows:
        print("Nema match-eva za pregled.")
        if return_rows:
            return [
                "match_id",
                "score",
                "sp_order_no",
                "order_name",
                "order_date",
                "invoice_no",
                "invoice_name",
                "invoice_date",
                "amount_due",
            ], []
        return
    for row in rows:
        print(
            f"match_id={row[0]} score={row[1]} "
            f"sp_order_no={row[2]} order_name={row[3]} order_date={row[4]} "
            f"invoice_no={row[5]} invoice_name={row[6]} invoice_date={row[7]} "
            f"amount_due={row[8]}"
        )
    if return_rows:
        return [
            "match_id",
            "score",
            "sp_order_no",
            "order_name",
            "order_date",
            "invoice_no",
            "invoice_name",
            "invoice_date",
            "amount_due",
        ], rows


def confirm_match(conn: sqlite3.Connection, match_id: int) -> None:
    row = conn.execute(
        "SELECT invoice_id FROM invoice_matches WHERE id = ?",
        (match_id,),
    ).fetchone()
    if not row:
        print("Match nije pronadjen.")
        return
    invoice_id = int(row[0])
    conn.execute(
        "UPDATE invoice_matches SET status = 'auto' WHERE id = ?",
        (match_id,),
    )
    conn.execute(
        "UPDATE invoices SET open_amount = 0, "
        "payment_amount = COALESCE(payment_amount, amount_due) "
        "WHERE id = ?",
        (invoice_id,),
    )
    conn.execute(
        "INSERT INTO action_log (action, ref_type, ref_id, note, created_at) "
        "VALUES ('confirm_match', 'invoice_match', ?, NULL, datetime('now'))",
        (match_id,),
    )
    conn.commit()
    print(f"Match potvrden: {match_id}.")


def apply_review_decisions(conn: sqlite3.Connection, path: Path) -> None:
    if path.suffix.lower() == ".xlsx":
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    if "match_id" not in df.columns:
        print("Fajl mora imati kolonu 'match_id'.")
        return
    if "confirm" not in df.columns and "needs_invoice" not in df.columns:
        print("Fajl mora imati kolonu 'confirm' ili 'needs_invoice' (1/0).")
        return
    confirmed = []
    needs_invoice = []
    if "confirm" in df.columns:
        confirmed = df[df["confirm"] == 1]["match_id"].dropna().astype(int).tolist()
    if "needs_invoice" in df.columns:
        needs_invoice = (
            df[df["needs_invoice"] == 1]["match_id"].dropna().astype(int).tolist()
        )
    if not confirmed:
        print("Nema potvrdenih match-eva.")
    for match_id in confirmed:
        confirm_match(conn, int(match_id))
    if needs_invoice:
        for match_id in needs_invoice:
            conn.execute(
                "UPDATE invoice_matches SET status = 'needs_invoice' WHERE id = ?",
                (int(match_id),),
            )
            conn.execute(
                "INSERT INTO action_log (action, ref_type, ref_id, note, created_at) "
                "VALUES ('needs_invoice', 'invoice_match', ?, NULL, datetime('now'))",
                (int(match_id),),
            )
        conn.commit()
    print(f"Potvrdeno ukupno: {len(confirmed)}")
    if needs_invoice:
        print(f"Oznaceno 'needs_invoice': {len(needs_invoice)}")


def report_unmatched_orders(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, o.status "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND o.id NOT IN (SELECT order_id FROM order_flags WHERE flag = 'needs_invoice') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%obradi%') "
        "AND date(substr(COALESCE(o.picked_up_at, o.created_at), 1, 10)) <= date('now', '-3 day') "
        "GROUP BY o.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") != 0 "
        "ORDER BY o.picked_up_at"
    ).fetchall()
    print(f"Neuparene narudzbe: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["sp_order_no", "customer_name", "picked_up_at", "status"], rows


def report_conflicts(conn: sqlite3.Connection, return_rows: bool = False):
    order_rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.picked_up_at, o.created_at, "
        "MAX(oi.cod_amount), MIN(oi.cod_amount), SUM(oi.cod_amount), "
        "MAX(oi.addon_cod), MIN(oi.addon_cod), SUM(oi.addon_cod), "
        "MAX(oi.advance_amount), MIN(oi.advance_amount), SUM(oi.advance_amount), "
        "MAX(oi.addon_advance), MIN(oi.addon_advance), SUM(oi.addon_advance) "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "GROUP BY o.id"
    ).fetchall()

    orders_by_date = {}
    orders = []
    for row in order_rows:
        cod = compute_order_component(row[5], row[6], row[7])
        addon = compute_order_component(row[8], row[9], row[10])
        advance = compute_order_component(row[11], row[12], row[13])
        addon_advance = compute_order_component(row[14], row[15], row[16])
        amount = compute_order_amount(cod, addon, advance, addon_advance)
        date_val = normalize_date(row[3] or row[4])
        order = {
            "id": int(row[0]),
            "sp_order_no": row[1],
            "name": row[2],
            "date": date_val,
            "amount": amount,
        }
        orders.append(order)
        if date_val:
            orders_by_date.setdefault(date_val, []).append(order)

    invoice_rows = conn.execute(
        "SELECT i.id, i.number, i.customer_name, i.turnover, i.amount_due, "
        "o.id, o.sp_order_no, o.customer_name "
        "FROM invoice_matches m "
        "JOIN invoices i ON i.id = m.invoice_id "
        "JOIN orders o ON o.id = m.order_id"
    ).fetchall()

    results = []
    for row in invoice_rows:
        inv_id = int(row[0])
        inv_no = row[1]
        inv_name = row[2]
        inv_date = normalize_date(row[3])
        inv_amount = row[4]
        matched_order_id = int(row[5])
        matched_sp = row[6]
        matched_name = row[7]

        if inv_date is None or inv_amount is None:
            continue
        candidates = []
        for offset in range(-10, 11):
            candidates.extend(orders_by_date.get(inv_date + timedelta(days=offset), []))
        for cand in candidates:
            if cand["id"] == matched_order_id:
                continue
            if cand["amount"] is None:
                continue
            try:
                if abs(float(cand["amount"]) - float(inv_amount)) > 0.01:
                    continue
            except (TypeError, ValueError):
                continue
            results.append(
                (
                    inv_no,
                    inv_name,
                    inv_date,
                    inv_amount,
                    matched_sp,
                    matched_name,
                    cand["sp_order_no"],
                    cand["name"],
                    cand["date"],
                    cand["amount"],
                )
            )

    print(f"Konflikti (racun vec uparen): {len(results)}")
    if return_rows:
        return [
            "invoice_no",
            "invoice_name",
            "invoice_date",
            "invoice_amount",
            "matched_sp_order",
            "matched_name",
            "candidate_sp_order",
            "candidate_name",
            "candidate_date",
            "candidate_amount",
        ], results


def report_nearest_invoice(conn: sqlite3.Connection, return_rows: bool = False):
    order_rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.picked_up_at, o.created_at, "
        "MAX(oi.cod_amount), MIN(oi.cod_amount), SUM(oi.cod_amount), "
        "MAX(oi.addon_cod), MIN(oi.addon_cod), SUM(oi.addon_cod), "
        "MAX(oi.advance_amount), MIN(oi.advance_amount), SUM(oi.advance_amount), "
        "MAX(oi.addon_advance), MIN(oi.addon_advance), SUM(oi.addon_advance) "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND o.id NOT IN (SELECT order_id FROM order_flags WHERE flag = 'needs_invoice') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%obradi%') "
        "GROUP BY o.id"
    ).fetchall()

    order_ids = [int(row[0]) for row in order_rows]
    net_map = build_order_net_map(conn, order_ids)

    inv_rows = conn.execute(
        "SELECT i.id, i.number, i.customer_name, i.turnover, i.amount_due "
        "FROM invoices i"
    ).fetchall()
    invoices_by_date = {}
    invoices_no_date = []
    for row in inv_rows:
        inv = {
            "id": int(row[0]),
            "number": row[1],
            "name": row[2],
            "date": normalize_date(row[3]),
            "amount": row[4],
        }
        if inv["date"]:
            invoices_by_date.setdefault(inv["date"], []).append(inv)
        else:
            invoices_no_date.append(inv)

    results = []
    for row in order_rows:
        sp_no = row[1]
        name = row[2]
        display_date = row[3] or row[4]
        amount = net_map.get(int(row[0]))
        order_date = normalize_date(display_date)
        if amount is None or abs(amount) < 0.01:
            continue

        candidates = []
        if order_date:
            for offset in range(-10, 11):
                candidates.extend(
                    invoices_by_date.get(order_date + timedelta(days=offset), [])
                )
            candidates.extend(invoices_no_date)
        else:
            candidates = list(invoices_no_date)

        best = None
        best_diff = None
        for inv in candidates:
            if inv["amount"] is None:
                continue
            try:
                diff = abs(float(amount) - float(inv["amount"]))
            except (TypeError, ValueError):
                continue
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best = inv

        if best is None:
            results.append((sp_no, name, display_date, amount, None, None, None, None))
        else:
            results.append(
                (
                    sp_no,
                    name,
                    display_date,
                    amount,
                    best["number"],
                    best["name"],
                    best["date"],
                    best_diff,
                )
            )

    print(f"Najblizi racuni: {len(results)}")
    if return_rows:
        return [
            "sp_order_no",
            "customer_name",
            "order_date",
            "order_amount",
            "invoice_no",
            "invoice_name",
            "invoice_date",
            "amount_diff",
        ], results


def report_unmatched_reasons(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.picked_up_at, o.created_at, o.status, "
        "MAX(oi.cod_amount), MIN(oi.cod_amount), SUM(oi.cod_amount), "
        "MAX(oi.addon_cod), MIN(oi.addon_cod), SUM(oi.addon_cod), "
        "MAX(oi.advance_amount), MIN(oi.advance_amount), SUM(oi.advance_amount), "
        "MAX(oi.addon_advance), MIN(oi.addon_advance), SUM(oi.addon_advance) "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND o.id NOT IN (SELECT order_id FROM order_flags WHERE flag = 'needs_invoice') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "GROUP BY o.id"
    ).fetchall()

    inv_rows = conn.execute(
        "SELECT i.id, i.customer_name, i.turnover, i.amount_due, "
        "CASE WHEN m.id IS NULL THEN 0 ELSE 1 END AS is_matched "
        "FROM invoices i "
        "LEFT JOIN invoice_matches m ON m.invoice_id = i.id"
    ).fetchall()
    matched_map = {
        int(row[0]): str(row[1])
        for row in conn.execute(
            "SELECT m.invoice_id, o.sp_order_no "
            "FROM invoice_matches m "
            "JOIN orders o ON o.id = m.order_id"
        ).fetchall()
    }
    invoices = []
    invoices_by_date = {}
    invoices_no_date = []
    for row in inv_rows:
        inv = {
            "id": int(row[0]),
            "name_norm": normalize_text(row[1]),
            "date": normalize_date(row[2]),
            "amount": row[3],
            "matched": bool(row[4]),
        }
        invoices.append(inv)
        if inv["date"]:
            invoices_by_date.setdefault(inv["date"], []).append(inv)
        else:
            invoices_no_date.append(inv)

    def amount_exact(a, b) -> bool:
        return amount_exact_strict(a, b)

    order_ids = [int(row[0]) for row in rows]
    net_map = build_order_net_map(conn, order_ids)

    results = []
    reason_counts = {}

    for row in rows:
        sp_no = row[1]
        name = row[2]
        picked_up_at = row[3]
        created_at = row[4]
        status = row[5]
        display_date = picked_up_at or created_at
        if is_cancelled_status(status) or is_in_progress_status(status):
            reason = "otkazano"
        else:
            amount = net_map.get(int(row[0]))
            order_date = normalize_date(display_date)

            if amount is None or abs(amount) < 0.01:
                continue
            else:
                if order_date and (date.today() - order_date).days <= 3:
                    continue
                if order_date:
                    date_candidates = []
                    for offset in range(-10, 11):
                        date_candidates.extend(
                            invoices_by_date.get(
                                order_date + timedelta(days=offset), []
                            )
                        )
                    date_candidates.extend(invoices_no_date)
                else:
                    date_candidates = invoices

                amount_candidates = [
                    inv
                    for inv in date_candidates
                    if amount_exact(amount, inv.get("amount"))
                ]
                unmatched_candidates = [
                    inv for inv in amount_candidates if not inv.get("matched")
                ]
                matched_candidates = [
                    inv for inv in amount_candidates if inv.get("matched")
                ]
                name_candidates = [
                    inv
                    for inv in amount_candidates
                    if inv.get("name_norm") == normalize_text(name)
                ]
                name_unmatched = [
                    inv for inv in name_candidates if not inv.get("matched")
                ]
                name_matched = [inv for inv in name_candidates if inv.get("matched")]

                recent = False
                if order_date:
                    try:
                        recent = (date.today() - order_date).days <= 3
                    except Exception:
                        recent = False

                if not order_date:
                    reason = (
                        "nema_datuma_ima_iznos"
                        if amount_candidates
                        else "nema_datuma_nema_iznos"
                    )
                elif recent:
                    reason = "svjeza_narudzba"
                elif not amount_candidates:
                    reason = "nema_iznosa_u_prozoru"
                elif name_unmatched:
                    reason = "ima_kandidata_manual"
                elif name_matched:
                    other_orders = [
                        matched_map.get(inv["id"])
                        for inv in name_matched
                        if matched_map.get(inv["id"])
                    ]
                    if other_orders:
                        reason = f"racun_vec_uparen_ime_iznos: {', '.join(sorted(set(other_orders)))}"
                    else:
                        reason = "racun_vec_uparen_ime_iznos"
                elif unmatched_candidates:
                    reason = "ima_kandidata_manual"
                elif matched_candidates:
                    reason = "racun_vec_uparen"
                else:
                    reason = "nema_kandidata"

        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        results.append((sp_no, name, display_date, reason))

    summary = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)
    print("Neuparene razlozi (top):")
    for reason, cnt in summary:
        print(f"{reason}: {cnt}")

    if return_rows:
        return ["sp_order_no", "customer_name", "picked_up_at", "reason"], results


def report_open_invoices(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT number, customer_name, turnover, amount_due, open_amount "
        "FROM invoices "
        "WHERE open_amount IS NOT NULL AND open_amount > 0 "
        "ORDER BY turnover"
    ).fetchall()
    print(f"Otvorene fakture: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | due={row[3]} | open={row[4]}")
    if return_rows:
        return [
            "number",
            "customer_name",
            "turnover",
            "amount_due",
            "open_amount",
        ], rows


def report_returns(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM returns GROUP BY status ORDER BY COUNT(*) DESC"
    ).fetchall()
    print("Povrati po statusu:")
    for row in rows:
        print(f"{row[0]}: {row[1]}")
    if return_rows:
        return ["status", "count"], rows


def report_needs_invoice(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT m.id, o.sp_order_no, o.customer_name, o.picked_up_at, "
        "i.number, i.turnover, i.amount_due "
        "FROM invoice_matches m "
        "JOIN orders o ON o.id = m.order_id "
        "JOIN invoices i ON i.id = m.invoice_id "
        "WHERE m.status = 'needs_invoice' "
        "ORDER BY o.picked_up_at"
    ).fetchall()
    print(f"Potrebno kreirati racun: {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]}"
        )
    if return_rows:
        return [
            "match_id",
            "sp_order_no",
            "customer_name",
            "picked_up_at",
            "invoice_no",
            "invoice_date",
            "amount_due",
        ], rows


def report_needs_invoice_orders(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, o.status "
        "FROM order_flags f "
        "JOIN orders o ON o.id = f.order_id "
        "WHERE f.flag = 'needs_invoice' "
        "ORDER BY o.picked_up_at"
    ).fetchall()
    print(f"Narudzbe bez kandidata (needs_invoice): {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["sp_order_no", "customer_name", "picked_up_at", "status"], rows


def report_no_value_orders(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, o.status "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "GROUP BY o.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") = 0 "
        "ORDER BY o.picked_up_at"
    ).fetchall()
    print(f"SP narudzbe bez vrijednosti: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["sp_order_no", "customer_name", "picked_up_at", "status"], rows


def report_candidates(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, "
        "i.number, i.customer_name, i.turnover, i.amount_due, c.score, c.detail "
        "FROM invoice_candidates c "
        "JOIN orders o ON o.id = c.order_id "
        "JOIN invoices i ON i.id = c.invoice_id "
        "ORDER BY o.picked_up_at, c.score DESC"
    ).fetchall()
    print(f"Kandidati za neuparene: {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | "
            f"{row[4]} | {row[5]} | {row[6]} | {row[7]} | {row[8]}"
        )
    if return_rows:
        return [
            "sp_order_no",
            "order_name",
            "order_date",
            "invoice_no",
            "invoice_name",
            "invoice_date",
            "amount_due",
            "score",
            "detail",
        ], rows


def report_storno(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT s.storno_invoice_id, si.number, s.original_invoice_id, oi.number, "
        "s.storno_amount, s.original_amount, s.remaining_open, s.is_partial "
        "FROM invoice_storno s "
        "JOIN invoices si ON si.id = s.storno_invoice_id "
        "JOIN invoices oi ON oi.id = s.original_invoice_id "
        "ORDER BY s.is_partial DESC, si.number"
    ).fetchall()
    print(f"Storno veze: {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[1]} -> {row[3]} | storno={row[4]} | orig={row[5]} | open={row[6]} | partial={row[7]}"
        )
    if return_rows:
        return [
            "storno_invoice_no",
            "original_invoice_no",
            "storno_amount",
            "original_amount",
            "remaining_open",
            "is_partial",
        ], [(r[1], r[3], r[4], r[5], r[6], r[7]) for r in rows]


def report_bank_sp(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT dtposted, amount, purpose, payee_name "
        "FROM bank_transactions "
        "WHERE benefit = 'credit' AND payee_name LIKE '%SLANJE PAKETA%' "
        "ORDER BY dtposted"
    ).fetchall()
    print(f"SP uplate na banku: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["dtposted", "amount", "purpose", "payee_name"], rows


def report_bank_refunds(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT dtposted, amount, purpose, payee_name "
        "FROM bank_transactions "
        "WHERE benefit = 'debit' AND (purpose LIKE '%Povrat%' OR purpose LIKE '%storno%') "
        "ORDER BY dtposted"
    ).fetchall()
    print(f"Refundacije/povrati (banka): {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["dtposted", "amount", "purpose", "payee_name"], rows


def extract_bank_refunds(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        "SELECT id, purpose, refnumber, payeerefnumber "
        "FROM bank_transactions "
        "WHERE benefit = 'debit' "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_refunds)"
    ).fetchall()
    inserted = 0
    for row in rows:
        txn_id, purpose, refnumber, payeerefnumber = row
        reason = classify_refund_reason(purpose)
        if not reason:
            continue
        invoice_no, digits = extract_invoice_no_from_text(purpose)
        source = "purpose" if invoice_no else None
        if not invoice_no:
            invoice_no, digits = extract_invoice_no_from_text(refnumber)
            source = "refnumber" if invoice_no else None
        if not invoice_no:
            invoice_no, digits = extract_invoice_no_from_text(payeerefnumber)
            source = "payeerefnumber" if invoice_no else None
        cur = conn.execute(
            "INSERT OR IGNORE INTO bank_refunds ("
            "bank_txn_id, invoice_no, invoice_no_digits, invoice_no_source, reason, created_at"
            ") VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (int(txn_id), invoice_no, digits, source, reason),
        )
        if cur.rowcount:
            inserted += 1
    conn.commit()
    print(f"Izvuceni povrati (banka): {inserted}")
    return inserted


def report_bank_refunds_extracted(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT bt.dtposted, bt.amount, bt.purpose, bt.payee_name, "
        "br.invoice_no, br.invoice_no_digits, br.invoice_no_source, br.reason "
        "FROM bank_refunds br "
        "JOIN bank_transactions bt ON bt.id = br.bank_txn_id "
        "ORDER BY bt.dtposted"
    ).fetchall()
    print(f"Povrati iz izvoda (ekstrakt): {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | "
            f"{row[4]} | {row[6]} | {row[7]}"
        )
    if return_rows:
        return [
            "dtposted",
            "amount",
            "purpose",
            "payee_name",
            "invoice_no",
            "invoice_no_digits",
            "invoice_no_source",
            "reason",
        ], rows


def report_bank_unmatched_sp(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT dtposted, amount, purpose, payee_name "
        "FROM bank_transactions "
        "WHERE benefit = 'credit' AND payee_name LIKE '%SLANJE PAKETA%' "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches) "
        "ORDER BY dtposted"
    ).fetchall()
    print(f"Neuparene SP uplate (banka): {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["dtposted", "amount", "purpose", "payee_name"], rows


def report_bank_unmatched_refunds(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT dtposted, amount, purpose, payee_name "
        "FROM bank_transactions "
        "WHERE benefit = 'debit' AND (purpose LIKE '%Povrat%' OR purpose LIKE '%storno%') "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches) "
        "ORDER BY dtposted"
    ).fetchall()
    print(f"Neuparene refundacije (banka): {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["dtposted", "amount", "purpose", "payee_name"], rows


def report_sp_vs_bank(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT substr(o.picked_up_at, 1, 7) AS period, SUM(p.amount) AS sp_sum "
        "FROM payments p "
        "JOIN orders o ON o.sp_order_no = p.sp_order_no "
        "WHERE o.picked_up_at IS NOT NULL "
        "GROUP BY period"
    ).fetchall()
    bank_rows = conn.execute(
        "SELECT substr(dtposted, 1, 7) AS period, SUM(amount) AS bank_sum "
        "FROM bank_transactions "
        "WHERE benefit = 'credit' AND payee_name LIKE '%SLANJE PAKETA%' "
        "GROUP BY period"
    ).fetchall()
    bank_map = {r[0]: r[1] for r in bank_rows}
    merged = []
    for period, sp_sum in rows:
        bank_sum = bank_map.get(period)
        diff = None
        if sp_sum is not None and bank_sum is not None:
            diff = float(sp_sum) - float(bank_sum)
        merged.append((period, sp_sum, bank_sum, diff))
    print(f"SP vs banka periodi: {len(merged)}")
    for row in merged[:50]:
        print(f"{row[0]} | sp={row[1]} | bank={row[2]} | diff={row[3]}")
    if return_rows:
        return ["period", "sp_sum", "bank_sum", "diff"], merged


def close_invoices_from_confirmed_matches(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT m.invoice_id FROM invoice_matches m WHERE m.status = 'auto'"
    ).fetchall()
    for (invoice_id,) in rows:
        conn.execute(
            "UPDATE invoices SET open_amount = 0, "
            "payment_amount = COALESCE(payment_amount, amount_due) "
            "WHERE id = ?",
            (int(invoice_id),),
        )
    conn.commit()


def reset_minimax_matches(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM invoice_matches")
    conn.execute("DELETE FROM invoice_candidates")
    conn.execute("DELETE FROM order_flags WHERE flag = 'needs_invoice'")
    conn.commit()


def report_alarms(conn: sqlite3.Connection, days: int = 7, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, o.status "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.picked_up_at IS NOT NULL "
        "AND julianday('now') - julianday(o.picked_up_at) > ? "
        "AND o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "GROUP BY o.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") != 0 "
        "ORDER BY o.picked_up_at",
        (days,),
    ).fetchall()
    print(f"Alarmi (stare bez racuna): {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["sp_order_no", "customer_name", "picked_up_at", "status"], rows


def report_refunds_without_storno(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT bt.dtposted, bt.amount, bt.purpose, bt.payee_name "
        "FROM bank_transactions bt "
        "LEFT JOIN bank_matches bm ON bm.bank_txn_id = bt.id AND bm.match_type = 'storno' "
        "WHERE bt.benefit = 'debit' AND (bt.purpose LIKE '%Povrat%' OR bt.purpose LIKE '%storno%') "
        "AND bm.id IS NULL "
        "ORDER BY bt.dtposted"
    ).fetchall()
    print(f"Refundacije bez storno racuna: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    if return_rows:
        return ["dtposted", "amount", "purpose", "payee_name"], rows


def report_order_amount_issues(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, "
        "SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        ") AS base_sum, "
        "SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") AS advance_sum, "
        "SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") AS net_sum "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "GROUP BY o.id "
        "HAVING net_sum < 0 "
        "ORDER BY o.picked_up_at"
    ).fetchall()
    print(f"Izvjestaj suma (neto < 0): {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | base={row[3]} | adv={row[4]} | net={row[5]}"
        )
    if return_rows:
        return [
            "sp_order_no",
            "customer_name",
            "picked_up_at",
            "base_sum",
            "advance_sum",
            "net_sum",
        ], rows


def report_duplicate_customers(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT customer_key, COUNT(*) AS cnt, MIN(picked_up_at), MAX(picked_up_at) "
        "FROM orders "
        "WHERE customer_key IS NOT NULL AND customer_key != '' "
        "GROUP BY customer_key "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC"
    ).fetchall()
    print(f"Dupli customer_key: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} -> {row[3]}")
    if return_rows:
        return ["customer_key", "count", "first_order", "last_order"], rows


def report_top_customers(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.customer_key, COUNT(DISTINCT o.id) AS orders_cnt, "
        "SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") AS net_total, "
        "MIN(o.picked_up_at), MAX(o.picked_up_at) "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.customer_key IS NOT NULL AND o.customer_key != '' "
        "GROUP BY o.customer_key "
        "ORDER BY net_total DESC"
    ).fetchall()
    print(f"Top kupci: {len(rows)}")
    for row in rows[:50]:
        print(f"{row[0]} | orders={row[1]} | net={row[2]} | {row[3]} -> {row[4]}")
    if return_rows:
        return [
            "customer_key",
            "orders_count",
            "net_total",
            "first_order",
            "last_order",
        ], rows


def report_minimax_items(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT sku, name, unit, mass_kg, closing_qty, "
        "closing_purchase_value, closing_sales_value, "
        "opening_qty, opening_purchase_value, incoming_qty, incoming_purchase_value, "
        "outgoing_qty, outgoing_sales_value "
        "FROM minimax_items "
        "ORDER BY sku"
    ).fetchall()
    print(f"Minimax artikli: {len(rows)}")
    enriched = []
    for row in rows[:50]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
    for row in rows:
        (
            sku,
            name,
            unit,
            mass_kg,
            closing_qty,
            closing_pv,
            closing_sv,
            opening_qty,
            opening_pv,
            incoming_qty,
            incoming_pv,
            outgoing_qty,
            outgoing_sv,
        ) = row
        try:
            total_in_qty = (opening_qty or 0) + (incoming_qty or 0)
            avg_purchase = (opening_pv or 0) + (incoming_pv or 0)
            avg_purchase = avg_purchase / total_in_qty if total_in_qty else None
        except Exception:
            avg_purchase = None
        try:
            avg_sale = (
                (outgoing_sv or 0) / (outgoing_qty or 0) if outgoing_qty else None
            )
        except Exception:
            avg_sale = None
        margin = None
        if avg_purchase is not None and avg_sale is not None:
            try:
                margin = float(avg_sale) - float(avg_purchase)
            except Exception:
                margin = None
        enriched.append(
            (
                sku,
                name,
                unit,
                mass_kg,
                closing_qty,
                closing_pv,
                closing_sv,
                avg_purchase,
                avg_sale,
                margin,
            )
        )
    if return_rows:
        return [
            "sku",
            "name",
            "unit",
            "mass_kg",
            "closing_qty",
            "closing_purchase_value",
            "closing_sales_value",
            "avg_purchase_price",
            "avg_sale_price",
            "avg_margin",
        ], enriched


def report_category_sales(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT oi.product_code, "
        "SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") AS net_total, "
        "SUM(COALESCE(oi.qty, 0)) AS qty "
        "FROM order_items oi "
        "WHERE oi.product_code IS NOT NULL AND oi.product_code != '' "
        "GROUP BY oi.product_code"
    ).fetchall()
    by_cat = {}
    for sku, net_total, qty in rows:
        cat = kategorija_za_sifru(str(sku))
        cur = by_cat.get(cat, {"net_total": 0.0, "qty": 0.0})
        cur["net_total"] += float(net_total or 0)
        cur["qty"] += float(qty or 0)
        by_cat[cat] = cur
    merged = [(cat, vals["net_total"], vals["qty"]) for cat, vals in by_cat.items()]
    merged.sort(key=lambda x: x[1], reverse=True)
    print(f"Prodaja po kategoriji: {len(merged)}")
    for row in merged[:50]:
        print(f"{row[0]} | net={row[1]} | qty={row[2]}")
    if return_rows:
        return ["category", "net_total", "qty"], merged


def report_category_returns(conn: sqlite3.Connection, return_rows: bool = False):
    rows = conn.execute(
        "SELECT o.id, o.status, oi.product_code, oi.qty "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.status IS NOT NULL"
    ).fetchall()
    by_cat = {}
    for _, status, sku, qty in rows:
        if not status:
            continue
        status_norm = normalize_text(status)
        if not status_norm.startswith("vrac"):
            continue
        if not sku:
            continue
        cat = kategorija_za_sifru(str(sku))
        by_cat[cat] = by_cat.get(cat, 0.0) + float(qty or 0)
    merged = [(cat, qty) for cat, qty in by_cat.items()]
    merged.sort(key=lambda x: x[1], reverse=True)
    print(f"Povrati po kategoriji: {len(merged)}")
    for row in merged[:50]:
        print(f"{row[0]} | qty={row[1]}")
    if return_rows:
        return ["category", "qty"], merged


def match_bank_sp_payments(
    conn: sqlite3.Connection,
    day_tolerance: int = 2,
    progress_task: str | None = None,
    start_at: int = 0,
    total: int | None = None,
):
    txns = conn.execute(
        "SELECT id, dtposted, amount FROM bank_transactions "
        "WHERE benefit = 'credit' AND payee_name LIKE '%SLANJE PAKETA%' "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches)"
    ).fetchall()
    payments = conn.execute(
        "SELECT p.id, p.amount, o.picked_up_at "
        "FROM payments p "
        "LEFT JOIN orders o ON o.sp_order_no = p.sp_order_no"
    ).fetchall()
    processed = start_at
    last_progress = start_at
    progress_every = 10

    def maybe_update_progress():
        nonlocal last_progress
        if not progress_task:
            return
        if processed - last_progress >= progress_every or (
            total and processed >= total
        ):
            update_task_progress(conn, progress_task, processed)
            last_progress = processed

    for txn_id, dtposted, amount in txns:
        txn_date = normalize_date(dtposted)
        best = None
        best_score = 0
        best_method = "amount"
        for pay_id, pay_amount, picked_up_at in payments:
            if pay_amount is None or amount is None:
                continue
            try:
                amount_diff = abs(float(pay_amount) - float(amount))
            except (TypeError, ValueError):
                continue
            if amount_diff <= 0.01:
                score = 60
            elif amount_diff <= 2.0:
                score = 40
            else:
                continue
            pay_date = normalize_date(picked_up_at)
            if txn_date and pay_date:
                day_diff = abs((txn_date - pay_date).days)
                if day_diff <= day_tolerance:
                    score += 20
                    best_method = "amount+date"
            if score > best_score:
                best_score = score
                best = pay_id
        if best is not None:
            conn.execute(
                "INSERT OR IGNORE INTO bank_matches "
                "(bank_txn_id, match_type, ref_id, score, method, matched_at) "
                "VALUES (?, 'sp_payment', ?, ?, ?, datetime('now'))",
                (txn_id, best, best_score, best_method),
            )
        processed += 1
        maybe_update_progress()
    conn.commit()
    if progress_task:
        update_task_progress(conn, progress_task, processed)
        return processed
    return None


def match_bank_refunds(
    conn: sqlite3.Connection,
    progress_task: str | None = None,
    start_at: int = 0,
    total: int | None = None,
):
    txns = conn.execute(
        "SELECT id, purpose, refnumber, payeerefnumber FROM bank_transactions "
        "WHERE benefit = 'debit' AND (purpose LIKE '%Povrat%' OR purpose LIKE '%storno%') "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches)"
    ).fetchall()
    processed = start_at
    last_progress = start_at
    progress_every = 10

    def maybe_update_progress():
        nonlocal last_progress
        if not progress_task:
            return
        if processed - last_progress >= progress_every or (
            total and processed >= total
        ):
            update_task_progress(conn, progress_task, processed)
            last_progress = processed

    for txn_id, purpose, refnumber, payeerefnumber in txns:
        text = " ".join(
            [str(purpose or ""), str(refnumber or ""), str(payeerefnumber or "")]
        )
        invoice_no = extract_invoice_number_from_text(text)
        if not invoice_no:
            processed += 1
            maybe_update_progress()
            continue
        row = conn.execute(
            "SELECT id FROM invoices WHERE number = ?",
            (invoice_no,),
        ).fetchone()
        if not row:
            processed += 1
            maybe_update_progress()
            continue
        conn.execute(
            "INSERT OR IGNORE INTO bank_matches "
            "(bank_txn_id, match_type, ref_id, score, method, matched_at) "
            "VALUES (?, 'storno', ?, 100, 'purpose', datetime('now'))",
            (txn_id, int(row[0])),
        )
        processed += 1
        maybe_update_progress()
    conn.commit()
    if progress_task:
        update_task_progress(conn, progress_task, processed)
        return processed
    return None


def run_match_minimax_process(db_path: str) -> None:
    conn = connect_db(Path(db_path))
    init_db(conn)
    match_minimax(conn, progress_task="match_minimax")
    conn.close()


def run_match_bank_process(db_path: str, day_tolerance: int = 2) -> None:
    conn = connect_db(Path(db_path))
    init_db(conn)
    task = "match_bank"
    credit_count = conn.execute(
        "SELECT COUNT(*) FROM bank_transactions "
        "WHERE benefit = 'credit' AND payee_name LIKE '%SLANJE PAKETA%' "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches)"
    ).fetchone()[0]
    debit_count = conn.execute(
        "SELECT COUNT(*) FROM bank_transactions "
        "WHERE benefit = 'debit' AND (purpose LIKE '%Povrat%' OR purpose LIKE '%storno%') "
        "AND id NOT IN (SELECT bank_txn_id FROM bank_matches)"
    ).fetchone()[0]
    total = int(credit_count or 0) + int(debit_count or 0) + 2
    set_task_progress(conn, task, total)
    update_task_progress(conn, task, 1)
    processed = 1
    processed = (
        match_bank_sp_payments(
            conn,
            day_tolerance,
            progress_task=task,
            start_at=processed,
            total=total,
        )
        or processed
    )
    processed = (
        match_bank_refunds(
            conn,
            progress_task=task,
            start_at=processed,
            total=total,
        )
        or processed
    )
    update_task_progress(conn, task, total)
    conn.close()


def run_close_invoices_process(db_path: str) -> None:
    conn = connect_db(Path(db_path))
    init_db(conn)
    close_invoices_from_confirmed_matches(conn)
    conn.close()


def run_tracking_process(
    db_path: str, batch_size: int = 20, force_refresh: int = 0
) -> None:
    conn = connect_db(Path(db_path))
    init_db(conn)
    ensure_customer_keys(conn)
    update_unpicked_tracking(
        conn,
        batch_size=batch_size,
        progress_task="tracking",
        force_refresh=bool(force_refresh),
    )
    conn.close()


def report_unmatched_with_candidates(
    conn: sqlite3.Connection, return_rows: bool = False
):
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at, o.status, "
        "i.number, i.customer_name, i.turnover, i.amount_due, c.score, c.detail "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "LEFT JOIN invoice_candidates c ON c.order_id = o.id "
        "LEFT JOIN invoices i ON i.id = c.invoice_id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND o.id NOT IN (SELECT order_id FROM order_flags WHERE flag = 'needs_invoice') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%obradi%') "
        "AND date(substr(COALESCE(o.picked_up_at, o.created_at), 1, 10)) <= date('now', '-3 day') "
        "GROUP BY o.id, c.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") != 0 "
        "ORDER BY o.picked_up_at, c.score DESC"
    ).fetchall()
    print(f"Neuparene + kandidati: {len(rows)}")
    for row in rows[:50]:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | "
            f"{row[4]} | {row[5]} | {row[6]} | {row[7]} | {row[8]} | {row[9]}"
        )
    if return_rows:
        return [
            "sp_order_no",
            "order_name",
            "order_date",
            "status",
            "candidate_invoice_no",
            "candidate_invoice_name",
            "candidate_invoice_date",
            "candidate_amount_due",
            "candidate_score",
            "candidate_detail",
        ], rows


def report_unmatched_with_candidates_grouped(
    conn: sqlite3.Connection, return_rows: bool = False
):
    cols, rows = report_unmatched_with_candidates(conn, return_rows=True)
    grouped_rows = []
    last_key = None
    for row in rows:
        key = (row[0], row[1], row[2], row[3])
        if key != last_key:
            if last_key is not None:
                grouped_rows.append([""] * len(cols))
            grouped_rows.append(
                [
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
        grouped_rows.append(list(row))
        last_key = key
    if return_rows:
        return cols, grouped_rows
    print(f"Neuparene + kandidati (grupisano): {len(grouped_rows)}")


def write_report(columns, rows, out_dir: Path, name: str, fmt: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{name}_{stamp}.{fmt}"
    df = pd.DataFrame(rows, columns=columns)
    if fmt == "xlsx":
        df.to_excel(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)
    return out_path


def _short_expense_name(label: str) -> str:
    if not label:
        return "Nepoznato"
    base = label.split(",")[0].strip()
    return base or label


def run_ui(db_path: Path) -> None:
    import customtkinter as ctk
    import concurrent.futures
    import os
    import tempfile
    import threading
    import tkinter as tk
    import time
    from tkinter import filedialog, messagebox, simpledialog
    from tkcalendar import Calendar
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure

    def acquire_app_lock():
        lock_path = Path(tempfile.gettempdir()) / "srb1_app.lock"
        handle = lock_path.open("a+")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            return None
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        return handle

    lock_handle = acquire_app_lock()
    if not lock_handle:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Info", "Aplikacija je vec pokrenuta.")
        root.destroy()
        return

    ctk.set_appearance_mode("light")
    ctk.set_default_color_theme("blue")

    app = ctk.CTk()
    app.title("SRB1.2 - Kontrola i Analitika")
    app.geometry("1200x750")

    def on_close():
        if messagebox.askyesno("Potvrda", "Da li želite završiti rad u aplikaciji?"):
            try:
                lock_handle.close()
            except Exception:
                pass
            app.destroy()

    app.protocol("WM_DELETE_WINDOW", on_close)

    settings = load_app_settings()
    stored_db = settings.get("db_path")
    if stored_db:
        db_path = (APP_DIR / Path(str(stored_db))).resolve() if not Path(str(stored_db)).is_absolute() else Path(str(stored_db))
    state = {
        "db_path": db_path,
        "period_days": None,
        "period_start": None,
        "period_end": None,
        "expense_period_days": None,
        "expense_period_start": None,
        "expense_period_end": None,
        "expense_year": None,
        "expense_month": None,
        "expense_top_n": 5,
        "prodaja_period_days": None,
        "prodaja_period_start": None,
        "prodaja_period_end": None,
    }
    ctx = UIContext(state=state)
    btn_reset_matches = None
    SKU_DAILY_CSV = Path("Kalkulacije_kartice_art/izlaz/sku_daily_metrics.csv")
    SKU_SUMMARY_CSV = Path("Kalkulacije_kartice_art/izlaz/kartice_sku_summary.csv")
    PROMO_CSV = Path("Kalkulacije_kartice_art/izlaz/sku_promo_periods.csv")
    _sku_daily_cache = {"mtime": None, "df": pd.DataFrame()}
    _sku_summary_cache = {"mtime": None, "df": pd.DataFrame()}
    _promo_cache = {"mtime": None, "df": pd.DataFrame()}

    def get_conn():
        conn = connect_db(state["db_path"])
        init_db(conn)
        ensure_customer_keys(conn)
        return conn

    def load_currency_mode():
        conn = get_conn()
        try:
            stored = get_app_state(conn, "currency_mode")
        finally:
            conn.close()
        state["currency_mode"] = stored or "RSD"

    def load_baseline_lock():
        conn = get_conn()
        try:
            locked = get_app_state(conn, "baseline_locked")
            locked_at = get_app_state(conn, "baseline_locked_at")
        finally:
            conn.close()
        state["baseline_locked"] = locked == "1"
        state["baseline_locked_at"] = locked_at

    def refresh_exchange_rate():
        conn = get_conn()
        error = None
        try:
            rate, error = fetch_cbbh_rsd_rate_debug()
            if rate is not None:
                set_app_state(conn, "rate_rsd_to_bam", f"{rate}")
            else:
                stored = get_app_state(conn, "rate_rsd_to_bam")
                rate = float(stored) if stored else None
            state["rate_rsd_to_bam"] = rate
        finally:
            conn.close()
        return rate, error

    def format_amount(amount: float) -> str:
        try:
            rsd_val = float(amount)
        except (TypeError, ValueError):
            return "0.00 RSD"
        rate = state.get("rate_rsd_to_bam")
        mode = state.get("currency_mode", "RSD")

        def fmt(val: float) -> str:
            return f"{val:,.0f}".replace(",", ".")

        if mode == "BAM":
            if rate is None:
                return f"{fmt(rsd_val)} RSD"
            bam_val = rsd_val * rate
            return f"{fmt(bam_val)} BAM"
        if rate is None:
            return f"{fmt(rsd_val)} RSD"
        bam_val = rsd_val * rate
        return f"{fmt(rsd_val)} RSD ({fmt(bam_val)} BAM)"

    def format_amount_rsd(amount: float) -> str:
        try:
            rsd_val = float(amount)
        except (TypeError, ValueError):
            return "0.00 RSD"
        return f"{rsd_val:,.2f}".replace(",", ".") + " RSD"

    def chart_currency_label() -> str:
        if (
            state.get("currency_mode") == "BAM"
            and state.get("rate_rsd_to_bam") is not None
        ):
            return "BAM"
        return "RSD"

    def chart_value(value) -> float:
        try:
            val = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        if state.get("currency_mode") == "BAM":
            rate = state.get("rate_rsd_to_bam")
            if rate is not None:
                return val * rate
        return val

    def format_amount_selected_only(amount: float) -> str:
        try:
            rsd_val = float(amount)
        except (TypeError, ValueError):
            return "0 RSD"
        mode = state.get("currency_mode", "RSD")
        rate = state.get("rate_rsd_to_bam")

        def fmt(val: float) -> str:
            return f"{val:,.0f}".replace(",", ".")

        if mode == "BAM" and rate is not None:
            return f"{fmt(rsd_val * rate)} BAM"
        return f"{fmt(rsd_val)} RSD"

    def format_display_amount(val: float) -> str:
        try:
            num = float(val or 0.0)
        except (TypeError, ValueError):
            num = 0.0

        def fmt(v: float) -> str:
            return f"{v:,.0f}".replace(",", ".")

        return f"{fmt(num)} {chart_currency_label()}"

    def _parse_user_date(text: str) -> date | None:
        from srb_modules.ui_helpers import parse_user_date

        return parse_user_date(text)

    def _format_user_date(value: date) -> str:
        from srb_modules.ui_helpers import format_user_date

        return format_user_date(value)

    def _add_calendar_picker(parent, var: ctk.StringVar, width: int = 120):
        from srb_modules.ui_helpers import add_calendar_picker

        return add_calendar_picker(
            app,
            ctk,
            tk,
            Calendar,
            parent,
            var,
            _parse_user_date,
            _format_user_date,
            width=width,
        )

    def _pick_date_range_dialog(
        title: str, initial_start: date | None, initial_end: date | None
    ) -> tuple[date | None, date | None]:
        from srb_modules.ui_helpers import pick_date_range_dialog

        return pick_date_range_dialog(
            app,
            tk,
            Calendar,
            title,
            initial_start,
            initial_end,
            _format_user_date,
        )

    def _load_sku_daily_dataframe() -> pd.DataFrame:
        path = SKU_DAILY_CSV
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            _sku_daily_cache["df"] = pd.DataFrame()
            _sku_daily_cache["mtime"] = None
            return _sku_daily_cache["df"]
        if _sku_daily_cache["mtime"] == mtime and not _sku_daily_cache["df"].empty:
            return _sku_daily_cache["df"]
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            _sku_daily_cache["df"] = pd.DataFrame()
            _sku_daily_cache["mtime"] = mtime
            return _sku_daily_cache["df"]
        if df.empty:
            _sku_daily_cache["df"] = df
            _sku_daily_cache["mtime"] = mtime
            return df
        if "date" in df.columns:
            df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        else:
            df["date_dt"] = pd.NaT
        df["sku"] = df.get("sku", pd.Series("", index=df.index)).astype(str).str.strip()
        numeric_cols = [
            "stock_eod_qty",
            "oos_flag",
            "verified_available_flag",
            "gross_sales_qty",
            "return_qty",
            "net_sales_qty",
            "demand_baseline_qty",
            "lost_sales_qty",
            "lost_sales_value_est",
            "sp_unit_net_price",
            "sp_discount_share",
            "sp_qty",
            "sp_net_value",
            "confidence_score",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        _sku_daily_cache["df"] = df
        _sku_daily_cache["mtime"] = mtime
        return df

    def _load_sku_summary_dataframe() -> pd.DataFrame:
        path = SKU_SUMMARY_CSV
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            _sku_summary_cache["df"] = pd.DataFrame()
            _sku_summary_cache["mtime"] = None
            return _sku_summary_cache["df"]
        if _sku_summary_cache["mtime"] == mtime and not _sku_summary_cache["df"].empty:
            return _sku_summary_cache["df"]
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            _sku_summary_cache["df"] = pd.DataFrame()
            _sku_summary_cache["mtime"] = mtime
            return _sku_summary_cache["df"]
        if not df.empty and "SKU" in df.columns:
            df["sku"] = df["SKU"].astype(str).str.strip()
        _sku_summary_cache["df"] = df
        _sku_summary_cache["mtime"] = mtime
        return df

    def _load_promo_dataframe() -> pd.DataFrame:
        path = PROMO_CSV
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            _promo_cache["df"] = pd.DataFrame()
            _promo_cache["mtime"] = None
            return _promo_cache["df"]
        if _promo_cache["mtime"] == mtime and not _promo_cache["df"].empty:
            return _promo_cache["df"]
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            _promo_cache["df"] = pd.DataFrame()
            _promo_cache["mtime"] = mtime
            return _promo_cache["df"]
        if not df.empty:
            df["promo_start_dt"] = pd.to_datetime(df.get("promo_start"), errors="coerce").dt.date
            df["promo_end_dt"] = pd.to_datetime(df.get("promo_end"), errors="coerce").dt.date
        _promo_cache["df"] = df
        _promo_cache["mtime"] = mtime
        return df

    def _resolve_prodaja_period():
        start = state.get("prodaja_period_start")
        end = state.get("prodaja_period_end")
        days = state.get("prodaja_period_days")
        if days and start is None and end is None:
            today = datetime.utcnow().date()
            end = today
            start = today - timedelta(days=days - 1)
        return start, end

    def _filter_daily_by_period(
        df: pd.DataFrame, start: date | None, end: date | None
    ) -> pd.DataFrame:
        if df.empty:
            return df
        mask = pd.Series(True, index=df.index)
        if start is not None and "date_dt" in df.columns:
            mask &= df["date_dt"] >= start
        if end is not None and "date_dt" in df.columns:
            mask &= df["date_dt"] <= end
        return df.loc[mask]

    def get_progress_info(task: str):
        conn = get_conn()
        try:
            row = get_task_progress(conn, task)
        finally:
            conn.close()
        return row

    def poll_global_status():
        conn = get_conn()
        try:
            latest = get_latest_task_progress(conn)
        finally:
            conn.close()
        if latest:
            total = latest.get("total", 0)
            processed = latest.get("processed", 0)
            task = latest.get("task", "-")
            if total:
                pct = int((processed / total) * 100)
                task_status_var.set(f"Task: {task} {processed}/{total} ({pct}%)")
            else:
                task_status_var.set(f"Task: {task}")
        else:
            task_status_var.set("Task: idle")
        log_path = Path("exports") / "app-errors.log"
        if log_path.exists():
            try:
                last_line = (
                    log_path.read_text(encoding="utf-8").strip().splitlines()[-1]
                )
                last_error_var.set(f"Zadnja greska: {last_line[:120]}")
            except Exception:
                last_error_var.set("Zadnja greska: -")
        else:
            last_error_var.set("Zadnja greska: -")
        app.after(3000, poll_global_status)

    def format_eta(seconds: float | None) -> str:
        if seconds is None or seconds < 0:
            return "--"
        mins, secs = divmod(int(seconds), 60)
        if mins >= 60:
            hrs, mins = divmod(mins, 60)
            return f"{hrs}h {mins}m"
        if mins > 0:
            return f"{mins}m {secs}s"
        return f"{secs}s"

    def refresh_dashboard(*, full_refresh: bool = True):
        conn = get_conn()
        kpis = get_kpis(
            conn,
            state.get("period_days"),
            state.get("period_start"),
            state.get("period_end"),
        )
        unpicked_all = conn.execute(
            "SELECT COUNT(*) FROM orders "
            "WHERE status LIKE '%Vra\u0107eno%' OR status LIKE '%Vraceno%'"
        ).fetchone()[0]
        lbl_total_orders.configure(text=str(kpis["total_orders"]))
        try:
            widgets = ctx.state.get("finansije_widgets") or {}
            lbl_fin_net = widgets.get("lbl_net_revenue")
            if lbl_fin_net is not None:
                lbl_fin_net.configure(text=format_amount(kpis["total_revenue"]))
        except Exception:
            pass
        lbl_returns.configure(text=str(unpicked_all or 0))
        lbl_unmatched.configure(text=str(kpis["unmatched"]))
        conn.close()
        refresh_charts()
        if full_refresh:
            refresh_expenses()
            refresh_returns_charts()
            refresh_unpicked_charts()
            try:
                refresh_prodaja_views()
            except Exception:
                pass
            try:
                refresh_finansije()
            except Exception:
                pass
        try:
            if ctx.status_var is not None:
                ctx.status_var.set("Osvjezeno.")
                app.after(1500, lambda: ctx.status_var.set("Spremno."))
        except Exception:
            pass

    def _iso_date(value: date | str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        if not text:
            return None
        return text[:10]

    def _resolve_period_to_strings(
        days: int | None, start: date | str | None, end: date | str | None
    ) -> tuple[str | None, str | None]:
        start_str = _iso_date(start)
        end_str = _iso_date(end)
        if start_str or end_str:
            return start_str, end_str
        if days:
            start_dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            return start_dt, None
        return None, None

    def refresh_finansije():
        widgets = ctx.state.get("finansije_widgets") or {}
        lock_overlay = widgets.get("lock_overlay")
        if not ctx.state.get("finansije_unlocked", False):
            if lock_overlay is not None:
                try:
                    lock_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
                    lock_overlay.lift()
                except Exception:
                    pass
            return

        if lock_overlay is not None:
            try:
                lock_overlay.place_forget()
            except Exception:
                pass

        lbl_main = widgets.get("lbl_net_revenue")
        lbl_cmp = widgets.get("lbl_net_revenue_cmp")
        lbl_delta = widgets.get("lbl_net_revenue_delta")
        lbl_net = widgets.get("lbl_net_profit")
        lbl_net_cmp = widgets.get("lbl_net_profit_cmp")
        lbl_net_delta = widgets.get("lbl_net_profit_delta")
        expenses_var = widgets.get("expenses_var")
        refunds_var = widgets.get("refunds_var")
        unpaid_var = widgets.get("unpaid_var")
        pending_var = widgets.get("pending_var")
        ax_monthly = widgets.get("ax_monthly")
        canvas_monthly = widgets.get("canvas_monthly")
        period_label_var = widgets.get("period_label_var")
        if lbl_main is None or unpaid_var is None:
            return

        fin_custom = bool(ctx.state.get("fin_period_custom"))
        fin_start = ctx.state.get("fin_period_start")
        fin_end = ctx.state.get("fin_period_end")
        if fin_custom:
            main_days = None
            main_start = fin_start
            main_end = fin_end
        else:
            main_days = state.get("period_days")
            main_start = state.get("period_start")
            main_end = state.get("period_end")
            if period_label_var is not None:
                mapping = {
                    None: "Svo vrijeme",
                    90: "3 mjeseca",
                    180: "6 mjeseci",
                    360: "12 mjeseci",
                    720: "24 mjeseca",
                }
                try:
                    period_label_var.set(f"Dashboard: {mapping.get(main_days, 'period')}")
                except Exception:
                    pass

        main_start_str, main_end_str = _resolve_period_to_strings(
            main_days, main_start, main_end
        )

        conn = get_conn()
        try:
            k_main = get_kpis(conn, main_days, main_start_str, main_end_str)
            gross_cash = float(k_main.get("total_revenue", 0.0) or 0.0)
            lbl_main.configure(text=format_amount(gross_cash))

            exp = get_expense_summary(conn, main_days, main_start_str, main_end_str)
            expenses_total = float(exp.get("total", 0.0) or 0.0)
            refunds_total = float(
                get_refund_total_amount(conn, main_days, main_start_str, main_end_str)
            )
            if expenses_var is not None:
                expenses_var.set(format_amount(-expenses_total))
            if refunds_var is not None:
                refunds_var.set(format_amount(-refunds_total))
            net_profit = gross_cash - expenses_total
            if lbl_net is not None:
                lbl_net.configure(text=format_amount(net_profit))

            cmp_custom = bool(ctx.state.get("fin_compare_custom"))
            cmp_start = ctx.state.get("fin_compare_start")
            cmp_end = ctx.state.get("fin_compare_end")
            if cmp_custom and (cmp_start or cmp_end):
                cmp_start_str, cmp_end_str = _resolve_period_to_strings(
                    None, cmp_start, cmp_end
                )
                k_cmp = get_kpis(conn, None, cmp_start_str, cmp_end_str)
                cmp_val = float(k_cmp.get("total_revenue", 0.0) or 0.0)
                main_val = gross_cash
                if lbl_cmp is not None:
                    lbl_cmp.configure(text=format_amount(cmp_val))
                if lbl_delta is not None:
                    diff = main_val - cmp_val
                    pct = (diff / cmp_val * 100.0) if cmp_val else None
                    pct_txt = f" ({pct:+.1f}%)" if pct is not None else ""
                    lbl_delta.configure(text=f"Δ {format_amount(diff)}{pct_txt}")

                cmp_exp = get_expense_summary(conn, None, cmp_start_str, cmp_end_str)
                cmp_exp_total = float(cmp_exp.get("total", 0.0) or 0.0)
                cmp_net = cmp_val - cmp_exp_total
                if lbl_net_cmp is not None:
                    lbl_net_cmp.configure(text=format_amount(cmp_net))
                if lbl_net_delta is not None:
                    diff_net = net_profit - cmp_net
                    pct_net = (diff_net / cmp_net * 100.0) if cmp_net else None
                    pct_net_txt = f" ({pct_net:+.1f}%)" if pct_net is not None else ""
                    lbl_net_delta.configure(text=f"Δ {format_amount(diff_net)}{pct_net_txt}")
            else:
                if lbl_cmp is not None:
                    lbl_cmp.configure(text="-")
                if lbl_delta is not None:
                    lbl_delta.configure(text="")
                if lbl_net_cmp is not None:
                    lbl_net_cmp.configure(text="-")
                if lbl_net_delta is not None:
                    lbl_net_delta.configure(text="")

            unpaid_cnt, unpaid_sum = get_unpaid_sp_orders_summary(conn, None, None)
            unpaid_var.set(f"{unpaid_cnt} | {format_amount(unpaid_sum)}")
            if pending_var is not None:
                pending_cnt, pending_sum = get_pending_sp_orders_summary(conn)
                pending_var.set(f"{pending_cnt} | {format_amount(pending_sum)}")

            if ax_monthly is not None and canvas_monthly is not None:
                monthly = get_finansije_monthly(conn, main_days, main_start_str, main_end_str)
                ax_monthly.clear()
                if not monthly:
                    ax_monthly.set_title("Razlika Prihodi/Rashodi (nema podataka)")
                    ax_monthly.text(0.5, 0.5, "Nema podataka", ha="center", va="center")
                else:
                    month_names = [
                        "JAN",
                        "FEB",
                        "MAR",
                        "APR",
                        "MAJ",
                        "JUN",
                        "JUL",
                        "AVG",
                        "SEP",
                        "OKT",
                        "NOV",
                        "DEC",
                    ]
                    years = {p.split("-")[0] for (p, _, _, _) in monthly if p}
                    show_year = len(years) > 1
                    labels = []
                    bruto = []
                    troskovi = []
                    neto = []
                    for period, b, t, n in monthly:
                        try:
                            y, m = period.split("-")
                            mi = int(m)
                        except Exception:
                            y = period[:4]
                            mi = 1
                        lab = month_names[mi - 1] if 1 <= mi <= 12 else period
                        if show_year:
                            lab = f"{lab}-{y[-2:]}"
                        labels.append(lab)
                        bruto.append(chart_value(float(b or 0.0)))
                        troskovi.append(chart_value(float(t or 0.0)))
                        neto.append(chart_value(float(n or 0.0)))

                    x = list(range(len(labels)))
                    w = 0.25
                    x_l = [i - w for i in x]
                    x_r = [i + w for i in x]
                    cont_prihodi = ax_monthly.bar(
                        x_l, bruto, width=w, label="Prihodi", color="#8ecae6"
                    )
                    cont_rashodi = ax_monthly.bar(
                        x, troskovi, width=w, label="Rashodi", color="#ffb703"
                    )
                    cont_neto = ax_monthly.bar(
                        x_r, neto, width=w, label="Neto", color="#219ebc"
                    )
                    ax_monthly.set_title(f"Razlika Prihodi/Rashodi ({chart_currency_label()})")
                    ax_monthly.set_xticks(x)
                    ax_monthly.set_xticklabels(labels, rotation=0)
                    ax_monthly.grid(axis="y", alpha=0.25)
                    ax_monthly.legend(loc="upper left")

                    # Hover tooltip (one-time wiring per canvas)
                    tooltip = ctx.state.get("fin_monthly_tooltip")
                    if not tooltip:
                        annot = ax_monthly.annotate(
                            "",
                            xy=(0, 0),
                            xytext=(12, 12),
                            textcoords="offset points",
                            bbox=dict(boxstyle="round", fc="white", ec="#666666", alpha=0.95),
                            arrowprops=dict(arrowstyle="->", color="#666666"),
                        )
                        annot.set_visible(False)
                        tooltip = {"annot": annot, "items": []}
                        ctx.state["fin_monthly_tooltip"] = tooltip

                        def _on_fin_hover(event):
                            if event.inaxes != ax_monthly:
                                annot.set_visible(False)
                                canvas_monthly.draw_idle()
                                return
                            items = ctx.state.get("fin_monthly_tooltip", {}).get("items") or []
                            for patch, series, lab, val in items:
                                contains, _ = patch.contains(event)
                                if not contains:
                                    continue
                                xmid = patch.get_x() + patch.get_width() / 2.0
                                ytop = patch.get_y() + patch.get_height()
                                annot.xy = (xmid, ytop)
                                annot.set_text(f"{series} {lab}: {format_display_amount(val)}")
                                annot.set_visible(True)
                                canvas_monthly.draw_idle()
                                return
                            if annot.get_visible():
                                annot.set_visible(False)
                                canvas_monthly.draw_idle()

                        def _on_fin_leave(_event):
                            if annot.get_visible():
                                annot.set_visible(False)
                                canvas_monthly.draw_idle()

                        canvas_monthly.mpl_connect("motion_notify_event", _on_fin_hover)
                        canvas_monthly.mpl_connect("figure_leave_event", _on_fin_leave)

                    # Update hover items every refresh (new patches each draw)
                    items = []
                    for idx, patch in enumerate(cont_prihodi.patches):
                        if idx < len(labels):
                            items.append((patch, "Prihodi", labels[idx], bruto[idx]))
                    for idx, patch in enumerate(cont_rashodi.patches):
                        if idx < len(labels):
                            items.append((patch, "Rashodi", labels[idx], troskovi[idx]))
                    for idx, patch in enumerate(cont_neto.patches):
                        if idx < len(labels):
                            items.append((patch, "Neto", labels[idx], neto[idx]))
                    ctx.state.setdefault("fin_monthly_tooltip", {})["items"] = items
                canvas_monthly.draw()
        except Exception as exc:
            log_app_error("refresh_finansije", str(exc))
            try:
                if ctx.status_var is not None:
                    ctx.status_var.set(f"Finansije greska: {exc}")
            except Exception:
                pass
        finally:
            conn.close()

    def _truncate_label(text: str, max_chars: int) -> str:
        if max_chars <= 0:
            return ""
        if len(text) <= max_chars:
            return text
        if max_chars <= 3:
            return text[:max_chars]
        return text[: max_chars - 3] + "..."

    def _fit_two_line_label(
        name: str, count_label: str, value_text: str, ratio: float
    ) -> str:
        max_line1 = int(10 + ratio * 24)
        max_line2 = int(12 + ratio * 22)
        line1 = _truncate_label(name, max_line1)
        line2 = _truncate_label(f"{count_label} | {value_text}", max_line2)
        return f"{line1}\n{line2}"

    def refresh_charts():
        conn = get_conn()
        period_days = state.get("period_days")
        start = state.get("period_start")
        end = state.get("period_end")
        top_customers = get_top_customers(conn, 5, period_days, start, end)
        top_products = get_top_products(conn, 10, period_days, start, end)
        monthly = get_sp_bank_monthly(conn, period_days, start, end)
        conn.close()

        ax_customers.clear()
        if top_customers:
            values = [chart_value(r[2]) for r in top_customers]
            max_val = max(values) if values else 0
            labels = []
            for name, orders_cnt, net_total in top_customers:
                ratio = (chart_value(net_total) / max_val) if max_val else 0
                labels.append(
                    _fit_two_line_label(
                        str(name),
                        f"N: {int(orders_cnt or 0)}",
                        format_amount(net_total),
                        ratio,
                    )
                )
            labels_rev = labels[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(labels_rev)))
            bars = ax_customers.barh(y_pos, values_rev, color="#5aa9e6")
            ax_customers.set_title(f"Top 5 kupaca ({chart_currency_label()})")
            ax_customers.tick_params(axis="y", left=False, labelleft=False)
            outside_labels = []
            outside_positions = []
            inside_labels = []
            for bar, label in zip(bars, labels_rev):
                width = bar.get_width()
                ratio = (width / max_val) if max_val else 0
                if ratio < 0.22:
                    outside_labels.append(label)
                    outside_positions.append(bar)
                    inside_labels.append("")
                else:
                    inside_labels.append(label)
            ax_customers.bar_label(
                bars,
                labels=inside_labels,
                label_type="center",
                padding=0,
                color="white",
                fontsize=8,
            )
            if outside_positions:
                offset = max_val * 0.02 if max_val else 0.1
                for bar, label in zip(outside_positions, outside_labels):
                    ax_customers.text(
                        bar.get_width() + offset,
                        bar.get_y() + bar.get_height() / 2,
                        label,
                        va="center",
                        ha="left",
                        fontsize=8,
                        color="black",
                    )
        else:
            ax_customers.set_title("Top 5 kupaca (nema podataka)")
        canvas_customers.draw()

        ax_products.clear()
        if top_products:
            names = [f"{r[0]} ({int(r[1] or 0)})" for r in top_products]
            values = [chart_value(r[2]) for r in top_products]
            names_rev = names[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(names_rev)))
            bars = ax_products.barh(y_pos, values_rev, color="#7cb518")
            ax_products.set_title(f"Top 10 artikala ({chart_currency_label()})")
            ax_products.tick_params(axis="y", left=False, labelleft=False)
            ax_products.bar_label(
                bars,
                labels=names_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            ax_products.set_title("Top 10 artikala (nema podataka)")
        canvas_products.draw()

        ax_sp_bank.clear()
        if monthly:
            periods = [r[0] for r in monthly]
            income = [r[1] or 0 for r in monthly]
            expense = [r[2] or 0 for r in monthly]
            x = range(len(periods))
            ax_sp_bank.bar(x, income, width=0.4, label="Prihodi", color="#5aa9e6")
            ax_sp_bank.bar(
                [i + 0.4 for i in x],
                expense,
                width=0.4,
                label="Rashodi",
                color="#f28482",
            )
            ax_sp_bank.set_xticks(list(x))
            ax_sp_bank.set_xticklabels(periods, rotation=45, ha="right")
            ax_sp_bank.set_title("Prihodi vs rashodi (banka, BAM)")
            ax_sp_bank.legend()
        else:
            ax_sp_bank.set_title("Prihodi vs rashodi (nema podataka)")
        canvas_sp_bank.draw()

    def refresh_expenses():
        widgets = ctx.state.get("troskovi_widgets") or {}
        expense_total_var = widgets.get("expense_total_var")
        expense_year_var = widgets.get("expense_year_var")
        expense_year_menu = widgets.get("expense_year_menu")
        ax_expenses_top = widgets.get("ax_expenses_top")
        canvas_expenses_top = widgets.get("canvas_expenses_top")
        ax_expenses_month = widgets.get("ax_expenses_month")
        canvas_expenses_month = widgets.get("canvas_expenses_month")
        if (
            not expense_total_var
            or not expense_year_var
            or not expense_year_menu
            or not ax_expenses_top
            or not canvas_expenses_top
            or not ax_expenses_month
            or not canvas_expenses_month
        ):
            return
        conn = get_conn()
        years = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT substr(dtposted, 1, 4) "
                "FROM bank_transactions "
                "WHERE dtposted IS NOT NULL "
                "ORDER BY substr(dtposted, 1, 4)"
            ).fetchall()
            if row[0]
        ]
        if years:
            expense_year_menu.configure(values=["Sve"] + years)
            if expense_year_var.get() not in (["Sve"] + years):
                expense_year_var.set("Sve")
                state["expense_year"] = None
        period_days = state.get("expense_period_days")
        start = state.get("expense_period_start")
        end = state.get("expense_period_end")
        year = state.get("expense_year")
        month = state.get("expense_month")
        if year or month:
            period_days = None
            start = None
            end = None
        summary = get_expense_summary(conn, period_days, start, end, year, month)
        conn.close()

        total = summary["total"] or 0.0
        totals = summary["totals"]
        display = summary["display_names"]
        monthly = summary["monthly"]
        expense_total_var.set(f"Ukupno: {format_amount(total)}")

        top_n = state.get("expense_top_n", 5)
        top_items = sorted(totals.items(), key=lambda item: item[1], reverse=True)[
            :top_n
        ]

        ax_expenses_top.clear()
        if top_items:
            values = [chart_value(v) for _, v in top_items]
            max_val = max(values) if values else 0
            labels = []
            full_labels = []
            for key, val in top_items:
                name = display.get(key, key)
                share = (val / total * 100.0) if total else 0.0
                ratio = (chart_value(val) / max_val) if max_val else 0
                label = _fit_two_line_label(
                    name,
                    f"Udio: {share:.1f}%",
                    format_amount(val),
                    ratio,
                )
                labels.append(label)
                full_labels.append(f"{name}\nUdio: {share:.1f}% | {format_amount(val)}")
            labels_rev = labels[::-1]
            full_labels_rev = full_labels[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(labels_rev)))
            bars = ax_expenses_top.barh(y_pos, values_rev, color="#f28e2c")
            ax_expenses_top.set_title(
                f"Top {len(top_items)} troskova ({chart_currency_label()})"
            )
            ax_expenses_top.tick_params(axis="y", left=False, labelleft=False)
            outside_labels = []
            outside_positions = []
            inside_labels = []
            for bar, label, full_label in zip(bars, labels_rev, full_labels_rev):
                width = bar.get_width()
                ratio = (width / max_val) if max_val else 0
                if ratio < 0.22:
                    outside_labels.append(full_label)
                    outside_positions.append(bar)
                    inside_labels.append("")
                else:
                    inside_labels.append(label)
            ax_expenses_top.bar_label(
                bars,
                labels=inside_labels,
                label_type="center",
                padding=0,
                color="white",
                fontsize=8,
            )
            if outside_positions:
                offset = max_val * 0.02 if max_val else 0.1
                for bar, label in zip(outside_positions, outside_labels):
                    ax_expenses_top.text(
                        bar.get_width() + offset,
                        bar.get_y() + bar.get_height() / 2,
                        label,
                        va="center",
                        ha="left",
                        fontsize=8,
                        color="black",
                    )
                ax_expenses_top.set_xlim(0, max_val * 1.25 if max_val else 1)
        else:
            ax_expenses_top.set_title("Top troskovi (nema podataka)")
        canvas_expenses_top.draw()

        ax_expenses_month.clear()
        if monthly and top_items:
            months = sorted(monthly.keys())
            top_keys = [k for k, _ in top_items]
            bottoms = [0.0 for _ in months]
            for key in top_keys:
                series = [chart_value(monthly.get(m, {}).get(key, 0.0)) for m in months]
                total_val = totals.get(key, 0.0)
                share = (total_val / total * 100.0) if total else 0.0
                name = _short_expense_name(display.get(key, key))
                label = f"{name} | {format_amount(total_val)} | {share:.1f}%"
                ax_expenses_month.bar(months, series, bottom=bottoms, label=label)
                bottoms = [b + s for b, s in zip(bottoms, series)]
            ax_expenses_month.set_title(f"Troskovi po mjesecu (Top {len(top_items)})")
            ax_expenses_month.tick_params(axis="x", rotation=35)
            ax_expenses_month.legend(fontsize=7)
        else:
            ax_expenses_month.set_title("Troskovi po mjesecu (nema podataka)")
        canvas_expenses_month.draw()

    ctx.refresh_dashboard = refresh_dashboard

    def refresh_returns_charts():
        conn = get_conn()
        period_days = state.get("period_days")
        start = state.get("period_start")
        end = state.get("period_end")
        try:
            extract_bank_refunds(conn)
        except Exception as exc:
            log_app_error("extract_bank_refunds", str(exc))
        try:
            has_invoice_matches = (
                int(conn.execute("SELECT COUNT(*) FROM invoice_matches").fetchone()[0]) > 0
            )
        except Exception:
            has_invoice_matches = False
        top_customers = get_refund_top_customers(conn, 5, period_days, start, end)
        top_items = get_refund_top_items(conn, 5, period_days, start, end)
        top_categories = get_refund_top_categories(
            conn, 5, period_days, start, end, categorize_sku=kategorija_za_sifru
        )
        conn.close()
        widgets = ctx.state.get("povrati_widgets") or {}
        ax_ref_customers = widgets.get("ax_ref_customers")
        canvas_ref_customers = widgets.get("canvas_ref_customers")
        ax_ref_items = widgets.get("ax_ref_items")
        canvas_ref_items = widgets.get("canvas_ref_items")
        ax_ref_categories = widgets.get("ax_ref_categories")
        canvas_ref_categories = widgets.get("canvas_ref_categories")
        if (
            not ax_ref_customers
            or not canvas_ref_customers
            or not ax_ref_items
            or not canvas_ref_items
            or not ax_ref_categories
            or not canvas_ref_categories
        ):
            return

        ax_ref_customers.clear()
        if top_customers:
            values = [r[1] or 0 for r in top_customers]
            max_val = max(values) if values else 0
            labels = []
            for name, count, total_amount in top_customers:
                ratio = (float(count or 0) / max_val) if max_val else 0
                labels.append(
                    _fit_two_line_label(
                        str(name),
                        f"P: {int(count or 0)}",
                        format_amount(total_amount or 0),
                        ratio,
                    )
                )
            labels_rev = labels[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(labels_rev)))
            bars = ax_ref_customers.barh(y_pos, values_rev, color="#3a86ff")
            ax_ref_customers.set_title("Top 5 kupaca (povrati, broj)")
            ax_ref_customers.tick_params(axis="y", left=False, labelleft=False)
            ax_ref_customers.bar_label(
                bars,
                labels=labels_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            ax_ref_customers.set_title("Top 5 kupaca (povrati, nema podataka)")
        canvas_ref_customers.draw()

        ax_ref_items.clear()
        if top_items:
            names = [f"{r[0]} ({int(r[1] or 0)})" for r in top_items]
            values = [r[1] or 0 for r in top_items]
            names_rev = names[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(names_rev)))
            bars = ax_ref_items.barh(y_pos, values_rev, color="#2a9d8f")
            ax_ref_items.set_title("Top 5 artikala (povrati, kom)")
            ax_ref_items.tick_params(axis="y", left=False, labelleft=False)
            ax_ref_items.bar_label(
                bars,
                labels=names_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            if top_customers and not has_invoice_matches:
                ax_ref_items.set_title("Top 5 artikala (povrati) - potrebno uparivanje Minimax")
            else:
                ax_ref_items.set_title("Top 5 artikala (povrati, nema podataka)")
        canvas_ref_items.draw()

        ax_ref_categories.clear()
        if top_categories:
            names = [f"{r[0]} ({int(r[1] or 0)})" for r in top_categories]
            values = [r[1] or 0 for r in top_categories]
            names_rev = names[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(names_rev)))
            bars = ax_ref_categories.barh(y_pos, values_rev, color="#ff9f1c")
            ax_ref_categories.set_title("Top 5 grupa (povrati, kom)")
            ax_ref_categories.tick_params(axis="y", left=False, labelleft=False)
            ax_ref_categories.bar_label(
                bars,
                labels=names_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            if top_customers and not has_invoice_matches:
                ax_ref_categories.set_title("Top 5 grupa (povrati) - potrebno uparivanje Minimax")
            else:
                ax_ref_categories.set_title("Top 5 grupa (povrati, nema podataka)")
        canvas_ref_categories.draw()

    def refresh_unpicked_charts():
        widgets = ctx.state.get("nepreuzete_widgets") or {}
        lbl_unpicked_total = widgets.get("lbl_unpicked_total")
        lbl_unpicked_lost = widgets.get("lbl_unpicked_lost")
        lbl_unpicked_repeat = widgets.get("lbl_unpicked_repeat")
        txt_tracking_summary = widgets.get("txt_tracking_summary")
        txt_nepreuzete_orders = widgets.get("txt_nepreuzete_orders")
        ax_unpicked_customers = widgets.get("ax_unpicked_customers")
        canvas_unpicked_customers = widgets.get("canvas_unpicked_customers")
        ax_unpicked_items = widgets.get("ax_unpicked_items")
        canvas_unpicked_items = widgets.get("canvas_unpicked_items")
        if (
            not lbl_unpicked_total
            or not lbl_unpicked_lost
            or not lbl_unpicked_repeat
            or not txt_tracking_summary
            or not txt_nepreuzete_orders
            or not ax_unpicked_customers
            or not canvas_unpicked_customers
            or not ax_unpicked_items
            or not canvas_unpicked_items
        ):
            return
        conn = get_conn()
        period_days = state.get("unpicked_period_days")
        start = state.get("unpicked_period_start")
        end = state.get("unpicked_period_end")
        stats = get_unpicked_stats(conn, period_days, start, end)
        top_customers, _ = get_unpicked_customer_groups(
            conn, 10, period_days, start, end
        )
        top_items = get_unpicked_top_items(conn, 5, period_days, start, end)
        orders = get_unpicked_orders_list(conn, period_days, start, end)
        tracking_codes = [row[1] for row in orders if row[1]]
        summary_map = {}
        if tracking_codes:
            placeholders = ",".join("?" * len(tracking_codes))
            for row in conn.execute(
                "SELECT tracking_code, delivery_attempts, failure_reasons, returned_at, "
                "days_to_first_attempt, has_attempt_before_return, has_returned, anomalies "
                f"FROM tracking_summary WHERE tracking_code IN ({placeholders})",
                tracking_codes,
            ).fetchall():
                summary_map[row[0]] = row[1:]
        conn.close()

        lbl_unpicked_total.configure(text=f"Nepreuzete: {stats['unpicked_orders']}")
        lbl_unpicked_lost.configure(
            text=f"Izgubljena prodaja: {format_amount(stats['lost_sales'])}"
        )
        lbl_unpicked_repeat.configure(
            text=f"Kupci 2+ nepreuzetih: {stats['repeat_customers']}"
        )

        ax_unpicked_customers.clear()
        if top_customers:
            names = [f"{r[0]} ({int(r[1] or 0)})" for r in top_customers]
            values = [r[1] or 0 for r in top_customers]
            names_rev = names[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(names_rev)))
            bars = ax_unpicked_customers.barh(y_pos, values_rev, color="#ff7f50")
            ax_unpicked_customers.set_title("Top 10 kupaca (nepreuzete, broj)")
            ax_unpicked_customers.tick_params(axis="y", left=False, labelleft=False)
            ax_unpicked_customers.bar_label(
                bars,
                labels=names_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            ax_unpicked_customers.set_title("Top 10 kupaca (nepreuzete, nema podataka)")
        canvas_unpicked_customers.draw()

        ax_unpicked_items.clear()
        if top_items:
            names = [f"{r[0]} ({int(r[1] or 0)})" for r in top_items]
            values = [r[1] or 0 for r in top_items]
            names_rev = names[::-1]
            values_rev = values[::-1]
            y_pos = list(range(len(names_rev)))
            bars = ax_unpicked_items.barh(y_pos, values_rev, color="#2a9d8f")
            ax_unpicked_items.set_title("Top 5 artikala (nepreuzete, kom)")
            ax_unpicked_items.tick_params(axis="y", left=False, labelleft=False)
            ax_unpicked_items.bar_label(
                bars,
                labels=names_rev,
                label_type="center",
                padding=0,
                color="white",
                fontsize=9,
            )
        else:
            ax_unpicked_items.set_title("Top 5 artikala (nepreuzete, nema podataka)")
        canvas_unpicked_items.draw()

        attempts_count = {"0": 0, "1": 0, "2": 0, "3+": 0}
        reasons_count = {}
        anomalies_count = {}
        days_to_first = []
        out_to_return = []
        no_attempt_before_return = 0
        not_returned = 0
        one_attempt = 0
        late_first_attempt = 0

        for code, summary in summary_map.items():
            (
                attempts,
                reasons,
                returned_at,
                days_first,
                has_attempt_before_return,
                has_returned,
                anomalies,
            ) = summary
            attempts = int(attempts or 0)
            if has_returned == 0:
                not_returned += 1
            if has_returned == 1 and has_attempt_before_return == 0:
                no_attempt_before_return += 1
            if attempts == 1:
                one_attempt += 1
            if attempts >= 3:
                attempts_count["3+"] += 1
            elif attempts == 2:
                attempts_count["2"] += 1
            elif attempts == 1:
                attempts_count["1"] += 1
            else:
                attempts_count["0"] += 1
            if days_first is not None:
                try:
                    val = float(days_first)
                    days_to_first.append(val)
                    if val > 2:
                        late_first_attempt += 1
                except (TypeError, ValueError):
                    pass
            if returned_at and has_attempt_before_return:
                pass
            for reason in [r.strip() for r in (reasons or "").split(";") if r.strip()]:
                reasons_count[reason] = reasons_count.get(reason, 0) + 1
            for anomaly in [
                a.strip() for a in (anomalies or "").split(";") if a.strip()
            ]:
                anomalies_count[anomaly] = anomalies_count.get(anomaly, 0) + 1

        avg_days_first = (
            sum(days_to_first) / len(days_to_first) if days_to_first else None
        )
        med_days_first = statistics.median(days_to_first) if days_to_first else None
        top_reason = (
            max(reasons_count.items(), key=lambda x: x[1])[0] if reasons_count else ""
        )
        top_anomaly = (
            max(anomalies_count.items(), key=lambda x: x[1])[0]
            if anomalies_count
            else ""
        )

        txt_tracking_summary.delete("1.0", "end")
        txt_tracking_summary.insert("end", f"Najcesci razlog: {top_reason or '-'}\n")
        txt_tracking_summary.insert(
            "end", f"Najcesci nelogicni slijed: {top_anomaly or '-'}\n"
        )
        txt_tracking_summary.insert(
            "end", f"Bez pokusaja prije vracanja: {no_attempt_before_return}\n"
        )
        txt_tracking_summary.insert("end", f"Samo jedan pokusaj: {one_attempt}\n")
        txt_tracking_summary.insert(
            "end", f"Prvi pokusaj >2 dana: {late_first_attempt}\n"
        )
        txt_tracking_summary.insert(
            "end",
            (
                f"Prosjek/medijan dana do prvog pokusaja: "
                f"{avg_days_first:.2f}/{med_days_first:.2f}\n"
                if avg_days_first is not None and med_days_first is not None
                else "Prosjek/medijan dana do prvog pokusaja: -\n"
            ),
        )
        txt_tracking_summary.insert("end", f"Distribucija pokusaja: {attempts_count}\n")
        txt_tracking_summary.insert("end", f"Nisu vracene: {not_returned}\n")
        month_counts = {}
        for row in orders:
            created_at = row[7]
            if not created_at:
                continue
            ts = pd.to_datetime(
                created_at,
                errors="coerce",
                dayfirst="." in str(created_at) and "-" not in str(created_at),
            )
            if pd.isna(ts):
                continue
            key = ts.strftime("%Y-%m")
            month_counts[key] = month_counts.get(key, 0) + 1
        if month_counts:
            trend = ", ".join(f"{k}:{v}" for k, v in sorted(month_counts.items())[-6:])
            txt_tracking_summary.insert("end", f"Trend (zadnjih 6): {trend}\n")

        txt_nepreuzete_orders.delete("1.0", "end")
        if orders:
            for row in orders:
                (
                    sp_order_no,
                    tracking,
                    name,
                    phone,
                    email,
                    city,
                    status,
                    created_at,
                    picked_up_at,
                    delivered_at,
                    net_total,
                ) = row
                txt_nepreuzete_orders.insert(
                    "end",
                    f"{sp_order_no} | {tracking or '-'} | {name or ''} | {phone or ''} | {city or ''} | {format_amount(net_total)}\n",
                )
        else:
            txt_nepreuzete_orders.insert("end", "Nema podataka.\n")

    def refresh_poslovanje_lists():
        conn = get_conn()
        needs = get_needs_invoice_orders(conn, 100)
        unmatched = get_unmatched_orders_list(conn, 100)
        conn.close()
        widgets = ctx.state.get("poslovanje_widgets") or {}
        txt_needs = widgets.get("txt_needs")
        txt_unmatched = widgets.get("txt_unmatched")
        if not txt_needs or not txt_unmatched:
            return
        txt_needs.configure(state="normal")
        txt_needs.delete("1.0", "end")
        for idx, r in enumerate(needs, start=1):
            txt_needs.insert("end", f"{idx}. {r[0]} | {r[1]} | {r[2]} | {r[3]}\n")
        txt_needs.configure(state="disabled")

        txt_unmatched.configure(state="normal")
        txt_unmatched.delete("1.0", "end")
        for idx, r in enumerate(unmatched, start=1):
            txt_unmatched.insert("end", f"{idx}. {r[0]} | {r[1]} | {r[2]}\n")
        txt_unmatched.configure(state="disabled")

    ctx.refresh_poslovanje_lists = refresh_poslovanje_lists

    def run_export_basic():
        conn = get_conn()
        try:
            cols, rows = report_unmatched_orders(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "unmatched", "xlsx")
            cols, rows = report_unmatched_reasons(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "unmatched-reasons", "xlsx")
            cols, rows = report_conflicts(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "conflicts", "xlsx")
            cols, rows = report_nearest_invoice(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "nearest", "xlsx")
            cols, rows = report_needs_invoice_orders(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "needs-invoice-orders", "xlsx")
            cols, rows = report_sp_vs_bank(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "sp-vs-bank", "xlsx")
            cols, rows = report_refunds_without_storno(conn, return_rows=True)
            write_report(cols, rows, Path("exports"), "refunds-no-storno", "xlsx")
            messagebox.showinfo("OK", "Export zavrsen u folderu exports.")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def run_export_single(report_fn, name: str):
        conn = get_conn()
        try:
            cols, rows = report_fn(conn, return_rows=True)
            out_path = write_report(cols, rows, Path("exports"), name, "xlsx")
            messagebox.showinfo("OK", f"Export snimljen: {out_path}")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def export_unpaid_sp_orders():
        start_str, end_str = (None, None)
        conn = get_conn()
        try:
            cols, rows = get_unpaid_sp_orders_details(conn, start_str, end_str)
            ts = datetime.now().strftime("%d-%m-%Y_%H%M")
            out_path = write_report(
                cols, rows, Path("exports"), f"sp-neuplacene-posiljke-{ts}", "xlsx"
            )
            messagebox.showinfo("OK", f"Export snimljen: {out_path}")
        except Exception as exc:
            log_app_error("export_unpaid_sp", str(exc))
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def export_pending_sp_orders():
        conn = get_conn()
        try:
            cols, rows = get_pending_sp_orders_details(conn)
            ts = datetime.now().strftime("%d-%m-%Y_%H%M")
            out_path = write_report(
                cols, rows, Path("exports"), f"sp-na-cekanju-posiljke-{ts}", "xlsx"
            )
            messagebox.showinfo("OK", f"Export snimljen: {out_path}")
        except Exception as exc:
            log_app_error("export_pending_sp", str(exc))
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def export_finansije_neto_breakdown():
        fin_custom = bool(ctx.state.get("fin_period_custom"))
        if fin_custom:
            main_days = None
            main_start = ctx.state.get("fin_period_start")
            main_end = ctx.state.get("fin_period_end")
        else:
            main_days = state.get("period_days")
            main_start = state.get("period_start")
            main_end = state.get("period_end")
        start_str, end_str = _resolve_period_to_strings(main_days, main_start, main_end)
        conn = get_conn()
        try:
            cols, rows = get_neto_breakdown_by_orders(conn, main_days, start_str, end_str)
            ts = datetime.now().strftime("%d-%m-%Y_%H%M")
            out_path = write_report(
                cols, rows, Path("exports"), f"finansije-cash-breakdown-{ts}", "xlsx"
            )
            messagebox.showinfo("OK", f"Export snimljen: {out_path}")
        except Exception as exc:
            log_app_error("export_finansije_neto", str(exc))
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def open_exports():
        folder = Path("exports").resolve()
        folder.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(str(folder))
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))

    def set_db_path():
        path = filedialog.askopenfilename(
            title="Izaberi SQLite bazu",
            filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")],
        )
        if not path:
            return
        state["db_path"] = Path(path)
        save_app_settings({"db_path": path})
        ent_db.configure(state="normal")
        ent_db.delete(0, "end")
        ent_db.insert(0, path)
        ent_db.configure(state="readonly")
        refresh_dashboard()

    def run_import_folder(import_fn, title: str, pattern: str) -> int:
        folder = filedialog.askdirectory(title=title)
        if not folder:
            return 0
        files = sorted(Path(folder).glob(pattern))
        if not files:
            messagebox.showwarning("Info", f"Nema fajlova za import ({pattern}).")
            return 0
        log_app_event("import_folder", "start", title=title, folder=str(folder), pattern=pattern, files=len(files), db=str(state.get("db_path")))
        for btn in ctx.action_buttons or []:
            btn.configure(state="disabled")
        ctx.progress.configure(mode="determinate")
        ctx.progress.set(0)
        ctx.progress_pct_var.set("Napredak: 0%")
        conn = get_conn()
        imported = 0
        skipped = []
        failed = 0
        rejects = []
        try:
            total = len(files)
            for idx, path in enumerate(files, start=1):
                digest = file_hash(path)
                exists = conn.execute(
                    "SELECT 1 FROM import_runs WHERE file_hash = ?",
                    (digest,),
                ).fetchone()
                if exists:
                    skipped.append(path.name)
                    append_reject(
                        rejects, title, path.name, None, "file_already_imported", ""
                    )
                else:
                    try:
                        import_fn(conn, path, rejects)
                        imported += 1
                    except Exception as exc:
                        failed += 1
                        append_reject(
                            rejects,
                            title,
                            path.name,
                            None,
                            "file_failed",
                            str(exc),
                        )
                        log_app_error("import_folder", f"{title} {path.name}: {exc}")
                pct = idx / total if total else 1
                ctx.progress.set(pct)
                ctx.progress_pct_var.set(
                    f"Napredak: {int(pct * 100)}% ({idx}/{total})"
                )
                ctx.status_var.set(f"Uvoz: {idx}/{total}")
                app.update_idletasks()
            msg = f"Import zavrsen. Fajlova: {imported}."
            if skipped:
                preview = ", ".join(skipped[:8])
                more = f" (+{len(skipped) - 8})" if len(skipped) > 8 else ""
                msg += f"\nPreskoceno (vec u bazi): {preview}{more}"
            if failed:
                msg += f"\nGreska na fajlovima: {failed}"
            messagebox.showinfo("OK", msg)
            log_app_event("import_folder", "done", title=title, imported=imported, skipped=len(skipped), rejects=len(rejects))
            if rejects:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                out_path = Path("exports") / f"rejected-rows-{ts}.xlsx"
                pd.DataFrame(rejects).to_excel(out_path, index=False)
                try:
                    os.startfile(out_path)  # type: ignore[attr-defined]
                except Exception:
                    messagebox.showinfo("Info", f"Log odbijenih redova: {out_path}")
        except Exception as exc:
            log_app_error("import_folder", f"{title}: {exc}")
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
            ctx.status_var.set("Spremno.")
            for btn in ctx.action_buttons or []:
                btn.configure(state="normal")
        if ctx.refresh_dashboard:
            ctx.refresh_dashboard()
        else:
            refresh_dashboard()
        return imported

    def run_import_default_folder(
        folder: Path, import_fn, title: str, pattern: str, *, silent_missing: bool = False
    ) -> tuple[int, int]:
        if not folder.exists():
            if silent_missing:
                log_app_event("import_auto", "missing_folder", title=title, folder=str(folder), pattern=pattern, silent=True)
                return (0, 0)
            messagebox.showwarning("Info", f"Nema foldera: {folder}")
            log_app_event("import_auto", "missing_folder", title=title, folder=str(folder), pattern=pattern, silent=False)
            return (0, 0)
        files = sorted(folder.glob(pattern))
        if not files:
            if silent_missing:
                log_app_event("import_auto", "no_files", title=title, folder=str(folder), pattern=pattern, silent=True)
                return (0, 0)
            messagebox.showwarning("Info", f"Nema fajlova za import ({pattern}) u {folder}.")
            log_app_event("import_auto", "no_files", title=title, folder=str(folder), pattern=pattern, silent=False)
            return (0, 0)
        log_app_event("import_auto", "start", title=title, folder=str(folder), pattern=pattern, files=len(files), db=str(state.get("db_path")))
        for btn in ctx.action_buttons or []:
            btn.configure(state="disabled")
        ctx.progress.configure(mode="determinate")
        ctx.progress.set(0)
        ctx.progress_pct_var.set("Napredak: 0%")
        conn = get_conn()
        imported = 0
        skipped = 0
        failed = 0
        rejects = []
        try:
            total = len(files)
            for idx, path in enumerate(files, start=1):
                digest = file_hash(path)
                exists = conn.execute(
                    "SELECT 1 FROM import_runs WHERE file_hash = ?",
                    (digest,),
                ).fetchone()
                if exists:
                    skipped += 1
                    append_reject(
                        rejects, title, path.name, None, "file_already_imported", ""
                    )
                else:
                    try:
                        import_fn(conn, path, rejects)
                        imported += 1
                    except Exception as exc:
                        failed += 1
                        append_reject(
                            rejects,
                            title,
                            path.name,
                            None,
                            "file_failed",
                            str(exc),
                        )
                        log_app_error("import_auto", f"{title} {path.name}: {exc}")
                pct = idx / total if total else 1
                ctx.progress.set(pct)
                ctx.progress_pct_var.set(
                    f"Napredak: {int(pct * 100)}% ({idx}/{total})"
                )
                ctx.status_var.set(f"Uvoz: {idx}/{total}")
                app.update_idletasks()
            if rejects:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                out_path = Path("exports") / f"rejected-rows-{ts}.xlsx"
                pd.DataFrame(rejects).to_excel(out_path, index=False)
        except Exception as exc:
            log_app_error("import_auto", f"{title}: {exc}")
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
            ctx.status_var.set("Spremno.")
            for btn in ctx.action_buttons or []:
                btn.configure(state="normal")
        log_app_event("import_auto", "done", title=title, imported=imported, skipped=skipped, failed=failed, rejects=len(rejects))
        return (imported, skipped)

    def show_last_imports():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT MAX(CAST(sp_order_no AS INTEGER)) "
                "FROM orders WHERE sp_order_no IS NOT NULL AND TRIM(sp_order_no) != ''"
            )
            max_sp_orders = cur.fetchone()[0]
            cur.execute(
                "SELECT MAX(CAST(sp_order_no AS INTEGER)) "
                "FROM payments WHERE sp_order_no IS NOT NULL AND TRIM(sp_order_no) != ''"
            )
            max_sp_payments = cur.fetchone()[0]
            cur.execute(
                "SELECT MAX(CAST(sp_order_no AS INTEGER)) "
                "FROM returns WHERE sp_order_no IS NOT NULL AND TRIM(sp_order_no) != ''"
            )
            max_sp_returns = cur.fetchone()[0]
            cur.execute("SELECT number FROM invoices WHERE number IS NOT NULL")
            max_mm = None
            max_mm_num = None
            for (num,) in cur.fetchall():
                if not num:
                    continue
                digits = invoice_digits(num)
                if not digits:
                    continue
                try:
                    val = int(digits)
                except ValueError:
                    continue
                if max_mm_num is None or val > max_mm_num:
                    max_mm_num = val
                    max_mm = num
            cur.execute(
                "SELECT MAX(dtposted) FROM bank_transactions WHERE dtposted IS NOT NULL"
            )
            max_bank_date = cur.fetchone()[0]

            cur.execute("SELECT MAX(verified_at) FROM sp_prijemi_receipts WHERE verified_at IS NOT NULL")
            max_sp_prijemi_verified = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM sp_prijemi_receipts")
            sp_prijemi_count = cur.fetchone()[0]

            kartice_end = get_app_state(conn, "kartice_range_end")
            kartice_start = get_app_state(conn, "kartice_range_start")
            kartice_pdf = get_app_state(conn, "kartice_pdf_name")
        finally:
            conn.close()

        bank_msg = "Nema podataka"
        if max_bank_date:
            try:
                dt = pd.to_datetime(max_bank_date, errors="coerce")
                if not pd.isna(dt):
                    bank_msg = dt.strftime("%d-%m-%Y")
                else:
                    bank_msg = str(max_bank_date)
            except Exception:
                bank_msg = str(max_bank_date)

        kartice_msg = "-"
        if kartice_start or kartice_end or kartice_pdf:
            kartice_msg = f"{kartice_start or '-'}–{kartice_end or '-'} ({kartice_pdf or '-'})"

        messagebox.showinfo(
            "Zadnji uvozi",
            "\n".join(
                [
                    f"SP narudzbe (max): {max_sp_orders or '-'}",
                    f"Minimax racun (max): {max_mm or '-'}",
                    f"SP uplate (max): {max_sp_payments or '-'}",
                    f"SP preuzimanja (max): {max_sp_returns or '-'}",
                    f"SP prijemi (max verified): {max_sp_prijemi_verified or '-'} (prijema: {sp_prijemi_count or 0})",
                    f"Kartice artikala: {kartice_msg}",
                    f"Zadnji izvod (banka): {bank_msg}",
                ]
            ),
        )

    def run_action(action_fn):
        conn = get_conn()
        try:
            action_fn(conn)
            messagebox.showinfo("OK", "Akcija zavrsena.")
        except Exception as exc:
            log_app_error("run_action", str(exc))
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
        if ctx.refresh_dashboard:
            ctx.refresh_dashboard()
        else:
            refresh_dashboard()

    def set_baseline_lock():
        if state.get("baseline_locked"):
            return
        if not messagebox.askyesno(
            "Potvrda",
            "Ovo ce zakljucati trenutno stanje baze kao pocetno.\n"
            "Nakon toga su moguci samo novi uvozi.\n"
            "Zelite li nastaviti?",
        ):
            return
        conn = get_conn()
        try:
            set_app_state(conn, "baseline_locked", "1")
            set_app_state(
                conn, "baseline_locked_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        finally:
            conn.close()
        load_baseline_lock()
        update_baseline_ui()

    def set_reset_password():
        value = reset_pass_var.get().strip()
        if not value:
            messagebox.showerror("Greska", "Unesi lozinku za reset.")
            return
        conn = get_conn()
        try:
            set_app_state(conn, "reset_password_hash", hash_password(value))
        finally:
            conn.close()
        reset_pass_var.set("")
        messagebox.showinfo("OK", "Lozinka za reset je sacuvana.")

    def unlock_finansije_password() -> bool:
        conn = get_conn()
        try:
            stored_hash = get_app_state(conn, "reset_password_hash")
        finally:
            conn.close()
        if not stored_hash:
            messagebox.showerror(
                "Greska",
                "Nije postavljena lozinka za reset. Postavi je u Podesavanja aplikacije.",
            )
            return False
        typed = simpledialog.askstring(
            "Otključaj Finansije",
            "Unesi lozinku (ista kao za Reset):",
            show="*",
            parent=app,
        )
        if not typed or hash_password(typed.strip()) != stored_hash:
            return False
        ctx.state["finansije_unlocked"] = True
        return True

    reset_source_var = ctk.StringVar(value=RESET_SPECS[0].label)
    reset_key_by_label = {spec.label: spec.key for spec in RESET_SPECS}

    def run_reset_source():
        if state.get("baseline_locked"):
            messagebox.showerror("Greska", "Baza je zakljucana. Reset nije dozvoljen.")
            return
        label = reset_source_var.get()
        key = reset_key_by_label.get(label)
        if not key:
            messagebox.showerror("Greska", "Nepoznat izvor za reset.")
            return
        if not messagebox.askyesno(
            "Potvrda",
            "Ovo ce obrisati uvezene podatke za izabrani izvor (bez brisanja baze fajla).\n"
            "Nakon toga mozes ponovo uvoziti.\n\n"
            f"Izvor: {label}\n\n"
            "Nastaviti?",
        ):
            return
        conn = get_conn()
        try:
            stored_hash = get_app_state(conn, "reset_password_hash")
        finally:
            conn.close()
        if not stored_hash:
            messagebox.showerror("Greska", "Nije postavljena lozinka za reset.")
            return
        typed = reset_pass_var.get().strip()
        if not typed or hash_password(typed) != stored_hash:
            messagebox.showerror("Greska", "Pogresna lozinka za reset.")
            return

        backup_path = state["db_path"].with_suffix(
            f".bak-{datetime.now().strftime('%Y%m%d%H%M%S')}-reset-{key}"
        )
        try:
            if state["db_path"].exists():
                shutil.copy2(state["db_path"], backup_path)
        except Exception as exc:
            messagebox.showerror("Greska", f"Backup neuspjesan: {exc}")
            return

        conn = get_conn()
        try:
            deleted_runs = _reset_source(conn, key)
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
            return
        finally:
            conn.close()
        refresh_dashboard()
        messagebox.showinfo(
            "OK",
            f"Reset zavrsen ({label}).\n"
            f"Obrisano import runova: {deleted_runs}\n"
            f"Backup: {backup_path.name}",
        )

    def run_export_bank_refunds():
        conn = get_conn()
        try:
            extract_bank_refunds(conn)
            cols, rows = report_bank_refunds_extracted(conn, return_rows=True)
            out_path = write_report(
                cols, rows, Path("exports"), "bank-refunds-extracted", "xlsx"
            )
            messagebox.showinfo("OK", f"Export zavrsen: {out_path}")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
        refresh_dashboard()

    def run_export_refund_category(category: str):
        conn = get_conn()
        try:
            cols, rows = report_refund_items_category(
                conn,
                category,
                state.get("period_days"),
                state.get("period_start"),
                state.get("period_end"),
                return_rows=True,
                categorize_sku=kategorija_za_sifru,
            )
            out_path = write_report(
                cols,
                rows,
                Path("exports"),
                f"refund-items-{category.replace(' ', '-').lower()}",
                "xlsx",
            )
            try:
                os.startfile(out_path)  # type: ignore[attr-defined]
            except Exception:
                messagebox.showinfo("Info", f"Export zavrsen: {out_path}")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
        refresh_dashboard()

    def run_export_refunds_full():
        conn = get_conn()
        try:
            extract_bank_refunds(conn)
            period_days = state.get("period_days")
            start = state.get("period_start")
            end = state.get("period_end")

            items_totals = build_refund_item_totals(conn, period_days, start, end)
            items_rows = sorted(items_totals.items(), key=lambda x: x[1], reverse=True)
            items_df = pd.DataFrame(items_rows, columns=["sku", "qty_refund"])

            by_cat = {}
            for sku, qty in items_rows:
                cat = kategorija_za_sifru(str(sku))
                by_cat[cat] = by_cat.get(cat, 0.0) + float(qty or 0)
            cat_rows = sorted(by_cat.items(), key=lambda x: x[1], reverse=True)
            cat_df = pd.DataFrame(cat_rows, columns=["category", "qty_refund"])

            refunds = conn.execute(
                "SELECT br.bank_txn_id, bt.dtposted, bt.amount, bt.purpose, bt.payee_name, "
                "bt.stmt_number, br.invoice_no, br.invoice_no_digits, br.invoice_no_source, br.reason "
                "FROM bank_refunds br "
                "JOIN bank_transactions bt ON bt.id = br.bank_txn_id"
            ).fetchall()

            inv_by_number = {}
            inv_by_digits = {}
            for inv_id, inv_no, inv_name in conn.execute(
                "SELECT id, number, customer_name FROM invoices"
            ).fetchall():
                inv_by_number[str(inv_no or "")] = (int(inv_id), inv_name)
                digits = invoice_digits(inv_no)
                if digits:
                    inv_by_digits.setdefault(digits, set()).add((int(inv_id), inv_name))

            storno_map = {
                int(r[0]): int(r[1])
                for r in conn.execute(
                    "SELECT storno_invoice_id, original_invoice_id FROM invoice_storno"
                ).fetchall()
            }
            order_by_invoice = {
                int(inv_id): int(order_id)
                for inv_id, order_id in conn.execute(
                    "SELECT invoice_id, order_id FROM invoice_matches"
                ).fetchall()
            }
            order_name = {
                int(oid): name
                for oid, name in conn.execute(
                    "SELECT id, customer_name FROM orders"
                ).fetchall()
            }
            items_by_order = {}
            for oid, sku, qty in conn.execute(
                "SELECT order_id, product_code, qty FROM order_items"
            ).fetchall():
                if not sku:
                    continue
                items_by_order.setdefault(int(oid), []).append(
                    (str(sku), float(qty or 0))
                )

            detail_rows = []
            for (
                bank_txn_id,
                dtposted,
                amount,
                purpose,
                payee_name,
                stmt_number,
                inv_no,
                inv_digits,
                inv_source,
                reason,
            ) in refunds:
                inv_id = None
                inv_name = None
                if inv_no and str(inv_no) in inv_by_number:
                    inv_id, inv_name = inv_by_number[str(inv_no)]
                elif (
                    inv_digits
                    and inv_digits in inv_by_digits
                    and len(inv_by_digits[inv_digits]) == 1
                ):
                    inv_id, inv_name = next(iter(inv_by_digits[inv_digits]))
                if inv_id in storno_map:
                    inv_id = storno_map[inv_id]
                order_id = order_by_invoice.get(inv_id) if inv_id else None
                customer = None
                if order_id:
                    customer = order_name.get(order_id)
                if not customer:
                    customer = inv_name or payee_name
                sku_list = ""
                if order_id and order_id in items_by_order:
                    sku_list = "; ".join(
                        f"{sku} x{int(qty)}" for sku, qty in items_by_order[order_id]
                    )
                detail_rows.append(
                    (
                        bank_txn_id,
                        dtposted,
                        amount,
                        stmt_number,
                        customer,
                        purpose,
                        inv_no,
                        inv_digits,
                        inv_source,
                        reason,
                        inv_id,
                        order_id,
                        sku_list,
                    )
                )
            detail_df = pd.DataFrame(
                detail_rows,
                columns=[
                    "bank_txn_id",
                    "dtposted",
                    "amount",
                    "stmt_number",
                    "customer_name",
                    "purpose",
                    "invoice_no",
                    "invoice_no_digits",
                    "invoice_no_source",
                    "reason",
                    "invoice_id",
                    "order_id",
                    "sku_list",
                ],
            )

            out_path = Path("exports") / "refunds-full.xlsx"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                items_df.to_excel(writer, sheet_name="items", index=False)
                cat_df.to_excel(writer, sheet_name="categories", index=False)
                detail_df.to_excel(writer, sheet_name="refunds", index=False)
            try:
                os.startfile(out_path)  # type: ignore[attr-defined]
            except Exception:
                messagebox.showinfo("Info", f"Export zavrsen: {out_path}")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()
        refresh_dashboard()

    def run_export_unpicked_full():
        conn = get_conn()
        try:
            period_days = state.get("unpicked_period_days")
            start = state.get("unpicked_period_start")
            end = state.get("unpicked_period_end")
            stats = get_unpicked_stats(conn, period_days, start, end)
            items = get_unpicked_top_items(conn, None, period_days, start, end)
            categories = get_unpicked_category_totals(
                conn, period_days, start, end, categorize_sku=kategorija_za_sifru
            )
            orders = get_unpicked_orders_list(conn, period_days, start, end)

            items_rows = []
            for sku, qty, net in items:
                items_rows.append((sku, qty, net, kategorija_za_sifru(str(sku))))
            items_df = pd.DataFrame(
                items_rows,
                columns=["sku", "qty", "net_total", "category"],
            )
            cat_df = pd.DataFrame(
                categories,
                columns=["category", "qty", "net_total"],
            )
            tracking_codes = [row[1] for row in orders if row[1]]
            summary_map = {}
            if tracking_codes:
                placeholders = ",".join("?" * len(tracking_codes))
                for row in conn.execute(
                    "SELECT tracking_code, received_at, first_out_for_delivery_at, "
                    "delivery_attempts, failure_reasons, returned_at, days_to_first_attempt, "
                    "has_attempt_before_return, has_returned, anomalies, last_status, last_status_at "
                    f"FROM tracking_summary WHERE tracking_code IN ({placeholders})",
                    tracking_codes,
                ).fetchall():
                    summary_map[row[0]] = row[1:]

            analysis_metrics = []
            reasons_count = {}
            anomalies_count = {}
            attempts_count = {"0": 0, "1": 0, "2": 0, "3+": 0}
            days_to_first = []
            out_to_return = []
            no_attempt_before_return = 0
            not_returned = 0
            one_attempt = 0
            late_first_attempt = 0

            for code in tracking_codes:
                summary = summary_map.get(code)
                if not summary:
                    continue
                received_at = summary[0]
                first_out = summary[1]
                attempts = summary[2] or 0
                reasons = summary[3] or ""
                returned_at = summary[4]
                days_first = summary[5]
                has_attempt_before_return = summary[6]
                has_returned = summary[7]
                anomalies = summary[8] or ""

                if has_returned == 0:
                    not_returned += 1
                if has_returned == 1 and has_attempt_before_return == 0:
                    no_attempt_before_return += 1
                if attempts == 1:
                    one_attempt += 1
                if attempts >= 3:
                    attempts_count["3+"] += 1
                elif attempts == 2:
                    attempts_count["2"] += 1
                elif attempts == 1:
                    attempts_count["1"] += 1
                else:
                    attempts_count["0"] += 1

                if days_first is not None:
                    try:
                        days_first_val = float(days_first)
                        days_to_first.append(days_first_val)
                        if days_first_val > 2:
                            late_first_attempt += 1
                    except (TypeError, ValueError):
                        pass

                if first_out and returned_at:
                    try:
                        dt_out = pd.to_datetime(first_out)
                        dt_ret = pd.to_datetime(returned_at)
                        out_to_return.append(
                            (dt_ret - dt_out).total_seconds() / 86400.0
                        )
                    except Exception:
                        pass

                for reason in [r.strip() for r in reasons.split(";") if r.strip()]:
                    reasons_count[reason] = reasons_count.get(reason, 0) + 1
                for anomaly in [a.strip() for a in anomalies.split(";") if a.strip()]:
                    anomalies_count[anomaly] = anomalies_count.get(anomaly, 0) + 1

            avg_days_first = (
                sum(days_to_first) / len(days_to_first) if days_to_first else None
            )
            median_days_first = (
                statistics.median(days_to_first) if days_to_first else None
            )
            avg_out_to_return = (
                sum(out_to_return) / len(out_to_return) if out_to_return else None
            )
            median_out_to_return = (
                statistics.median(out_to_return) if out_to_return else None
            )

            top_reason = None
            if reasons_count:
                top_reason = max(reasons_count.items(), key=lambda x: x[1])[0]

            analysis_metrics.extend(
                [
                    ("posiljke_ukupno", len(tracking_codes)),
                    ("posiljke_nisu_vracene", not_returned),
                    ("posiljke_bez_pokusaja_prije_vracanja", no_attempt_before_return),
                    ("posiljke_samo_jedan_pokusaj", one_attempt),
                    ("posiljke_prvi_pokusaj_>2_dana", late_first_attempt),
                    ("prosjek_dana_do_prvog_pokusaja", avg_days_first),
                    ("medijan_dana_do_prvog_pokusaja", median_days_first),
                    ("prosjek_dana_od_zaduzenja_do_vracanja", avg_out_to_return),
                    ("medijan_dana_od_zaduzenja_do_vracanja", median_out_to_return),
                    ("najcesci_razlog_nedostavljanja", top_reason or ""),
                ]
            )

            orders_rows = []
            for row in orders:
                (
                    sp_order_no,
                    tracking_code,
                    customer_name,
                    phone,
                    email,
                    city,
                    status,
                    created_at,
                    picked_up_at,
                    delivered_at,
                    net_total,
                ) = row
                summary = summary_map.get(tracking_code, (None,) * 11)
                tracking_url = (
                    tracking_public_url(tracking_code) if tracking_code else None
                )
                orders_rows.append(
                    (
                        sp_order_no,
                        tracking_code,
                        tracking_url,
                        customer_name,
                        phone,
                        email,
                        city,
                        status,
                        created_at,
                        picked_up_at,
                        delivered_at,
                        net_total,
                    )
                    + summary
                )

            orders_df = pd.DataFrame(
                orders_rows,
                columns=[
                    "sp_order_no",
                    "tracking_code",
                    "tracking_url",
                    "customer_name",
                    "phone",
                    "email",
                    "city",
                    "status",
                    "created_at",
                    "picked_up_at",
                    "delivered_at",
                    "net_total",
                    "received_at",
                    "first_out_for_delivery_at",
                    "delivery_attempts",
                    "failure_reasons",
                    "returned_at",
                    "days_to_first_attempt",
                    "has_attempt_before_return",
                    "has_returned",
                    "anomalies",
                    "last_status",
                    "last_status_at",
                ],
            )
            summary_df = pd.DataFrame(
                [
                    ("unpicked_orders", stats["unpicked_orders"]),
                    ("lost_sales", stats["lost_sales"]),
                    ("repeat_customers", stats["repeat_customers"]),
                ],
                columns=["metric", "value"],
            )
            analysis_df = pd.DataFrame(analysis_metrics, columns=["metric", "value"])
            reasons_df = pd.DataFrame(
                sorted(reasons_count.items(), key=lambda x: x[1], reverse=True),
                columns=["reason", "count"],
            )
            anomalies_df = pd.DataFrame(
                sorted(anomalies_count.items(), key=lambda x: x[1], reverse=True),
                columns=["anomaly", "count"],
            )
            attempts_df = pd.DataFrame(
                [(k, v) for k, v in attempts_count.items()],
                columns=["attempts", "count"],
            )

            out_path = Path("exports") / "unpicked-full.xlsx"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                summary_df.to_excel(writer, sheet_name="summary", index=False)
                items_df.to_excel(writer, sheet_name="items", index=False)
                cat_df.to_excel(writer, sheet_name="categories", index=False)
                orders_df.to_excel(writer, sheet_name="orders", index=False)
                analysis_df.to_excel(
                    writer, sheet_name="tracking-analysis", index=False
                )
                reasons_df.to_excel(writer, sheet_name="tracking-reasons", index=False)
                anomalies_df.to_excel(
                    writer, sheet_name="tracking-anomalies", index=False
                )
                attempts_df.to_excel(
                    writer, sheet_name="tracking-attempts", index=False
                )
            try:
                os.startfile(out_path)  # type: ignore[attr-defined]
            except Exception:
                messagebox.showinfo("Info", f"Export zavrsen: {out_path}")
        except Exception as exc:
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def run_export_audit():
        conn = get_conn()
        try:
            imports_df = pd.read_sql_query(
                "SELECT * FROM import_runs ORDER BY imported_at", conn
            )
            tasks_df = pd.read_sql_query(
                "SELECT * FROM task_progress ORDER BY updated_at DESC", conn
            )
            actions_df = pd.read_sql_query(
                "SELECT * FROM action_log ORDER BY created_at DESC", conn
            )
            tracking_df = pd.read_sql_query(
                "SELECT * FROM tracking_summary ORDER BY last_fetched_at DESC", conn
            )

            log_path = Path("exports") / "tracking-log.csv"
            if log_path.exists():
                tracking_log_df = pd.read_csv(log_path)
            else:
                tracking_log_df = pd.DataFrame(
                    columns=[
                        "timestamp",
                        "tracking_code",
                        "url",
                        "status_code",
                        "latency_ms",
                        "result",
                        "error",
                    ]
                )

            err_path = Path("exports") / "app-errors.log"
            if err_path.exists():
                error_lines = err_path.read_text(encoding="utf-8").splitlines()
                errors_df = pd.DataFrame(error_lines, columns=["error"])
            else:
                errors_df = pd.DataFrame(columns=["error"])

            out_path = Path("exports") / "audit.xlsx"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                imports_df.to_excel(writer, sheet_name="imports", index=False)
                tasks_df.to_excel(writer, sheet_name="tasks", index=False)
                actions_df.to_excel(writer, sheet_name="actions", index=False)
                tracking_df.to_excel(writer, sheet_name="tracking_summary", index=False)
                tracking_log_df.to_excel(writer, sheet_name="tracking_log", index=False)
                errors_df.to_excel(writer, sheet_name="errors", index=False)
            try:
                os.startfile(out_path)  # type: ignore[attr-defined]
            except Exception:
                messagebox.showinfo("Info", f"Audit export: {out_path}")
        except Exception as exc:
            log_app_error("audit_export", str(exc))
            messagebox.showerror("Greska", str(exc))
        finally:
            conn.close()

    def poll_tracking_progress():
        if not state.get("tracking_polling"):
            return
        info = get_progress_info("tracking")
        widgets = ctx.state.get("nepreuzete_widgets") or {}
        tracking_status_var = widgets.get("tracking_status_var")
        if not tracking_status_var:
            return
        if not info:
            tracking_status_var.set("Dexpress: cekanje...")
            app.after(1000, poll_tracking_progress)
            return
        total = info.get("total", 0)
        processed = info.get("processed", 0)
        if total <= 0:
            tracking_status_var.set("Dexpress: nema podataka")
            state["tracking_polling"] = False
            return
        pct = int((processed / total) * 100)
        tracking_status_var.set(f"Dexpress: {processed}/{total} ({pct}%)")
        if processed >= total:
            tracking_status_var.set(f"Dexpress: zavrseno ({processed}/{total})")
            state["tracking_polling"] = False
            refresh_dashboard()
            return
        app.after(1000, poll_tracking_progress)

    def run_dexpress_tracking():
        widgets = ctx.state.get("nepreuzete_widgets") or {}
        tracking_status_var = widgets.get("tracking_status_var")
        tracking_batch_var = widgets.get("tracking_batch_var")
        tracking_force_var = widgets.get("tracking_force_var")
        if not tracking_status_var or not tracking_batch_var or tracking_force_var is None:
            messagebox.showerror("Greska", "UI (Nepreuzete) nije inicijalizovan.")
            return
        state["tracking_polling"] = True
        tracking_status_var.set("Dexpress: pokrenuto...")
        try:
            batch = int(tracking_batch_var.get().strip() or "20")
            if batch <= 0:
                batch = 20
        except ValueError:
            batch = 20
        force_flag = 1 if tracking_force_var.get() else 0
        run_action_async_process(
            run_tracking_process,
            [str(state["db_path"]), batch, force_flag],
            "Dexpress analiza",
            progress_task="tracking",
        )
        app.after(1000, poll_tracking_progress)

    test_session_rows = []
    test_session_index = 0
    test_session_log = []
    test_session_path = None

    def _update_test_view():
        nonlocal test_session_index
        widgets = ctx.state.get("poslovanje_widgets") or {}
        lbl_test_status = widgets.get("lbl_test_status")
        txt_test_order = widgets.get("txt_test_order")
        txt_test_invoice = widgets.get("txt_test_invoice")
        if not lbl_test_status or not txt_test_order or not txt_test_invoice:
            return
        txt_test_order.delete("1.0", "end")
        txt_test_invoice.delete("1.0", "end")
        if not test_session_rows:
            lbl_test_status.configure(text="Nema podataka za test.")
            return
        if test_session_index >= len(test_session_rows):
            lbl_test_status.configure(text="Sesija zavrsena.")
            return
        item = test_session_rows[test_session_index]
        lbl_test_status.configure(
            text=f"{test_session_index + 1}/{len(test_session_rows)} (score {item['score']})"
        )
        txt_test_order.insert(
            "end",
            f"SP broj: {item['sp_order_no']}\n"
            f"Ime: {item['order_name']}\n"
            f"Datum: {item['order_date']}\n"
            f"Iznos (RSD): {format_amount_rsd(item['order_amount'])}\n",
        )
        txt_test_invoice.insert(
            "end",
            f"Racun: {item['invoice_no']}\n"
            f"Kupac: {item['invoice_name']}\n"
            f"Datum: {item['invoice_date']}\n"
            f"Iznos (RSD): {format_amount_rsd(item['invoice_amount'])}\n",
        )

    def _save_test_log():
        if not test_session_path:
            return
        df = pd.DataFrame(test_session_log)
        df.to_excel(test_session_path, index=False)

    def _record_decision(decision: str):
        nonlocal test_session_index
        if not test_session_rows or test_session_index >= len(test_session_rows):
            return
        item = test_session_rows[test_session_index]
        test_session_log.append(
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "match_id": item["match_id"],
                "score": item["score"],
                "sp_order_no": item["sp_order_no"],
                "order_name": item["order_name"],
                "order_date": item["order_date"],
                "order_amount": item["order_amount"],
                "invoice_no": item["invoice_no"],
                "invoice_name": item["invoice_name"],
                "invoice_date": item["invoice_date"],
                "invoice_amount": item["invoice_amount"],
                "decision": decision,
            }
        )
        _save_test_log()
        test_session_index += 1
        _update_test_view()

    def start_test_session(mode: str):
        nonlocal test_session_rows, test_session_index, test_session_log, test_session_path
        widgets = ctx.state.get("poslovanje_widgets") or {}
        lbl_test_status = widgets.get("lbl_test_status")
        txt_test_order = widgets.get("txt_test_order")
        txt_test_invoice = widgets.get("txt_test_invoice")
        test_size_var = widgets.get("test_size_var")
        if not lbl_test_status or not txt_test_order or not txt_test_invoice or not test_size_var:
            messagebox.showerror("Greska", "UI (test uparivanja) nije inicijalizovan.")
            return
        try:
            size = int(test_size_var.get().strip() or "30")
        except ValueError:
            size = 30
        if size <= 0:
            size = 30
        conn = get_conn()
        try:
            if mode == "all":
                test_session_rows = load_all_match_samples(conn, size)
            else:
                test_session_rows = load_review_samples(conn, size)
        finally:
            conn.close()
        if not test_session_rows:
            msg = (
                "Nema sumnjivih matchova za test."
                if mode != "all"
                else "Nema matchova za test."
            )
            messagebox.showinfo("Info", msg)
            lbl_test_status.configure(text="Nema podataka.")
            return
        test_session_index = 0
        test_session_log = []
        exports_dir = Path("exports")
        exports_dir.mkdir(parents=True, exist_ok=True)
        test_session_path = (
            exports_dir
            / f"match-test-log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        _update_test_view()

    def stop_test_session():
        nonlocal test_session_rows, test_session_index
        widgets = ctx.state.get("poslovanje_widgets") or {}
        lbl_test_status = widgets.get("lbl_test_status")
        txt_test_order = widgets.get("txt_test_order")
        txt_test_invoice = widgets.get("txt_test_invoice")
        if not lbl_test_status or not txt_test_order or not txt_test_invoice:
            return
        test_session_rows = []
        test_session_index = 0
        lbl_test_status.configure(text="Sesija prekinuta.")
        txt_test_order.delete("1.0", "end")
        txt_test_invoice.delete("1.0", "end")

    def run_reset_minimax_matches():
        if not messagebox.askyesno(
            "Potvrda",
            "Ovo ce obrisati sva Minimax uparivanja i kandidate.\n"
            "Zelite li nastaviti?",
        ):
            return
        run_action(reset_minimax_matches)

    def run_action_async(action_fn, label: str):
        for btn in ctx.action_buttons or []:
            btn.configure(state="disabled")
        ctx.status_var.set(f"Radi: {label}...")
        ctx.progress.configure(mode="indeterminate")
        ctx.progress_pct_var.set("")
        ctx.progress.start()

        result = {"error": None}

        def worker():
            conn = get_conn()
            try:
                action_fn(conn)
            except Exception as exc:
                result["error"] = exc
            finally:
                conn.close()
            app.after(0, on_done)

        def on_done():
            ctx.progress.stop()
            for btn in ctx.action_buttons or []:
                btn.configure(state="normal")
            if result["error"]:
                log_app_error(label, str(result["error"]))
                messagebox.showerror("Greska", str(result["error"]))
                ctx.status_var.set("Greska.")
            else:
                ctx.status_var.set("Zavrseno.")
                if ctx.refresh_dashboard:
                    ctx.refresh_dashboard()
                else:
                    refresh_dashboard()
                if ctx.refresh_poslovanje_lists:
                    ctx.refresh_poslovanje_lists()
                else:
                    refresh_poslovanje_lists()

        threading.Thread(target=worker, daemon=True).start()

    def run_action_async_process(
        fn, args, label: str, progress_task: str | None = None, on_success=None
    ):
        for btn in ctx.action_buttons or []:
            btn.configure(state="disabled")
        ctx.status_var.set(f"Radi: {label}...")
        task_status_var.set(f"Task: {label}")
        last_error_var.set("Zadnja greska: -")
        if progress_task:
            ctx.progress.configure(mode="determinate")
            ctx.progress.set(0)
            ctx.progress_pct_var.set("Napredak: 0%")
            ctx.progress_eta_var.set("ETA: --")
            state["progress_start"] = time.time()
        else:
            ctx.progress.configure(mode="indeterminate")
            ctx.progress_pct_var.set("")
            ctx.progress_eta_var.set("")
            ctx.progress.start()

        future = ctx.executor.submit(fn, *args)

        def poll():
            if progress_task:
                info = get_progress_info(progress_task)
                if info is not None:
                    total = info["total"]
                    processed = info["processed"]
                    pct = min(1.0, processed / total) if total > 0 else 0.0
                    if processed > 0 and pct < 0.01:
                        pct_display = 0.01
                    else:
                        pct_display = pct
                    ctx.progress.set(pct_display)
                    if processed == 0:
                        ctx.progress_pct_var.set("Priprema...")
                        ctx.progress_eta_var.set("ETA: --")
                    else:
                        ctx.progress_pct_var.set(f"Napredak: {int(pct * 100)}%")
                        elapsed = time.time() - state.get("progress_start", time.time())
                        eta = (
                            elapsed * (total - processed) / processed
                            if processed > 0
                            else None
                        )
                        ctx.progress_eta_var.set(f"ETA: {format_eta(eta)}")
                    try:
                        if progress_task == REGEN_TASK:
                            regen_progress.set(pct_display)
                            regen_progress_var.set(
                                f"Regenerisanje metrika: {int(pct * 100)}%"
                            )
                    except Exception:
                        pass
            if not future.done():
                app.after(5000, poll)
                return
            if progress_task:
                ctx.progress.set(1)
                ctx.progress_pct_var.set("Napredak: 100%")
                ctx.progress_eta_var.set("ETA: 0s")
                try:
                    if progress_task == REGEN_TASK:
                        regen_progress.set(1)
                        regen_progress_var.set("Regenerisanje metrika: 100%")
                except Exception:
                    pass
            else:
                ctx.progress.stop()
            for btn in ctx.action_buttons or []:
                btn.configure(state="normal")
            try:
                future.result()
            except Exception as exc:
                log_app_error(label, str(exc))
                last_error_var.set(f"Zadnja greska: {exc}")
                task_status_var.set("Task: greska")
                try:
                    if progress_task == REGEN_TASK:
                        regen_progress_var.set("Regenerisanje metrika: greska")
                except Exception:
                    pass
                messagebox.showerror("Greska", str(exc))
                ctx.status_var.set("Greska.")
                return
            ctx.status_var.set("Zavrseno.")
            task_status_var.set("Task: zavrseno")
            if ctx.refresh_dashboard:
                ctx.refresh_dashboard()
            else:
                refresh_dashboard()
            if ctx.refresh_poslovanje_lists:
                ctx.refresh_poslovanje_lists()
            else:
                refresh_poslovanje_lists()
            if on_success is not None:
                try:
                    on_success()
                except Exception:
                    pass
            try:
                if progress_task == REGEN_TASK:
                    regen_progress_var.set("Regenerisanje metrika: zavrseno")
            except Exception:
                pass

        poll()

    def run_financial_refresh_chain(*, after_done=None):
        def _done():
            if after_done is not None:
                try:
                    after_done()
                except Exception:
                    pass

        def after_close():
            log_app_event("financial_refresh", "done", db=str(state.get("db_path")))
            _done()

        def after_bank():
            try:
                do_close = messagebox.askyesno(
                    "Finansije",
                    "Match Minimax + Match Banka su zavrseni.\n\n"
                    "Želiš li sada pokrenuti 'Zatvori račune'?",
                )
            except Exception:
                do_close = False
            if not do_close:
                after_close()
                return
            run_action_async_process(
                run_close_invoices_process,
                [str(state["db_path"])],
                "Zatvori racune",
                progress_task=None,
                on_success=after_close,
            )

        def after_minimax():
            run_action_async_process(
                run_match_bank_process,
                [str(state["db_path"]), 2],
                "Match Banka",
                progress_task="match_bank",
                on_success=after_bank,
            )

        log_app_event("financial_refresh", "start", db=str(state.get("db_path")))
        run_action_async_process(
            run_match_minimax_process,
            [str(state["db_path"])],
            "Match Minimax",
            progress_task="match_minimax",
            on_success=after_minimax,
        )

    def maybe_prompt_financial_refresh(*, after_done=None, imported_any: bool = False):
        if not imported_any:
            if after_done is not None:
                after_done()
            return
        ok = messagebox.askyesno(
            "Osvježi finansije",
            "Uvezeni su novi podaci.\n\n"
            "Želiš li osvježiti finansijske podatke?\n"
            "(Match Minimax + Match Banka)",
        )
        if not ok:
            if after_done is not None:
                after_done()
            return
        run_financial_refresh_chain(after_done=after_done)

    top = ctk.CTkFrame(app)
    top.pack(fill="x", padx=12, pady=8)

    def on_global_refresh():
        ok = messagebox.askyesno(
            "Osvježi",
            "Želiš li osvježiti finansijske podatke?\n"
            "(Match Minimax + Match Banka)\n\n"
            "Ako odabereš 'Ne', aplikacija će samo osvježiti prikaz.",
        )
        if ok:
            run_financial_refresh_chain(after_done=refresh_dashboard)
            return
        refresh_dashboard()

    ctk.CTkButton(top, text="Osvjezi", command=on_global_refresh).pack(
        side="left", padx=6
    )

    def refresh_external_views():
        _sku_daily_cache["df"] = pd.DataFrame()
        _sku_daily_cache["mtime"] = None
        _sku_summary_cache["df"] = pd.DataFrame()
        _sku_summary_cache["mtime"] = None
        _promo_cache["df"] = pd.DataFrame()
        _promo_cache["mtime"] = None
        try:
            refresh_kartice_status()
        except Exception:
            pass
        try:
            refresh_dashboard()
        except Exception:
            pass
        try:
            refresh_prodaja_views()
        except Exception:
            pass

    PDF_ROOT = Path("Kartice artikala")
    PRIJEMI_ROOT = Path("SP Prijemi")
    METRICS_OUT = Path("Kalkulacije_kartice_art/izlaz")
    SP_DB = state["db_path"]
    REGEN_TASK = "regen_metrics"
    regen_progress_var = ctk.StringVar(value="")

    def _latest_file(root: Path, pattern: str) -> Path | None:
        candidates = list(root.rglob(pattern)) if root.exists() else []
        candidates = [p for p in candidates if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]

    def _folder_fingerprint(root: Path, pattern: str) -> str:
        items = []
        if root.exists():
            for p in root.rglob(pattern):
                if not p.is_file():
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                rel = str(p.relative_to(root))
                items.append((rel, int(st.st_size), int(st.st_mtime)))
        items.sort()
        h = hashlib.sha1()
        for rel, size, mtime in items:
            h.update(rel.encode("utf-8", errors="ignore"))
            h.update(b"\0")
            h.update(str(size).encode("ascii"))
            h.update(b"\0")
            h.update(str(mtime).encode("ascii"))
            h.update(b"\0")
        return h.hexdigest()

    def _compute_regen_signature() -> str | None:
        pdf_path = _latest_file(PDF_ROOT, "*.pdf")
        if pdf_path is None:
            pdf_path = _latest_file(Path.cwd(), "*.pdf")
        if pdf_path is None:
            return None
        try:
            pdf_stat = pdf_path.stat()
            pdf_sig = f"{pdf_path.name}:{int(pdf_stat.st_size)}:{int(pdf_stat.st_mtime)}"
        except OSError:
            pdf_sig = pdf_path.name

        prijemi_sig = _folder_fingerprint(PRIJEMI_ROOT, "*.xlsx")
        try:
            db_stat = Path(state["db_path"]).stat()
            db_sig = f"{int(db_stat.st_size)}:{int(db_stat.st_mtime)}"
        except OSError:
            db_sig = "na"
        return hashlib.sha1(f"{pdf_sig}|{prijemi_sig}|{db_sig}".encode("utf-8")).hexdigest()

    def confirm_regen_metrics():
        new_sig = _compute_regen_signature()
        if new_sig is not None:
            conn = get_conn()
            try:
                last_sig = get_app_state(conn, "regen_signature")
            finally:
                conn.close()
            if last_sig and last_sig == new_sig:
                force = messagebox.askyesno(
                    "Regenerisi metrike",
                    "Nema promjena u ulaznim fajlovima (PDF/SP Prijemi/DB).\n"
                    "Želiš li ipak pokrenuti regeneraciju?",
                )
                if not force:
                    refresh_external_views()
                    return

        ok = messagebox.askyesno(
            "Regenerisi metrike",
                    "Ovo ce:\n"
                    "- procitati najnoviji PDF iz 'Kartice artikala'\n"
                    "- procitati sve xlsx iz 'SP Prijemi'\n"
                    "- ponovo izracunati 'sku_daily_metrics.csv'\n\n"
                    "Nastaviti?",
                )
        if not ok:
            return
        regen_progress_var.set("Regenerisanje metrika: 0%")
        regen_progress.set(0)
        regen_progress.configure(mode="determinate")

        def on_regen_success():
            if new_sig is not None:
                conn = get_conn()
                try:
                    set_app_state(conn, "regen_signature", new_sig)
                finally:
                    conn.close()
            refresh_external_views()

        run_action_async_process(
            run_regenerate_sku_metrics_process,
            [str(PDF_ROOT), str(PRIJEMI_ROOT), str(METRICS_OUT), str(SP_DB), str(state["db_path"]), REGEN_TASK],
            "Regenerisi metrike",
            progress_task=REGEN_TASK,
            on_success=on_regen_success,
        )

    task_status_var = ctk.StringVar(value="Task: idle")
    last_error_var = ctk.StringVar(value="Zadnja greska: -")
    ctk.CTkLabel(top, textvariable=task_status_var).pack(side="left", padx=12)
    ctk.CTkLabel(top, textvariable=last_error_var).pack(side="left", padx=12)

    def open_error_log():
        path = APP_DIR / "exports" / "app-errors.log"
        if not path.exists():
            messagebox.showinfo("Info", "Nema loga gresaka.")
            return
        try:
            os.startfile(path)  # type: ignore[attr-defined]
        except Exception:
            messagebox.showinfo("Info", f"Log gresaka: {path}")

    def open_run_log():
        path = APP_DIR / "exports" / "app-run.log"
        if not path.exists():
            messagebox.showinfo("Info", "Nema loga aktivnosti.")
            return
        try:
            os.startfile(path)  # type: ignore[attr-defined]
        except Exception:
            messagebox.showinfo("Info", f"Log aktivnosti: {path}")

    ctk.CTkButton(top, text="Otvori log gresaka", command=open_error_log).pack(
        side="left", padx=6
    )
    ctk.CTkButton(top, text="Otvori log aktivnosti", command=open_run_log).pack(
        side="left", padx=6
    )

    tabs = ctk.CTkTabview(app)
    tabs.pack(fill="both", expand=True, padx=12, pady=8)

    tab_dashboard = tabs.add("Dashboard")
    tab_finansije = tabs.add("Finansije")
    tab_prodaja = tabs.add("Prodaja")
    tabs.add("Marze")
    tab_troskovi = tabs.add("Troskovi")
    tab_povrati = tabs.add("Povrati")
    tab_nepreuzete = tabs.add("Nepreuzete")
    tab_poslovanje = tabs.add("Poslovanje")
    tab_settings = tabs.add("Podesavanja aplikacije")

    def _render_textbox(widget: ctk.CTkTextbox, lines: list[str]):
        widget.configure(state="normal")
        widget.delete("0.0", "end")
        widget.insert("0.0", "\n\n".join(lines))
        widget.configure(state="disabled")

    def _sku_name_map() -> dict[str, str]:
        df = _load_sku_summary_dataframe()
        if df.empty or "sku" not in df.columns or "Artikal" not in df.columns:
            return {}
        return {
            str(r["sku"]): str(r.get("Artikal") or "")
            for _, r in df[["sku", "Artikal"]].dropna().iterrows()
        }

    def _aggregate_by_sku(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return (
            df.groupby("sku", dropna=False)
            .agg(
                lost_qty=("lost_sales_qty", "sum"),
                lost_value=("lost_sales_value_est", "sum"),
                oos_days=("oos_flag", "sum"),
                net_qty=("net_sales_qty", "sum"),
                demand_qty=("demand_baseline_qty", "sum"),
            )
            .reset_index()
        )

    # UI za tab Prodaja je izvučen u srb_modules/ui_prodaja.py (widget reference se čuva u ctx.state["prodaja_widgets"]).

    # (prodaja refresh/export logika je definirana niže, nakon što se UI builda u modulu)



    prodaja_logic = init_prodaja_logic(
        ctx,
        ProdajaLogicDeps(
            messagebox=messagebox,
            format_amount=format_amount,
            load_sku_daily_dataframe=_load_sku_daily_dataframe,
            load_promo_dataframe=_load_promo_dataframe,
            sku_name_map=_sku_name_map,
            resolve_prodaja_period=_resolve_prodaja_period,
            filter_daily_by_period=_filter_daily_by_period,
            aggregate_by_sku=_aggregate_by_sku,
            render_textbox=_render_textbox,
            parse_user_date=_parse_user_date,
            format_user_date=_format_user_date,
        ),
    )

    ctx.state["prodaja_widgets"] = build_prodaja_tab(
        ctx,
        ctk=ctk,
        tab_prodaja=tab_prodaja,
        Figure=Figure,
        FigureCanvasTkAgg=FigureCanvasTkAgg,
        messagebox=messagebox,
        add_calendar_picker=_add_calendar_picker,
        pick_date_range_dialog=_pick_date_range_dialog,
        parse_user_date=_parse_user_date,
        format_user_date=_format_user_date,
        refresh_prodaja_pregled=prodaja_logic["refresh_prodaja_pregled"],
        refresh_trending=prodaja_logic["refresh_trending"],
        refresh_trending_chart=prodaja_logic["refresh_trending_chart"],
        refresh_snizenja=prodaja_logic["refresh_snizenja"],
        export_oos_gubitci_excel=prodaja_logic["export_oos_gubitci_excel"],
    )

    refresh_prodaja_views = prodaja_logic["refresh_prodaja_views"]
    refresh_prodaja_views()

    settings_body = ctk.CTkFrame(tab_settings)
    settings_body.pack(fill="both", expand=True, padx=12, pady=12)
    ctk.CTkLabel(settings_body, text="Baza podataka").pack(
        anchor="w", padx=6, pady=(6, 4)
    )
    db_row = ctk.CTkFrame(settings_body)
    db_row.pack(fill="x", padx=6, pady=(0, 10))
    ctk.CTkLabel(db_row, text="DB:").pack(side="left", padx=(6, 4))
    ent_db = ctk.CTkEntry(db_row, width=600)
    ent_db.insert(0, str(db_path))
    ent_db.configure(state="readonly")
    ent_db.pack(side="left", padx=4)
    ctk.CTkButton(db_row, text="Promijeni", command=set_db_path).pack(
        side="left", padx=6
    )

    ctk.CTkLabel(settings_body, text="Uvoz (pocetno stanje)").pack(
        anchor="w", padx=6, pady=(6, 4)
    )
    ctk.CTkLabel(
        settings_body,
        text="Workflow: 1) SP Narudzbe 2) Minimax 3) SP Uplate 4) Banka XML 5) SP Preuzimanja",
    ).pack(anchor="w", padx=6, pady=(0, 6))
    base_imports = ctk.CTkFrame(settings_body)
    base_imports.pack(anchor="w", fill="x", padx=6, pady=(0, 10))

    def run_import_with_financial_prompt(import_fn, title: str, pattern: str):
        imported = run_import_folder(import_fn, title, pattern)
        maybe_prompt_financial_refresh(imported_any=imported > 0)

    ctk.CTkButton(
        base_imports, text="Zadnji uvozi", command=show_last_imports
    ).pack(anchor="w", pady=(2, 8))

    btn_import_sp_orders = ctk.CTkButton(
        base_imports,
        text="SP Narudzbe",
        command=lambda: run_import_with_financial_prompt(
            import_sp_orders, "SP Narudzbe (folder)", "*.xlsx"
        ),
    )
    btn_import_sp_orders.pack(anchor="w", pady=2)
    btn_import_minimax = ctk.CTkButton(
        base_imports,
        text="Minimax",
        command=lambda: run_import_with_financial_prompt(
            import_minimax, "Minimax (folder)", "*.xlsx"
        ),
    )
    btn_import_minimax.pack(anchor="w", pady=2)
    btn_import_sp_payments = ctk.CTkButton(
        base_imports,
        text="SP Uplate",
        command=lambda: run_import_with_financial_prompt(
            import_sp_payments, "SP Uplate (folder)", "*.xlsx"
        ),
    )
    btn_import_sp_payments.pack(anchor="w", pady=2)
    btn_import_bank = ctk.CTkButton(
        base_imports,
        text="Banka XML",
        command=lambda: run_import_with_financial_prompt(
            import_bank_xml, "Banka XML (folder)", "*.xml"
        ),
    )
    btn_import_bank.pack(anchor="w", pady=2)
    btn_import_sp_returns = ctk.CTkButton(
        base_imports,
        text="SP Preuzimanja",
        command=lambda: run_import_with_financial_prompt(
            import_sp_returns, "SP Preuzimanja (folder)", "*.xlsx"
        ),
    )
    btn_import_sp_returns.pack(anchor="w", pady=2)

    ctk.CTkLabel(settings_body, text="Metrike i kartice").pack(
        anchor="w", padx=6, pady=(6, 4)
    )
    metrics_row = ctk.CTkFrame(settings_body)
    metrics_row.pack(fill="x", padx=6, pady=(0, 10))

    kartice_status_var = ctk.StringVar(value="Kartice artikala: -")

    def refresh_kartice_status():
        conn = get_conn()
        try:
            start = get_app_state(conn, "kartice_range_start")
            end = get_app_state(conn, "kartice_range_end")
            pdf_name = get_app_state(conn, "kartice_pdf_name")
        finally:
            conn.close()

        def _iso_to_date(text: str | None) -> date | None:
            if not text:
                return None
            try:
                return datetime.strptime(str(text)[:10], "%Y-%m-%d").date()
            except Exception:
                return None

        start_dt = _iso_to_date(start)
        end_dt = _iso_to_date(end)
        if start_dt and end_dt:
            kartice_status_var.set(
                f"Kartice artikala: {_format_user_date(start_dt)}–{_format_user_date(end_dt)} ({pdf_name or '-'})"
            )
        elif end_dt:
            kartice_status_var.set(
                f"Kartice artikala: do {_format_user_date(end_dt)} ({pdf_name or '-'})"
            )
        else:
            kartice_status_var.set(f"Kartice artikala: - ({pdf_name or '-'})")

    btn_refresh_external = ctk.CTkButton(
        metrics_row, text="Osvjezi novim podacima", command=refresh_external_views
    )
    btn_refresh_external.pack(side="left", padx=(0, 6))

    btn_regen = ctk.CTkButton(metrics_row, text="Regenerisi metrike", command=confirm_regen_metrics)
    btn_regen.pack(side="left", padx=6)

    regen_progress = ctk.CTkProgressBar(settings_body, mode="determinate")
    regen_progress.set(0)
    regen_progress.pack(in_=settings_body, fill="x", padx=6, pady=(0, 0))
    ctk.CTkLabel(settings_body, textvariable=regen_progress_var).pack(anchor="w", padx=6, pady=(2, 10))
    ctk.CTkLabel(settings_body, textvariable=kartice_status_var).pack(anchor="w", padx=6, pady=(0, 10))
    refresh_kartice_status()

    def _resolve_existing_folder(*names: str) -> Path | None:
        for name in names:
            p = Path(name)
            if p.exists():
                return p
        return None

    def run_import_all_defaults():
        if state.get("baseline_locked"):
            messagebox.showerror("Greska", "Baza je zakljucana. Uvoz nije dozvoljen.")
            return
        if not messagebox.askyesno(
            "Uvezi sve podatke",
            "Ovo ce pokusati uvesti sve izvore iz default foldera projekta, jedan po jedan,\n"
            "zatim (opcionalno) osvjeziti finansijske podatke (Match),\n"
            "i na kraju pokrenuti 'Regenerisi metrike'.\n\n"
            "Nastaviti?",
        ):
            return
        totals = []
        totals.append(
            (
                "SP Narudzbe",
            )
            + run_import_default_folder(
                _resolve_existing_folder("SP Narudzbe", "SP-Narudzbe") or Path("SP Narudzbe"),
                import_sp_orders,
                "SP Narudzbe (auto)",
                "*.xlsx",
                silent_missing=True,
            )
        )
        totals.append(
            ("Minimax",)
            + run_import_default_folder(
                _resolve_existing_folder("Minimax") or Path("Minimax"),
                import_minimax,
                "Minimax (auto)",
                "*.xlsx",
                silent_missing=True,
            )
        )
        totals.append(
            ("SP Uplate",)
            + run_import_default_folder(
                _resolve_existing_folder("SP Uplate", "SP-Uplate") or Path("SP Uplate"),
                import_sp_payments,
                "SP Uplate (auto)",
                "*.xlsx",
                silent_missing=True,
            )
        )
        totals.append(
            ("Banka XML",)
            + run_import_default_folder(
                _resolve_existing_folder("Banka XML", "Izvodi") or Path("Banka XML"),
                import_bank_xml,
                "Banka XML (auto)",
                "*.xml",
                silent_missing=True,
            )
        )
        totals.append(
            ("SP Preuzimanja",)
            + run_import_default_folder(
                _resolve_existing_folder("SP Preuzimanja", "SP-Preuzimanja")
                or Path("SP Preuzimanja"),
                import_sp_returns,
                "SP Preuzimanja (auto)",
                "*.xlsx",
                silent_missing=True,
            )
        )
        msg_lines = []
        for name, imp, skip in totals:
            if imp == 0 and skip == 0:
                continue
            msg_lines.append(f"{name}: uvezeno {imp}, preskoceno {skip}")
        if msg_lines:
            messagebox.showinfo("Info", "Uvoz zavrsen:\n" + "\n".join(msg_lines))
        total_imported = sum(imp for _, imp, _ in totals)
        maybe_prompt_financial_refresh(
            after_done=confirm_regen_metrics, imported_any=total_imported > 0
        )

    btn_import_all = ctk.CTkButton(
        base_imports, text="Uvezi sve podatke (auto)", command=run_import_all_defaults
    )
    btn_import_all.pack(anchor="w", pady=(8, 2))

    ctx.action_buttons = (ctx.action_buttons or []) + [
        btn_import_sp_orders,
        btn_import_minimax,
        btn_import_sp_payments,
        btn_import_bank,
        btn_import_sp_returns,
        btn_import_all,
        btn_refresh_external,
        btn_regen,
    ]

    ctk.CTkLabel(settings_body, text="Kurs i valuta").pack(
        anchor="w", padx=6, pady=(6, 4)
    )
    rate_row = ctk.CTkFrame(settings_body)
    rate_row.pack(fill="x", padx=6, pady=(0, 10))
    ctk.CTkLabel(rate_row, text="1 BAM =").pack(side="left", padx=(6, 4))
    rate_var = ctk.StringVar(value="")
    ent_rate = ctk.CTkEntry(rate_row, width=120, textvariable=rate_var)
    ent_rate.pack(side="left", padx=4)
    ctk.CTkLabel(rate_row, text="RSD").pack(side="left", padx=(4, 8))

    def sync_rate_entry():
        rate = state.get("rate_rsd_to_bam")
        if rate is None:
            rate_var.set("")
            return
        try:
            inv = 1.0 / float(rate)
        except Exception:
            rate_var.set("")
            return
        rate_var.set(f"{inv:.2f}")

    def apply_rate_manual():
        text = rate_var.get().strip().replace(",", ".")
        if not text:
            messagebox.showerror("Greska", "Unesi kurs RSD->BAM.")
            return
        try:
            bam_to_rsd = float(text)
        except ValueError:
            messagebox.showerror("Greska", "Neispravan kurs.")
            return
        if bam_to_rsd <= 0:
            messagebox.showerror("Greska", "Kurs mora biti veći od 0.")
            return
        rate = 1.0 / bam_to_rsd
        conn = get_conn()
        try:
            set_app_state(conn, "rate_rsd_to_bam", f"{rate}")
        finally:
            conn.close()
        state["rate_rsd_to_bam"] = rate
        sync_rate_entry()
        refresh_dashboard()

    def refresh_rate_auto():
        rate, error = refresh_exchange_rate()
        if rate is None:
            messagebox.showwarning(
                "Info",
                "Neuspjelo povlacenje kursa sa CBBH. "
                f"{error or 'Provjeri internet ili unesi kurs rucno.'}",
            )
        sync_rate_entry()
        refresh_dashboard()

    ctk.CTkButton(rate_row, text="Refresh kurs", command=refresh_rate_auto).pack(
        side="left", padx=4
    )
    ctk.CTkButton(rate_row, text="Snimi kurs", command=apply_rate_manual).pack(
        side="left", padx=4
    )

    currency_row = ctk.CTkFrame(settings_body)
    currency_row.pack(fill="x", padx=6, pady=(0, 10))
    ctk.CTkLabel(currency_row, text="Prikaz valute:").pack(side="left", padx=(6, 4))
    load_currency_mode()
    currency_var = ctk.StringVar(value=state.get("currency_mode", "RSD"))

    def on_currency_change(choice: str):
        state["currency_mode"] = choice
        conn = get_conn()
        try:
            set_app_state(conn, "currency_mode", choice)
        finally:
            conn.close()
        refresh_dashboard()

    currency_menu = ctk.CTkOptionMenu(
        currency_row,
        values=["RSD", "BAM"],
        variable=currency_var,
        command=on_currency_change,
    )
    currency_menu.pack(side="left", padx=4)

    ctk.CTkLabel(settings_body, text="Sigurnost").pack(anchor="w", padx=6, pady=(6, 4))
    security_row = ctk.CTkFrame(settings_body)
    security_row.pack(fill="x", padx=6, pady=(0, 10))
    baseline_status_var = ctk.StringVar(value="")
    ctk.CTkLabel(security_row, text="Status baze:").pack(side="left", padx=(6, 4))
    lbl_baseline = ctk.CTkLabel(security_row, textvariable=baseline_status_var)
    lbl_baseline.pack(side="left", padx=4)
    btn_lock_baseline = ctk.CTkButton(
        security_row, text="Zakljucaj pocetno stanje", command=set_baseline_lock
    )
    btn_lock_baseline.pack(side="left", padx=6)

    reset_row = ctk.CTkFrame(settings_body)
    reset_row.pack(fill="x", padx=6, pady=(0, 10))
    ctk.CTkLabel(reset_row, text="Lozinka za reset:").pack(side="left", padx=(6, 4))
    reset_pass_var = ctk.StringVar(value="")
    ent_reset = ctk.CTkEntry(
        reset_row, width=160, textvariable=reset_pass_var, show="*"
    )
    ent_reset.pack(side="left", padx=4)
    ctk.CTkButton(reset_row, text="Snimi lozinku", command=set_reset_password).pack(
        side="left", padx=4
    )
    reset_menu = ctk.CTkOptionMenu(
        reset_row,
        values=[spec.label for spec in RESET_SPECS],
        variable=reset_source_var,
    )
    reset_menu.pack(side="left", padx=6)
    btn_reset_source = ctk.CTkButton(
        reset_row, text="Reset (izvor)", command=run_reset_source
    )
    btn_reset_source.pack(side="left", padx=6)

    ctk.CTkLabel(settings_body, text="Audit").pack(anchor="w", padx=6, pady=(6, 4))
    audit_row = ctk.CTkFrame(settings_body)
    audit_row.pack(fill="x", padx=6, pady=(0, 10))
    ctk.CTkButton(audit_row, text="Export audit.xlsx", command=run_export_audit).pack(
        side="left", padx=6
    )

    def update_baseline_ui():
        locked = state.get("baseline_locked", False)
        locked_at = state.get("baseline_locked_at")
        if locked:
            text = "Zakljucano"
            if locked_at:
                text += f" ({locked_at})"
            baseline_status_var.set(text)
            btn_lock_baseline.configure(state="disabled")
        else:
            baseline_status_var.set("Otkljucano")
            btn_lock_baseline.configure(state="normal")
        if btn_reset_matches is not None:
            btn_reset_matches.configure(state="disabled" if locked else "normal")
        if btn_reset_source is not None:
            btn_reset_source.configure(state="disabled" if locked else "normal")

    kpi_frame = ctk.CTkFrame(tab_dashboard)
    kpi_frame.pack(fill="x", padx=10, pady=10)

    def make_kpi(parent, title):
        frame = ctk.CTkFrame(parent, width=200)
        frame.pack(side="left", padx=8, pady=6, fill="x", expand=True)
        ctk.CTkLabel(frame, text=title).pack(pady=(6, 2))
        lbl = ctk.CTkLabel(frame, text="0", font=ctk.CTkFont(size=18, weight="bold"))
        lbl.pack(pady=(0, 6))
        return lbl

    lbl_total_orders = make_kpi(kpi_frame, "Narudzbe")
    lbl_returns = make_kpi(kpi_frame, "Nepreuzete")
    lbl_unmatched = make_kpi(kpi_frame, "Neuparene")

    bank_period_frame = ctk.CTkFrame(tab_dashboard)
    bank_period_frame.pack(fill="x", padx=10, pady=(0, 6))
    ctk.CTkLabel(bank_period_frame, text="Prikazi period:").pack(
        side="left", padx=(6, 4)
    )
    bank_period_var = ctk.StringVar(value="12 mjeseci")
    state["period_days"] = 360
    state["period_start"] = None
    state["period_end"] = None
    state["unpicked_period_days"] = None
    state["unpicked_period_start"] = None
    state["unpicked_period_end"] = None
    state["dashboard_refresh_after_id"] = None

    def _schedule_dashboard_refresh():
        pending = state.get("dashboard_refresh_after_id")
        if pending:
            try:
                app.after_cancel(pending)
            except Exception:
                pass
        if ctx.status_var is not None:
            try:
                ctx.status_var.set("Osvjezavam dashboard...")
            except Exception:
                pass

        def _run():
            state["dashboard_refresh_after_id"] = None
            refresh_dashboard(full_refresh=False)

        state["dashboard_refresh_after_id"] = app.after(250, _run)

    def on_bank_period_change(choice: str):
        mapping = {
            "Svo vrijeme": None,
            "3 mjeseca": 90,
            "6 mjeseci": 180,
            "12 mjeseci": 360,
            "24 mjeseca": 720,
        }
        state["period_days"] = mapping.get(choice)
        state["period_start"] = None
        state["period_end"] = None
        _schedule_dashboard_refresh()

    bank_period_menu = ctk.CTkOptionMenu(
        bank_period_frame,
        values=["Svo vrijeme", "3 mjeseca", "6 mjeseci", "12 mjeseci", "24 mjeseca"],
        variable=bank_period_var,
        command=on_bank_period_change,
    )
    bank_period_menu.pack(side="left", padx=4)

    load_baseline_lock()
    update_baseline_ui()
    app.after(1000, poll_global_status)

    refresh_exchange_rate()
    sync_rate_entry()

    charts_frame = ctk.CTkFrame(tab_dashboard)
    charts_frame.pack(fill="both", expand=True, padx=10, pady=10)

    fig_customers = Figure(figsize=(4, 3), dpi=100)
    ax_customers = fig_customers.add_subplot(111)
    canvas_customers = FigureCanvasTkAgg(fig_customers, master=charts_frame)
    canvas_customers.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_products = Figure(figsize=(4, 3), dpi=100)
    ax_products = fig_products.add_subplot(111)
    canvas_products = FigureCanvasTkAgg(fig_products, master=charts_frame)
    canvas_products.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_sp_bank = Figure(figsize=(4, 3), dpi=100)
    ax_sp_bank = fig_sp_bank.add_subplot(111)
    canvas_sp_bank = FigureCanvasTkAgg(fig_sp_bank, master=charts_frame)
    canvas_sp_bank.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    ctx.state["finansije_widgets"] = build_finansije_tab(
        ctx,
        ctk=ctk,
        tab_finansije=tab_finansije,
        Figure=Figure,
        FigureCanvasTkAgg=FigureCanvasTkAgg,
        messagebox=messagebox,
        unlock_finansije=unlock_finansije_password,
        pick_date_range_dialog=_pick_date_range_dialog,
        format_user_date=_format_user_date,
        refresh_finansije=refresh_finansije,
        export_unpaid_sp_orders=export_unpaid_sp_orders,
        export_pending_sp_orders=export_pending_sp_orders,
        export_neto_breakdown=export_finansije_neto_breakdown,
    )

    ctx.state["troskovi_widgets"] = build_troskovi_tab(
        ctx,
        ctk=ctk,
        tab_troskovi=tab_troskovi,
        Figure=Figure,
        FigureCanvasTkAgg=FigureCanvasTkAgg,
        refresh_expenses=refresh_expenses,
    )

    ctx.state["povrati_widgets"] = build_povrati_tab(
        ctx,
        ctk=ctk,
        tab_povrati=tab_povrati,
        Figure=Figure,
        FigureCanvasTkAgg=FigureCanvasTkAgg,
        run_export_refunds_full=run_export_refunds_full,
    )

    ctx.state["nepreuzete_widgets"] = build_nepreuzete_tab(
        ctx,
        ctk=ctk,
        tab_nepreuzete=tab_nepreuzete,
        Figure=Figure,
        FigureCanvasTkAgg=FigureCanvasTkAgg,
        refresh_unpicked_charts=refresh_unpicked_charts,
        run_export_unpicked_full=run_export_unpicked_full,
        run_dexpress_tracking=run_dexpress_tracking,
    )

    ctx.state["poslovanje_widgets"] = build_poslovanje_tab(
        ctx,
        ctk=ctk,
        tab_poslovanje=tab_poslovanje,
        show_last_imports=show_last_imports,
        run_import_folder=run_import_folder,
        import_sp_orders=import_sp_orders,
        import_minimax=import_minimax,
        import_sp_payments=import_sp_payments,
        import_bank_xml=import_bank_xml,
        import_sp_returns=import_sp_returns,
        run_action_async_process=run_action_async_process,
        run_match_minimax_process=run_match_minimax_process,
        run_match_bank_process=run_match_bank_process,
        close_invoices_from_confirmed_matches=close_invoices_from_confirmed_matches,
        run_action=run_action,
        run_reset_minimax_matches=run_reset_minimax_matches,
        run_export_basic=run_export_basic,
        run_export_single=run_export_single,
        report_unmatched_reasons=report_unmatched_reasons,
        run_export_bank_refunds=run_export_bank_refunds,
        open_exports=open_exports,
        refresh_poslovanje_lists=refresh_poslovanje_lists,
        start_test_session=start_test_session,
        _record_decision=_record_decision,
        stop_test_session=stop_test_session,
        update_baseline_ui=update_baseline_ui,
        executor_factory=lambda: concurrent.futures.ProcessPoolExecutor(max_workers=1),
    )

    refresh_dashboard()
    refresh_poslovanje_lists()
    app.mainloop()


def run_smoke_tests() -> int:
    failures = 0

    def check(label: str, cond: bool, detail: str = ""):
        nonlocal failures
        if cond:
            print(f"OK: {label}")
        else:
            failures += 1
            print(f"FAIL: {label} {detail}")

    check("normalize_phone +381", normalize_phone("+381641234567") == "0641234567")
    check("normalize_phone 064", normalize_phone("064 123 4567") == "0641234567")
    check("normalize_phone 6xx", normalize_phone("61234567").startswith("0"))

    key = compute_customer_key("0641234567", None, "Test User", "Beograd")
    check("customer_key phone", key.startswith("phone:"))

    history = [
        {
            "statusTime": "21.11.2025 18:33:17",
            "statusValue": "Pošiljka je preuzeta od pošiljaoca",
        },
        {
            "statusTime": "24.11.2025 07:51:56",
            "statusValue": "Pošiljka zadužena za isporuku",
        },
        {
            "statusTime": "25.11.2025 13:31:31",
            "statusValue": "Ponovni pokušaj isporuke, nema nikoga na adresi",
        },
        {
            "statusTime": "08.12.2025 11:40:58",
            "statusValue": "Pošiljka je vraćena pošiljaocu",
        },
    ]
    events, summary = analyze_tracking_history(history)
    check("tracking received_at", summary.get("received_at") is not None)
    check(
        "tracking first_out_for_delivery_at",
        summary.get("first_out_for_delivery_at") is not None,
    )
    check("tracking has_returned", summary.get("has_returned") == 1)
    check("tracking attempts >=1", (summary.get("delivery_attempts") or 0) >= 1)

    prijemi_root = Path("Sp Prijemi")
    prijemi_files = [p for p in prijemi_root.glob("*.xlsx") if p.is_file()]
    if prijemi_files:
        mem = None
        try:
            mem = sqlite3.connect(":memory:")
            mem.execute("PRAGMA foreign_keys = ON;")
            init_db(mem)
            rejects: list[dict] = []
            import_sp_prijemi(mem, prijemi_files[0], rejects=rejects)
            receipts = int(mem.execute("SELECT COUNT(*) FROM sp_prijemi_receipts").fetchone()[0])
            lines = int(mem.execute("SELECT COUNT(*) FROM sp_prijemi_lines").fetchone()[0])
            check("sp_prijemi import receipt", receipts == 1, f"receipts={receipts}")
            check("sp_prijemi import lines", lines > 0, f"lines={lines}")
            import_sp_prijemi(mem, prijemi_files[0], rejects=rejects)
            runs = int(mem.execute("SELECT COUNT(*) FROM import_runs WHERE source='SP-Prijemi'").fetchone()[0])
            check("sp_prijemi file dedupe", runs == 1, f"runs={runs}")
        finally:
            try:
                if mem is not None:
                    mem.close()
            except Exception:
                pass
    else:
        check("sp_prijemi import (skipped)", True, "no xlsx in Sp Prijemi/")

    events_csv = Path("Kalkulacije_kartice_art") / "izlaz" / "kartice_events.csv"
    if events_csv.exists():
        mem = None
        try:
            mem = sqlite3.connect(":memory:")
            mem.execute("PRAGMA foreign_keys = ON;")
            init_db(mem)
            rejects: list[dict] = []
            import_kartice_events(mem, events_csv, rejects=rejects)
            rows = int(mem.execute("SELECT COUNT(*) FROM kartice_events").fetchone()[0])
            check("kartice_events import rows", rows > 0, f"rows={rows}")
            import_kartice_events(mem, events_csv, rejects=rejects)
            runs = int(
                mem.execute(
                    "SELECT COUNT(*) FROM import_runs WHERE source='Kartice-Events-CSV'"
                ).fetchone()[0]
            )
            check("kartice_events file dedupe", runs == 1, f"runs={runs}")
        finally:
            try:
                if mem is not None:
                    mem.close()
            except Exception:
                pass
    else:
        check("kartice_events import (skipped)", True, "no kartice_events.csv")

    print(f"Tests finished. Failures: {failures}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="SRB1.0 import tool")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("init-db")

    sp_orders = sub.add_parser("import-sp-orders")
    sp_orders.add_argument("path", type=Path)

    sp_payments = sub.add_parser("import-sp-payments")
    sp_payments.add_argument("path", type=Path)

    sp_returns = sub.add_parser("import-sp-returns")
    sp_returns.add_argument("path", type=Path)

    sp_prijemi = sub.add_parser("import-sp-prijemi")
    sp_prijemi.add_argument("path", type=Path, help="Fajl ili folder sa SP Prijemi (*.xlsx)")

    kartice = sub.add_parser("import-kartice-events")
    kartice.add_argument(
        "path",
        type=Path,
        help="kartice_events.csv (izlaz iz extract_kalkulacije_kartice.py)",
    )

    minimax = sub.add_parser("import-minimax")
    minimax.add_argument("path", type=Path)

    minimax_items = sub.add_parser("import-minimax-items")
    minimax_items.add_argument("path", type=Path)

    bank = sub.add_parser("import-bank-xml")
    bank.add_argument("path", type=Path)

    bank_match = sub.add_parser("match-bank")
    bank_match.add_argument("--day-tolerance", type=int, default=2)

    sub.add_parser("extract-bank-refunds")

    reset_src = sub.add_parser("reset-source")
    reset_src.add_argument(
        "source",
        choices=[spec.key for spec in RESET_SPECS],
        help="Koji izvor obrisati iz baze (bez brisanja DB fajla)",
    )

    match = sub.add_parser("match-minimax")
    match.add_argument("--auto-threshold", type=int, default=70)
    match.add_argument("--review-threshold", type=int, default=50)

    review = sub.add_parser("list-review")
    confirm = sub.add_parser("confirm-match")
    confirm.add_argument("match_id", type=int)

    close_invoices = sub.add_parser("close-invoices")

    report = sub.add_parser("report")
    report.add_argument(
        "name",
        choices=[
            "unmatched",
            "unmatched-reasons",
            "conflicts",
            "nearest",
            "open",
            "returns",
            "needs-invoice",
            "needs-invoice-orders",
            "no-value",
            "candidates",
            "unmatched-candidates",
            "unmatched-candidates-grouped",
            "storno",
            "bank-sp",
            "bank-refunds",
            "bank-refunds-extracted",
            "bank-unmatched-sp",
            "bank-unmatched-refunds",
            "sp-vs-bank",
            "alarms",
            "refunds-no-storno",
            "order-amount-issues",
            "duplicate-customers",
            "top-customers",
            "category-sales",
            "category-returns",
            "minimax-items",
        ],
        help="unmatched | unmatched-reasons | conflicts | nearest | open | returns | needs-invoice | needs-invoice-orders | no-value | candidates | unmatched-candidates | unmatched-candidates-grouped | storno | bank-sp | bank-refunds | bank-refunds-extracted | bank-unmatched-sp | bank-unmatched-refunds | sp-vs-bank | alarms | refunds-no-storno | order-amount-issues | duplicate-customers | top-customers | category-sales | category-returns | minimax-items",
    )
    report.add_argument(
        "--out-dir",
        type=Path,
        default=Path("exports"),
        help="Folder for CSV/XLSX exports",
    )
    report.add_argument(
        "--format",
        choices=["csv", "xlsx"],
        default="csv",
        help="Export format",
    )
    report.add_argument(
        "--days",
        type=int,
        default=7,
        help="Days threshold for alarms report",
    )

    export_review = sub.add_parser("export-review")
    export_review.add_argument(
        "--out-dir",
        type=Path,
        default=Path("exports"),
        help="Folder for CSV/XLSX exports",
    )
    export_review.add_argument(
        "--format",
        choices=["csv", "xlsx"],
        default="csv",
        help="Export format",
    )

    export_all = sub.add_parser("export-all")
    export_all.add_argument(
        "--out-dir",
        type=Path,
        default=Path("exports"),
        help="Folder for CSV/XLSX exports",
    )
    export_all.add_argument(
        "--format",
        choices=["csv", "xlsx"],
        default="csv",
        help="Export format",
    )

    import_confirm = sub.add_parser("import-confirm")
    import_confirm.add_argument(
        "path", type=Path, help="CSV/XLSX sa match_id i confirm"
    )

    ui = sub.add_parser("ui")
    ui.add_argument("--db", type=Path, default=DB_PATH)

    sub.add_parser("tests")

    cat = sub.add_parser("category")
    cat_sub = cat.add_subparsers(dest="action", required=True)
    cat_prefix = cat_sub.add_parser("add-prefix")
    cat_prefix.add_argument("prefix")
    cat_prefix.add_argument("name")
    cat_override = cat_sub.add_parser("add-sku-override")
    cat_override.add_argument("sku")
    cat_override.add_argument("category")
    cat_custom = cat_sub.add_parser("add-custom-sku")
    cat_custom.add_argument("sku")

    args = parser.parse_args()
    conn = connect_db(args.db)
    init_db(conn)
    ensure_customer_keys(conn)

    if not args.cmd:
        run_ui(args.db)
        return
    if args.cmd == "init-db":
        return
    if args.cmd == "tests":
        conn.close()
        run_smoke_tests()
        return
    if args.cmd == "import-sp-orders":
        import_sp_orders(conn, args.path)
    elif args.cmd == "import-sp-payments":
        import_sp_payments(conn, args.path)
    elif args.cmd == "import-sp-returns":
        import_sp_returns(conn, args.path)
    elif args.cmd == "import-sp-prijemi":
        import_sp_prijemi(conn, args.path)
    elif args.cmd == "import-kartice-events":
        import_kartice_events(conn, args.path)
    elif args.cmd == "import-minimax":
        import_minimax(conn, args.path)
    elif args.cmd == "import-minimax-items":
        import_minimax_items(conn, args.path)
    elif args.cmd == "import-bank-xml":
        import_bank_xml(conn, args.path)
    elif args.cmd == "match-bank":
        match_bank_sp_payments(conn, args.day_tolerance)
        match_bank_refunds(conn)
    elif args.cmd == "extract-bank-refunds":
        extract_bank_refunds(conn)
    elif args.cmd == "reset-source":
        deleted = _reset_source(conn, args.source)
        print(f"Reset zavrsen ({args.source}). Obrisano import runova: {deleted}")
    elif args.cmd == "match-minimax":
        match_minimax(conn, args.auto_threshold, args.review_threshold)
    elif args.cmd == "list-review":
        list_review_matches(conn)
    elif args.cmd == "confirm-match":
        confirm_match(conn, args.match_id)
    elif args.cmd == "close-invoices":
        close_invoices_from_confirmed_matches(conn)
    elif args.cmd == "report":
        if args.name == "unmatched":
            cols, rows = report_unmatched_orders(conn, return_rows=True)
        elif args.name == "unmatched-reasons":
            cols, rows = report_unmatched_reasons(conn, return_rows=True)
        elif args.name == "conflicts":
            cols, rows = report_conflicts(conn, return_rows=True)
        elif args.name == "nearest":
            cols, rows = report_nearest_invoice(conn, return_rows=True)
        elif args.name == "open":
            cols, rows = report_open_invoices(conn, return_rows=True)
        elif args.name == "returns":
            cols, rows = report_returns(conn, return_rows=True)
        elif args.name == "needs-invoice":
            cols, rows = report_needs_invoice(conn, return_rows=True)
        elif args.name == "needs-invoice-orders":
            cols, rows = report_needs_invoice_orders(conn, return_rows=True)
        elif args.name == "no-value":
            cols, rows = report_no_value_orders(conn, return_rows=True)
        elif args.name == "candidates":
            cols, rows = report_candidates(conn, return_rows=True)
        elif args.name == "unmatched-candidates":
            cols, rows = report_unmatched_with_candidates(conn, return_rows=True)
        elif args.name == "unmatched-candidates-grouped":
            cols, rows = report_unmatched_with_candidates_grouped(
                conn, return_rows=True
            )
        elif args.name == "storno":
            cols, rows = report_storno(conn, return_rows=True)
        elif args.name == "bank-sp":
            cols, rows = report_bank_sp(conn, return_rows=True)
        elif args.name == "bank-refunds":
            cols, rows = report_bank_refunds(conn, return_rows=True)
        elif args.name == "bank-refunds-extracted":
            cols, rows = report_bank_refunds_extracted(conn, return_rows=True)
        elif args.name == "bank-unmatched-sp":
            cols, rows = report_bank_unmatched_sp(conn, return_rows=True)
        elif args.name == "bank-unmatched-refunds":
            cols, rows = report_bank_unmatched_refunds(conn, return_rows=True)
        elif args.name == "sp-vs-bank":
            cols, rows = report_sp_vs_bank(conn, return_rows=True)
        elif args.name == "alarms":
            cols, rows = report_alarms(conn, days=args.days, return_rows=True)
        elif args.name == "refunds-no-storno":
            cols, rows = report_refunds_without_storno(conn, return_rows=True)
        elif args.name == "order-amount-issues":
            cols, rows = report_order_amount_issues(conn, return_rows=True)
        elif args.name == "duplicate-customers":
            cols, rows = report_duplicate_customers(conn, return_rows=True)
        elif args.name == "top-customers":
            cols, rows = report_top_customers(conn, return_rows=True)
        elif args.name == "category-sales":
            cols, rows = report_category_sales(conn, return_rows=True)
        elif args.name == "category-returns":
            cols, rows = report_category_returns(conn, return_rows=True)
        elif args.name == "minimax-items":
            cols, rows = report_minimax_items(conn, return_rows=True)
        out_path = write_report(cols, rows, args.out_dir, args.name, args.format)
        print(f"Export snimljen u: {out_path}")
    elif args.cmd == "export-review":
        cols, rows = list_review_matches(conn, return_rows=True)
        out_path = write_report(cols, rows, args.out_dir, "review", args.format)
        print(f"Export snimljen u: {out_path}")
    elif args.cmd == "export-all":
        exports = []
        cols, rows = report_unmatched_orders(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "unmatched", args.format))
        cols, rows = report_open_invoices(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "open", args.format))
        cols, rows = report_returns(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "returns", args.format))
        cols, rows = report_needs_invoice(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "needs-invoice", args.format)
        )
        cols, rows = report_needs_invoice_orders(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "needs-invoice-orders", args.format)
        )
        cols, rows = report_no_value_orders(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "no-value", args.format))
        cols, rows = report_candidates(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "candidates", args.format)
        )
        cols, rows = report_unmatched_with_candidates(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "unmatched-candidates", args.format)
        )
        cols, rows = report_unmatched_with_candidates_grouped(conn, return_rows=True)
        exports.append(
            write_report(
                cols, rows, args.out_dir, "unmatched-candidates-grouped", args.format
            )
        )
        cols, rows = report_storno(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "storno", args.format))
        cols, rows = report_sp_vs_bank(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "sp-vs-bank", args.format)
        )
        cols, rows = report_refunds_without_storno(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "refunds-no-storno", args.format)
        )
        cols, rows = report_alarms(conn, days=7, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "alarms", args.format))
        cols, rows = report_order_amount_issues(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "order-amount-issues", args.format)
        )
        cols, rows = report_duplicate_customers(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "duplicate-customers", args.format)
        )
        cols, rows = report_top_customers(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "top-customers", args.format)
        )
        cols, rows = report_category_sales(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "category-sales", args.format)
        )
        cols, rows = report_category_returns(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "category-returns", args.format)
        )
        cols, rows = report_minimax_items(conn, return_rows=True)
        exports.append(
            write_report(cols, rows, args.out_dir, "minimax-items", args.format)
        )
        cols, rows = list_review_matches(conn, return_rows=True)
        exports.append(write_report(cols, rows, args.out_dir, "review", args.format))
        print("Exporti snimljeni:")
        for path in exports:
            print(path)
    elif args.cmd == "import-confirm":
        apply_review_decisions(conn, args.path)
    elif args.cmd == "ui":
        run_ui(args.db)
    elif args.cmd == "category":
        if args.action == "add-prefix":
            add_category_prefix(args.prefix, args.name)
        elif args.action == "add-sku-override":
            add_sku_category_override(args.sku, args.category)
        elif args.action == "add-custom-sku":
            add_custom_sku(args.sku)


if __name__ == "__main__":
    main()
