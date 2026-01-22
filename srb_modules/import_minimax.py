from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd

from .import_common import append_reject, format_missing_int_ranges, start_import


def _parse_invoice_number(number: str | None) -> tuple[str | None, int | None]:
    if not number:
        return None, None
    text = str(number).strip()
    m = re.search(r"(20\d{2}).*?(\d+)\s*$", text)
    if not m:
        return None, None
    year = m.group(1)
    digits = m.group(2)
    try:
        return year, int(digits)
    except Exception:
        return year, None


def import_minimax(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    col: dict[str, str],
    sheet_minimax: str,
    file_hash: Callable[[Path], str],
    apply_storno: Callable[[sqlite3.Connection], None],
) -> None:
    df = pd.read_excel(path, sheet_name=sheet_minimax)
    import_id = start_import(conn, "Minimax", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "Minimax", path.name, None, "file_already_imported", "")
        return

    file_numbers_by_year: dict[str, set[int]] = {}
    max_by_year: dict[str, int] = {}
    for _, row in df.iterrows():
        year, num = _parse_invoice_number(str(row.get(col["mm_number"], "")).strip() or None)
        if not year or num is None:
            continue
        file_numbers_by_year.setdefault(year, set()).add(num)
        if year not in max_by_year or num > max_by_year[year]:
            max_by_year[year] = num

    for year, max_num in max_by_year.items():
        db_max_number = conn.execute(
            "SELECT MAX(number) FROM invoices WHERE number LIKE ?",
            (f"%{year}%",),
        ).fetchone()[0]
        _, db_max_num = _parse_invoice_number(str(db_max_number) if db_max_number else None)
        if db_max_num is None or max_num <= db_max_num:
            continue
        expected_start = db_max_num + 1
        gaps = format_missing_int_ranges(
            expected_start, file_numbers_by_year.get(year, set()), max_num
        )
        if gaps:
            append_reject(
                rejects,
                "Minimax",
                path.name,
                None,
                "gap_warning",
                f"Godina {year}: ocekivano {expected_start}..{max_num}, nedostaje: {gaps}",
            )

    for idx, row in df.iterrows():
        values = (
            str(row.get(col["mm_number"], "")).strip() or None,
            str(row.get(col["mm_customer"], "")).strip() or None,
            str(row.get(col["mm_country"], "")).strip() or None,
            str(row.get(col["mm_date"], "")).strip() or None,
            str(row.get(col["mm_due_date"], "")).strip() or None,
            str(row.get(col["mm_revenue"], "")).strip() or None,
            row.get(col["mm_amount_local"], None),
            row.get(col["mm_amount_due"], None),
            str(row.get(col["mm_analytics"], "")).strip() or None,
            str(row.get(col["mm_turnover"], "")).strip() or None,
            str(row.get(col["mm_account"], "")).strip() or None,
            str(row.get(col["mm_basis"], "")).strip() or None,
            str(row.get(col["mm_note"], "")).strip() or None,
            row.get(col["mm_payment_amount"], None),
            row.get(col["mm_open_amount"], None),
            import_id,
        )
        cur = conn.execute(
            "INSERT OR IGNORE INTO invoices ("
            "number, customer_name, country, date, due_date, revenue, "
            "amount_local, amount_due, analytics, turnover, account, basis, "
            "note, payment_amount, open_amount, import_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        if cur.rowcount == 0:
            append_reject(
                rejects,
                "Minimax",
                path.name,
                int(idx) + 1,
                "invoice_duplicate",
                f"number={values[0]}",
            )

    conn.commit()
    apply_storno(conn)


def import_minimax_items(
    conn: sqlite3.Connection,
    path: Path,
    *,
    sheet_minimax: str,
    file_hash: Callable[[Path], str],
) -> None:
    df = pd.read_excel(path, sheet_name=sheet_minimax)
    import_id = start_import(conn, "Minimax-Items", path, len(df), file_hash=file_hash)
    if import_id is None:
        return

    for _, row in df.iterrows():
        sku = str(row.get("Šifra", "")).strip().upper()
        if not sku:
            continue
        values = (
            sku,
            str(row.get("Naziv artikla", "")).strip() or None,
            str(row.get("Jedinica mere", "")).strip() or None,
            row.get("Masa(kg)", None),
            row.get("Stanje", None),
            row.get("Početna količina", None),
            row.get("Početna nabavna vrednost", None),
            row.get("Početna prodajna vrednost", None),
            row.get("Količina prijema", None),
            row.get("Nabavna vrednost prijema", None),
            row.get("Prodajna vrednost prijema", None),
            row.get("Količina izdavanja", None),
            row.get("Nabavna vrednost izdavanja", None),
            row.get("Prodajna vrednost izdavanja", None),
            row.get("Stanje.1", None),
            row.get("Konačna količina", None),
            row.get("Konačna nabavna vrednost", None),
            row.get("Konačna prodajna vrednost", None),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            import_id,
        )
        conn.execute(
            "INSERT OR REPLACE INTO minimax_items ("
            "sku, name, unit, mass_kg, stock, "
            "opening_qty, opening_purchase_value, opening_sales_value, "
            "incoming_qty, incoming_purchase_value, incoming_sales_value, "
            "outgoing_qty, outgoing_purchase_value, outgoing_sales_value, "
            "stock_2, closing_qty, closing_purchase_value, closing_sales_value, "
            "updated_at, import_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )

    conn.commit()
