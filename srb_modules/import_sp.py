from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .import_common import append_reject, format_missing_int_ranges, start_import


def get_or_create_order(conn: sqlite3.Connection, sp_order_no: str, values: dict) -> tuple[int, bool]:
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


def maybe_mark_delivered_from_payment(conn: sqlite3.Connection, sp_order_no: str) -> None:
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
            ("Isporučeno", order_id),
        )
        add_status_history(conn, order_id, "Isporučeno", "", "SP-Uplate")


def import_sp_orders(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    col: dict[str, str],
    sheet_orders: str,
    file_hash: Callable[[Path], str],
    compute_customer_key: Callable[[Any, Any, Any, Any], str],
    set_app_state: Callable[[Any, str, str], None],
) -> None:
    df = pd.read_excel(path, sheet_name=sheet_orders)
    import_id = start_import(conn, "SP-Narudzbe", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "SP-Narudzbe", path.name, None, "file_already_imported", "")
        return

    db_max = conn.execute(
        "SELECT MAX(CAST(sp_order_no AS INTEGER)) "
        "FROM orders WHERE sp_order_no IS NOT NULL AND TRIM(sp_order_no) != ''"
    ).fetchone()[0]
    try:
        db_max_int = int(db_max) if db_max is not None else None
    except Exception:
        db_max_int = None

    seen_orders = set()
    file_numbers: set[int] = set()
    max_sp_order_no = None
    for idx, row in df.iterrows():
        sp_order_no = str(row.get(col["sp_order_no"], "")).strip()
        if not sp_order_no:
            continue
        try:
            sp_order_int = int(float(sp_order_no))
            file_numbers.add(sp_order_int)
            if max_sp_order_no is None or sp_order_int > max_sp_order_no:
                max_sp_order_no = sp_order_int
        except ValueError:
            pass

        customer_key = compute_customer_key(
            row.get(col["phone"], None),
            row.get(col["email"], None),
            row.get(col["customer_name"], None),
            row.get(col["city"], None),
        )
        values = {
            "sp_order_no": sp_order_no,
            "woo_order_no": str(row.get(col["woo_order_no"], "")).strip() or None,
            "client_code": str(row.get(col["client"], "")).strip() or None,
            "tracking_code": str(row.get(col["tracking"], "")).strip() or None,
            "customer_code": str(row.get(col["customer_code"], "")).strip() or None,
            "customer_name": str(row.get(col["customer_name"], "")).strip() or None,
            "city": str(row.get(col["city"], "")).strip() or None,
            "address": str(row.get(col["address"], "")).strip() or None,
            "postal_code": str(row.get(col["postal_code"], "")).strip() or None,
            "phone": str(row.get(col["phone"], "")).strip() or None,
            "email": str(row.get(col["email"], "")).strip() or None,
            "customer_key": customer_key or None,
            "note": str(row.get(col["note"], "")).strip() or None,
            "location": str(row.get(col["location"], "")).strip() or None,
            "status": str(row.get(col["status"], "")).strip() or None,
            "created_at": str(row.get(col["created_at"], "")).strip() or None,
            "picked_up_at": str(row.get(col["picked_up_at"], "")).strip() or None,
            "delivered_at": str(row.get(col["delivered_at"], "")).strip() or None,
            "import_run_id": import_id,
        }

        order_id, created = get_or_create_order(conn, sp_order_no, values)
        if not created:
            append_reject(
                rejects,
                "SP-Narudzbe",
                path.name,
                int(idx) + 1,
                "order_exists",
                f"sp_order_no={sp_order_no}",
            )

        item_values = (
            order_id,
            str(row.get(col["product_code"], "")).strip() or None,
            row.get(col["qty"], None),
            row.get(col["cod_amount"], None),
            row.get(col["advance_amount"], None),
            row.get(col["discount"], None),
            str(row.get(col["discount_type"], "")).strip() or None,
            row.get(col["addon_cod"], None),
            row.get(col["addon_advance"], None),
            row.get(col["extra_discount"], None),
            str(row.get(col["extra_discount_type"], "")).strip() or None,
        )
        cur = conn.execute(
            "INSERT OR IGNORE INTO order_items ("
            "order_id, product_code, qty, cod_amount, advance_amount, "
            "discount, discount_type, addon_cod, addon_advance, "
            "extra_discount, extra_discount_type"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            item_values,
        )
        if cur.rowcount == 0:
            append_reject(
                rejects,
                "SP-Narudzbe",
                path.name,
                int(idx) + 1,
                "item_duplicate",
                f"sp_order_no={sp_order_no}, sku={item_values[1]}",
            )

        if sp_order_no not in seen_orders:
            status = values["status"] or ""
            status_at = values["delivered_at"] if status.lower() == "isporučeno" else values["picked_up_at"]
            if status:
                add_status_history(
                    conn,
                    order_id,
                    status,
                    status_at or values["created_at"] or "",
                    "SP-Narudzbe",
                )
            seen_orders.add(sp_order_no)

    conn.commit()
    if db_max_int is not None and max_sp_order_no is not None and max_sp_order_no > db_max_int:
        expected_start = db_max_int + 1
        expected_end = int(max_sp_order_no)
        gaps = format_missing_int_ranges(expected_start, file_numbers, expected_end)
        if gaps:
            append_reject(
                rejects,
                "SP-Narudzbe",
                path.name,
                None,
                "gap_warning",
                f"Ocekivano {expected_start}..{expected_end}, nedostaje: {gaps}",
            )
    if max_sp_order_no is not None:
        set_app_state(conn, "last_sp_order_no", str(max_sp_order_no))
        conn.commit()


def import_sp_payments(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    col: dict[str, str],
    sheet_payments: str,
    file_hash: Callable[[Path], str],
) -> None:
    df = pd.read_excel(path, sheet_name=sheet_payments)
    import_id = start_import(conn, "SP-Uplate", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "SP-Uplate", path.name, None, "file_already_imported", "")
        return

    for idx, row in df.iterrows():
        sp_order_no = str(row.get(col["sp_order_no"], "")).strip()
        if not sp_order_no:
            continue
        values = (
            sp_order_no,
            str(row.get(col["client"], "")).strip() or None,
            str(row.get(col["customer_code"], "")).strip() or None,
            str(row.get(col["payment_customer_name"], "")).strip() or None,
            row.get(col["payment_amount"], None),
            str(row.get(col["payment_order_status"], "")).strip() or None,
            str(row.get(col["payment_client_status"], "")).strip() or None,
            import_id,
        )
        cur = conn.execute(
            "INSERT OR IGNORE INTO payments ("
            "sp_order_no, client_code, customer_code, customer_name, amount, "
            "order_status, client_status, import_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        if cur.rowcount == 0:
            append_reject(
                rejects,
                "SP-Uplate",
                path.name,
                int(idx) + 1,
                "payment_duplicate",
                f"sp_order_no={sp_order_no}, amount={values[4]}, status={values[6]}",
            )
        maybe_mark_delivered_from_payment(conn, sp_order_no)

    conn.commit()


def import_sp_returns(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    col: dict[str, str],
    sheet_orders: str,
    file_hash: Callable[[Path], str],
) -> None:
    df = pd.read_excel(path, sheet_name=sheet_orders)
    import_id = start_import(conn, "SP-Preuzimanja", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "SP-Preuzimanja", path.name, None, "file_already_imported", "")
        return

    for idx, row in df.iterrows():
        sp_order_no = str(row.get(col["sp_order_no"], "")).strip() or None
        values = (
            sp_order_no,
            str(row.get(col["tracking"], "")).strip() or None,
            str(row.get(col["customer_name"], "")).strip() or None,
            str(row.get(col["phone"], "")).strip() or None,
            str(row.get(col["city"], "")).strip() or None,
            str(row.get(col["status"], "")).strip() or None,
            str(row.get(col["created_at"], "")).strip() or None,
            str(row.get(col["picked_up_at"], "")).strip() or None,
            str(row.get(col["delivered_at"], "")).strip() or None,
            import_id,
        )
        cur = conn.execute(
            "INSERT OR IGNORE INTO returns ("
            "sp_order_no, tracking_code, customer_name, phone, city, status, "
            "created_at, picked_up_at, delivered_at, import_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        if cur.rowcount == 0:
            append_reject(
                rejects,
                "SP-Preuzimanja",
                path.name,
                int(idx) + 1,
                "return_duplicate",
                f"sp_order_no={sp_order_no}, tracking={values[1]}",
            )

    conn.commit()
