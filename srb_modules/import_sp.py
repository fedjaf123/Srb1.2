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

    # SP exports are inconsistent for qty>1 lines:
    # - sometimes `cod_amount` is a unit price (e.g. 3990) and should be multiplied by qty
    # - sometimes `cod_amount` is already a line total (e.g. 7980 for qty=2) and must NOT be multiplied again
    # We normalize during import so DB always stores unit prices for `cod_amount` and `advance_amount`.
    def _to_float(v) -> float | None:
        if v is None:
            return None
        try:
            if isinstance(v, str) and not v.strip():
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    def _round2(v: float) -> float:
        return round(float(v), 2)

    def _approx(a: float, b: float, tol: float = 0.01) -> bool:
        try:
            return abs(float(a) - float(b)) <= tol
        except Exception:
            return False

    # Build per-SKU unit price candidates from qty==1 rows.
    unit_cod_map: dict[str, float] = {}
    unit_adv_map: dict[str, float] = {}
    cod_candidates: dict[str, list[float]] = {}
    adv_candidates: dict[str, list[float]] = {}

    skus_in_file: set[str] = set()
    for _, r in df.iterrows():
        sku = str(r.get(col["product_code"], "")).strip()
        if not sku:
            continue
        skus_in_file.add(sku)
        qty = _to_float(r.get(col["qty"], None))
        if qty is None or _round2(qty) != 1.0:
            continue
        cod = _to_float(r.get(col["cod_amount"], None))
        if cod is not None and cod > 0:
            cod_candidates.setdefault(sku, []).append(_round2(cod))
        adv = _to_float(r.get(col["advance_amount"], None))
        if adv is not None and adv > 0:
            adv_candidates.setdefault(sku, []).append(_round2(adv))

    # Choose the most common candidate as unit value (mode). Fallback: last value.
    for sku, vals in cod_candidates.items():
        if not vals:
            continue
        counts: dict[float, int] = {}
        for v in vals:
            counts[v] = counts.get(v, 0) + 1
        unit_cod_map[sku] = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]
    for sku, vals in adv_candidates.items():
        if not vals:
            continue
        counts = {}
        for v in vals:
            counts[v] = counts.get(v, 0) + 1
        unit_adv_map[sku] = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

    # Also learn unit prices from the existing DB (qty==1 rows), limited to SKUs present in this file.
    # This handles cases where the file contains only qty>1 totals (e.g. qty=2, cod_amount=7980 for unit 3990).
    if skus_in_file:
        sku_list = sorted(skus_in_file)
        chunk_size = 800
        for i in range(0, len(sku_list), chunk_size):
            chunk = sku_list[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            for sku, cod in conn.execute(
                "SELECT product_code, cod_amount "
                "FROM order_items "
                f"WHERE qty = 1 AND product_code IN ({placeholders}) "
                "AND cod_amount IS NOT NULL AND cod_amount > 0",
                chunk,
            ).fetchall():
                sku = str(sku or "").strip()
                if not sku:
                    continue
                cod_f = _to_float(cod)
                if cod_f is None or cod_f <= 0:
                    continue
                cod_candidates.setdefault(sku, []).append(_round2(cod_f))

            for sku, adv in conn.execute(
                "SELECT product_code, advance_amount "
                "FROM order_items "
                f"WHERE qty = 1 AND product_code IN ({placeholders}) "
                "AND advance_amount IS NOT NULL AND advance_amount > 0",
                chunk,
            ).fetchall():
                sku = str(sku or "").strip()
                if not sku:
                    continue
                adv_f = _to_float(adv)
                if adv_f is None or adv_f <= 0:
                    continue
                adv_candidates.setdefault(sku, []).append(_round2(adv_f))

        # Re-pick modes after adding DB candidates.
        for sku, vals in cod_candidates.items():
            if not vals:
                continue
            counts: dict[float, int] = {}
            for v in vals:
                counts[v] = counts.get(v, 0) + 1
            unit_cod_map[sku] = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]
        for sku, vals in adv_candidates.items():
            if not vals:
                continue
            counts: dict[float, int] = {}
            for v in vals:
                counts[v] = counts.get(v, 0) + 1
            unit_adv_map[sku] = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

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

        # Normalize qty>1 lines so cod_amount/advance_amount are stored as unit values.
        # SP export typically provides totals for qty>1 (e.g. 2x3990 => 7980), but occasionally exports unit values.
        # We store unit values in DB so all downstream logic can safely do qty * unit_price.
        try:
            sku = item_values[1]
            qty = _to_float(item_values[2])
            cod = _to_float(item_values[3])
            adv = _to_float(item_values[4])
            if sku and qty and qty > 1:
                unit_cod = unit_cod_map.get(str(sku))
                unit_adv = unit_adv_map.get(str(sku))

                if cod is not None and cod > 0:
                    # If the file already gives unit (cod == known unit), keep it.
                    if unit_cod is not None and _approx(cod, unit_cod):
                        pass
                    else:
                        # Otherwise treat cod as line total and normalize to unit.
                        item_values = (
                            item_values[0],
                            item_values[1],
                            item_values[2],
                            _round2(cod / qty),
                            item_values[4],
                            item_values[5],
                            item_values[6],
                            item_values[7],
                            item_values[8],
                            item_values[9],
                            item_values[10],
                        )
                        append_reject(
                            rejects,
                            "SP-Narudzbe",
                            path.name,
                            int(idx) + 1,
                            "normalized_cod_total_to_unit",
                            f"sp_order_no={sp_order_no}, sku={sku}, qty={qty}, cod_total={cod}",
                        )

                if adv is not None and adv > 0:
                    if unit_adv is not None and _approx(adv, unit_adv):
                        pass
                    else:
                        item_values = (
                            item_values[0],
                            item_values[1],
                            item_values[2],
                            item_values[3],
                            _round2(adv / qty),
                            item_values[5],
                            item_values[6],
                            item_values[7],
                            item_values[8],
                            item_values[9],
                            item_values[10],
                        )
                        append_reject(
                            rejects,
                            "SP-Narudzbe",
                            path.name,
                            int(idx) + 1,
                            "normalized_advance_total_to_unit",
                            f"sp_order_no={sp_order_no}, sku={sku}, qty={qty}, adv_total={adv}",
                        )
        except Exception:
            # Best-effort: keep original values if normalization fails for any reason.
            pass

        cur = conn.execute(
            "INSERT OR IGNORE INTO order_items ("
            "order_id, product_code, qty, cod_amount, advance_amount, "
            "discount, discount_type, addon_cod, addon_advance, "
            "extra_discount, extra_discount_type"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            item_values,
        )
        if cur.rowcount == 0:
            # Some SP exports repeat identical item lines (same SKU/qty/unit price/discounts) instead of using qty>1
            # or using one aggregated row. Our UNIQUE index drops the duplicate row, which undercounts qty and breaks
            # matching/analytics. Merge duplicates by adding qty (and advance_amount), while keeping unit prices the same.
            try:
                existing = conn.execute(
                    "SELECT id FROM order_items "
                    "WHERE order_id = ? "
                    "AND product_code IS ? "
                    "AND cod_amount IS ? "
                    "AND discount IS ? "
                    "AND extra_discount IS ? "
                    "LIMIT 1",
                    (
                        order_id,
                        item_values[1],
                        item_values[3],
                        item_values[5],
                        item_values[9],
                    ),
                ).fetchone()
                if existing:
                    existing_id = int(existing[0])
                    conn.execute(
                        "UPDATE order_items SET "
                        "qty = COALESCE(qty, 0) + COALESCE(?, 0), "
                        "advance_amount = COALESCE(advance_amount, 0) + COALESCE(?, 0), "
                        "addon_cod = MAX(COALESCE(addon_cod, 0), COALESCE(?, 0)), "
                        "addon_advance = MAX(COALESCE(addon_advance, 0), COALESCE(?, 0)), "
                        "extra_discount = MAX(COALESCE(extra_discount, 0), COALESCE(?, 0)) "
                        "WHERE id = ?",
                        (
                            item_values[2],
                            item_values[4],
                            item_values[7],
                            item_values[8],
                            item_values[9],
                            existing_id,
                        ),
                    )
                    append_reject(
                        rejects,
                        "SP-Narudzbe",
                        path.name,
                        int(idx) + 1,
                        "item_merged_duplicate",
                        f"sp_order_no={sp_order_no}, sku={item_values[1]}",
                    )
                else:
                    append_reject(
                        rejects,
                        "SP-Narudzbe",
                        path.name,
                        int(idx) + 1,
                        "item_duplicate",
                        f"sp_order_no={sp_order_no}, sku={item_values[1]}",
                    )
            except Exception:
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
