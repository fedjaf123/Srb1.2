from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections.abc import Callable
from datetime import date, datetime, timedelta

import pandas as pd


def date_expr(column: str) -> str:
    return (
        "CASE "
        f"WHEN {column} IS NULL THEN NULL "
        f"WHEN substr({column}, 3, 1) = '.' AND substr({column}, 6, 1) = '.' "
        f"THEN date(substr({column}, 7, 4) || '-' || substr({column}, 4, 2) || '-' || substr({column}, 1, 2)) "
        f"ELSE date(substr({column}, 1, 10)) "
        "END"
    )


def date_filter_clause(
    column: str, days: int | None, start: str | None = None, end: str | None = None
):
    params: list[str] = []
    clauses: list[str] = []
    expr = date_expr(column)
    if start:
        clauses.append(f"{expr} >= date(?)")
        params.append(start)
    if end:
        clauses.append(f"{expr} <= date(?)")
        params.append(end)
    if not start and not end and days:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        clauses.append(f"{expr} >= date(?)")
        params.append(start_date)
    if not clauses:
        return "", []
    clause = " AND " + " AND ".join(clauses)
    return clause, params


def get_top_customers(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    rows = conn.execute(
        "WITH od AS ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "  FROM order_items "
        "  GROUP BY order_id"
        "), "
        "order_totals AS ("
        "  SELECT "
        "    o.id AS order_id, "
        "    o.customer_key AS customer_key, "
        "    ("
        "      COALESCE(SUM("
        "        COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "        * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "        * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "      ), 0) "
        "      + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "    ) AS cash_total "
        "  FROM orders o "
        "  LEFT JOIN order_items oi ON oi.order_id = o.id "
        "  LEFT JOIN od ON od.order_id = o.id "
        "  WHERE o.customer_key IS NOT NULL AND TRIM(o.customer_key) != '' "
        "    AND o.created_at IS NOT NULL "
        "    AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause
        + "  GROUP BY o.id"
        ") "
        "SELECT "
        "  COALESCE(NULLIF(TRIM(MAX(o.customer_name)), ''), ot.customer_key) AS display_name, "
        "  COUNT(DISTINCT ot.order_id) AS orders_cnt, "
        "  SUM(COALESCE(ot.cash_total, 0)) AS net_total "
        "FROM order_totals ot "
        "JOIN orders o ON o.id = ot.order_id "
        "GROUP BY ot.customer_key "
        "ORDER BY net_total DESC "
        "LIMIT ?",
        (*params, limit),
    ).fetchall()
    return rows


def get_top_products(
    conn: sqlite3.Connection,
    limit: int = 10,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    rows = conn.execute(
        "WITH od AS ("
        "  SELECT order_id, MAX(COALESCE(discount, 0)) AS order_discount "
        "  FROM order_items "
        "  GROUP BY order_id"
        ") "
        "SELECT oi.product_code, "
        "  SUM(COALESCE(oi.qty, 0)) AS total_qty, "
        "  SUM("
        "    COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "    * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "    * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "  ) AS net_total "
        "FROM order_items oi "
        "JOIN orders o ON o.id = oi.order_id "
        "LEFT JOIN od ON od.order_id = o.id "
        "WHERE oi.product_code IS NOT NULL AND TRIM(oi.product_code) != '' "
        "  AND o.created_at IS NOT NULL "
        "  AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause
        + " GROUP BY oi.product_code "
        "ORDER BY net_total DESC "
        "LIMIT ?",
        (*params, limit),
    ).fetchall()
    return rows


def get_top_products_qty(
    conn: sqlite3.Connection,
    limit: int = 10,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    rows = conn.execute(
        "SELECT oi.product_code, "
        "SUM(COALESCE(oi.qty, 0)) AS total_qty "
        "FROM order_items oi "
        "JOIN orders o ON o.id = oi.order_id "
        "WHERE oi.product_code IS NOT NULL AND oi.product_code != '' "
        "AND o.created_at IS NOT NULL "
        "AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause
        + " GROUP BY oi.product_code "
        "ORDER BY total_qty DESC "
        "LIMIT ?",
        (*params, limit),
    ).fetchall()
    return rows


def get_top_categories_qty_share(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    *,
    categorize_sku: Callable[[str], str] | None = None,
) -> list[tuple[str, float, float]]:
    if categorize_sku is None:
        raise ValueError("categorize_sku callback is required for category totals")
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    sku_rows = conn.execute(
        "SELECT oi.product_code, SUM(COALESCE(oi.qty, 0)) AS total_qty "
        "FROM order_items oi "
        "JOIN orders o ON o.id = oi.order_id "
        "WHERE oi.product_code IS NOT NULL AND oi.product_code != '' "
        "AND o.created_at IS NOT NULL "
        "AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause
        + " GROUP BY oi.product_code ",
        params,
    ).fetchall()
    totals: dict[str, float] = {}
    total_all = 0.0
    for sku, qty in sku_rows:
        if not sku:
            continue
        q = float(qty or 0.0)
        total_all += q
        cat = str(categorize_sku(str(sku)))
        totals[cat] = totals.get(cat, 0.0) + q
    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    out: list[tuple[str, float, float]] = []
    for cat, qty in ranked[:limit]:
        pct = (qty / total_all * 100.0) if total_all else 0.0
        out.append((cat, qty, pct))
    return out


def get_kpis(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    total_orders = conn.execute(
        "SELECT COUNT(*) FROM orders o "
        "WHERE o.created_at IS NOT NULL "
        "AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause,
        params,
    ).fetchone()[0]
    total_revenue = conn.execute(
        "WITH od AS ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "  FROM order_items "
        "  GROUP BY order_id"
        "), "
        "order_totals AS ("
        "  SELECT "
        "    o.id AS order_id, "
        "    ("
        "      COALESCE(SUM("
        "        COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "        * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "        * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "      ), 0) "
        "      + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "    ) AS cash_total "
        "  FROM orders o "
        "  LEFT JOIN order_items oi ON oi.order_id = o.id "
        "  LEFT JOIN od ON od.order_id = o.id "
        "  WHERE o.created_at IS NOT NULL "
        "    AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + date_clause
        + "  GROUP BY o.id"
        ") "
        "SELECT SUM(COALESCE(cash_total, 0)) FROM order_totals",
        params,
    ).fetchone()[0]
    unpicked_clause, unpicked_params = date_filter_clause("o.created_at", days, start, end)
    total_unpicked = conn.execute(
        "SELECT COUNT(*) FROM orders o "
        "WHERE o.status LIKE '%Vra\u0107eno%' " + unpicked_clause,
        unpicked_params,
    ).fetchone()[0]
    cutoff_expr = date_expr("COALESCE(o.picked_up_at, o.created_at)")
    unmatched = conn.execute(
        "SELECT COUNT(*) FROM ("
        "SELECT o.id "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%obradi%') "
        f"AND {cutoff_expr} <= date('now', '-3 day') "
        "AND o.created_at IS NOT NULL "
        + date_clause
        + " GROUP BY o.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") != 0"
        ") t",
        params,
    ).fetchone()[0]
    return {
        "total_orders": total_orders or 0,
        "total_revenue": float(total_revenue or 0),
        "total_returns": total_unpicked or 0,
        "unmatched": int(unmatched or 0),
    }


def get_sp_bank_monthly(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("dtposted", days, start, end)
    rows = conn.execute(
        "SELECT substr(dtposted, 1, 7) AS period, "
        "SUM(CASE WHEN benefit = 'credit' "
        "AND lower(COALESCE(purpose, '')) NOT LIKE '%pozajmica%' "
        "THEN amount ELSE 0 END) AS income, "
        "("
        "SUM(CASE WHEN benefit = 'debit' "
        "AND lower(COALESCE(purpose, '')) NOT LIKE '%kupoprodaja deviza%' "
        "AND COALESCE(purposecode, '') != '286' "
        "AND lower(COALESCE(purpose, '')) NOT LIKE '%carin%' "
        "THEN amount ELSE 0 END) "
        "+ SUM(CASE WHEN benefit = 'debit' "
        "AND lower(COALESCE(purpose, '')) LIKE '%carin%' "
        "THEN amount * 0.20 ELSE 0 END)"
        ") AS expense "
        "FROM bank_transactions "
        "WHERE dtposted IS NOT NULL " + date_clause + " GROUP BY period "
        "ORDER BY period",
        params,
    ).fetchall()
    return rows


def get_finansije_monthly(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[tuple[str, float, float, float]]:
    """
    Returns rows: (period_yyyy_mm, bruto_cash, troskovi, neto)
    - bruto_cash: SP COD (+dodatno) poslije popusta, po orders.created_at, bez Vraćeno/Vraceno
    - troskovi: bank debit totals (existing filters), po dtposted
    """
    order_date_clause, order_params = date_filter_clause("o.created_at", days, start, end)
    order_expr = date_expr("o.created_at")
    bruto_rows = conn.execute(
        "WITH od AS ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "  FROM order_items "
        "  GROUP BY order_id"
        "), "
        "order_totals AS ("
        "  SELECT "
        "    substr(" + order_expr + ", 1, 7) AS period, "
        "    o.id AS order_id, "
        "    ("
        "      COALESCE(SUM("
        "        COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "        * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "        * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "      ), 0) "
        "      + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "    ) AS cash_total "
        "  FROM orders o "
        "  LEFT JOIN order_items oi ON oi.order_id = o.id "
        "  LEFT JOIN od ON od.order_id = o.id "
        "  WHERE o.created_at IS NOT NULL "
        "    AND (o.status IS NULL OR (o.status NOT LIKE '%Vra\u0107eno%' AND o.status NOT LIKE '%Vraceno%')) "
        + order_date_clause
        + "  GROUP BY o.id"
        ") "
        "SELECT period, SUM(COALESCE(cash_total, 0)) AS bruto_cash "
        "FROM order_totals "
        "GROUP BY period "
        "ORDER BY period",
        order_params,
    ).fetchall()

    bank_rows = get_sp_bank_monthly(conn, days, start, end)

    by_period: dict[str, dict[str, float]] = {}
    for period, bruto_cash in bruto_rows:
        if not period:
            continue
        by_period.setdefault(period, {})["bruto_cash"] = float(bruto_cash or 0.0)
    for period, _, expense in bank_rows:
        if not period:
            continue
        by_period.setdefault(period, {})["troskovi"] = float(expense or 0.0)

    out: list[tuple[str, float, float, float]] = []
    for period in sorted(by_period.keys()):
        bruto_cash = float(by_period.get(period, {}).get("bruto_cash", 0.0))
        troskovi = float(by_period.get(period, {}).get("troskovi", 0.0))
        out.append((period, bruto_cash, troskovi, bruto_cash - troskovi))
    return out


def _normalize_expense_key(text: str) -> str:
    value = re.sub(r"\s+", " ", (text or "").strip().lower())
    return value or "nepoznato"


def _expense_category(payee_name: str | None, purpose: str | None) -> str:
    text = normalize_text_loose(" ".join([str(payee_name or ""), str(purpose or "")]))
    if "slanje paketa" in text:
        return "Slanje Paketa"
    if (
        "svrha doprinosi" in text
        or "uplata poreza i doprinosa po odbitku" in text
        or "doprinosi" in text
    ):
        return "Doprinosi za socijalno osiguranje"
    if "840-0000714112843-10" in text:
        return "PDV"
    if "placanje pdv" in text or "plaćanje pdv" in text:
        return "PDV"
    label = (payee_name or "").strip()
    if not label:
        label = (purpose or "").strip()
    return label or "Nepoznato"


def _extract_month_year(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\b(0[1-9]|1[0-2])\.(20\d{2})\b", text)
    if not match:
        return None
    month, year = match.group(1), match.group(2)
    return f"{year}-{month}"


def _effective_expense_period(
    dtposted: str | None, purpose: str | None, category: str | None
) -> str | None:
    explicit = _extract_month_year(purpose)
    if explicit:
        return explicit
    if not dtposted:
        return None
    try:
        dt = pd.to_datetime(dtposted, errors="coerce")
    except Exception:
        return None
    if pd.isna(dt):
        return None
    if category == "PDV":
        dt = dt - pd.DateOffset(months=1)
    return dt.strftime("%Y-%m")


def _is_forex_expense(purpose: str | None, purposecode: str | None) -> bool:
    code = str(purposecode or "").strip()
    if code == "286":
        return True
    text = normalize_text_loose(purpose)
    return "kupoprodaja deviza" in text


def _expense_amount(
    purpose: str | None, purposecode: str | None, amount: float
) -> float | None:
    if _is_forex_expense(purpose, purposecode):
        return None
    text = normalize_text_loose(purpose)
    if "carin" in text:
        return amount * 0.20
    return amount


def get_expense_summary(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> dict:
    date_clause, params = date_filter_clause("dtposted", days, start, end)
    rows = conn.execute(
        "SELECT dtposted, amount, purpose, purposecode, payee_name "
        "FROM bank_transactions "
        "WHERE benefit = 'debit' AND dtposted IS NOT NULL " + date_clause,
        params,
    ).fetchall()
    total = 0.0
    totals: dict[str, float] = {}
    display_names: dict[str, str] = {}
    monthly: dict[str, dict[str, float]] = {}
    for dtposted, amount, purpose, purposecode, payee_name in rows:
        try:
            amt = float(amount or 0)
        except (TypeError, ValueError):
            continue
        final_amount = _expense_amount(purpose, purposecode, amt)
        if final_amount is None:
            continue
        label = _expense_category(payee_name, purpose)
        key = _normalize_expense_key(label)
        period = _effective_expense_period(dtposted, purpose, label)
        if year and (not period or not period.startswith(year)):
            continue
        if month and (not period or len(period) < 7 or period[5:7] != month):
            continue
        totals[key] = totals.get(key, 0.0) + final_amount
        if key not in display_names:
            display_names[key] = label
        total += final_amount
        if period:
            if period not in monthly:
                monthly[period] = {}
            monthly[period][key] = monthly[period].get(key, 0.0) + final_amount
    return {
        "total": total,
        "totals": totals,
        "display_names": display_names,
        "monthly": monthly,
    }


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


def to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
        by_order: dict[int, dict[str, float]] = {}
        order_discount_map: dict[int, float] = {}
        for row in rows:
            order_id = int(row[0])
            order_disc = to_float(row[6]) or 0.0
            if order_disc > order_discount_map.get(order_id, 0.0):
                order_discount_map[order_id] = order_disc
            entry = by_order.get(order_id)
            if not entry:
                entry = {
                    "cod_sum": 0.0,
                    "advance_sum": 0.0,
                    "addon_cod_max": 0.0,
                    "addon_adv_max": 0.0,
                }
                by_order[order_id] = entry
            addon_cod = to_float(row[3]) or 0.0
            addon_adv = to_float(row[5]) or 0.0
            if addon_cod > entry["addon_cod_max"]:
                entry["addon_cod_max"] = addon_cod
            if addon_adv > entry["addon_adv_max"]:
                entry["addon_adv_max"] = addon_adv

        for row in rows:
            order_id = int(row[0])
            entry = by_order.get(order_id)
            if not entry:
                continue
            order_discount = order_discount_map.get(order_id, 0.0)
            item_discount = row[7]
            qty = to_float(row[1]) or 0.0
            cod_unit = to_float(row[2]) or 0.0
            cod_line_total = qty * cod_unit
            cod_line = apply_percent_chain(cod_line_total, [order_discount, item_discount])
            entry["cod_sum"] += float(cod_line or 0.0)
            adv_unit = to_float(row[4]) or 0.0
            adv_line_total = qty * adv_unit
            adv_line = apply_percent_chain(adv_line_total, [order_discount, item_discount])
            entry["advance_sum"] += float(adv_line or 0.0)

        for order_id, entry in by_order.items():
            order_discount = order_discount_map.get(order_id, 0.0)
            shipping = apply_percent_chain(entry["addon_cod_max"], [order_discount])
            net_map[order_id] = (
                float(entry["cod_sum"] or 0.0)
                + float(shipping or 0.0)
                + float(entry["advance_sum"] or 0.0)
                + float(entry["addon_adv_max"] or 0.0)
            )
    return net_map


def get_unpicked_rows(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.phone, o.email, o.city, "
        "o.status, o.created_at, o.customer_key, o.tracking_code, o.picked_up_at, o.delivered_at "
        "FROM orders o "
        "WHERE o.status IS NOT NULL " + date_clause,
        params,
    ).fetchall()
    return [row for row in rows if is_unpicked_status(row[6])]


def _order_items_for_orders(conn: sqlite3.Connection, order_ids: list[int]):
    if not order_ids:
        return []
    rows = []
    chunk_size = 900
    for i in range(0, len(order_ids), chunk_size):
        chunk = order_ids[i : i + chunk_size]
        placeholders = ",".join("?" * len(chunk))
        rows.extend(
            conn.execute(
                "SELECT order_id, product_code, qty, cod_amount, addon_cod, "
                "advance_amount, addon_advance "
                f"FROM order_items WHERE order_id IN ({placeholders})",
                chunk,
            ).fetchall()
        )
    return rows


def _net_simple(cod, addon, advance, addon_advance) -> float:
    return (
        float(cod or 0)
        + float(addon or 0)
        - float(advance or 0)
        - float(addon_advance or 0)
    )


def get_unpicked_stats(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_unpicked_rows(conn, days, start, end)
    order_ids = [int(r[0]) for r in rows]
    items = _order_items_for_orders(conn, order_ids)
    order_net = {}
    for order_id, _, _, cod, addon, adv, addon_adv in items:
        order_net[order_id] = order_net.get(order_id, 0.0) + _net_simple(
            cod, addon, adv, addon_adv
        )
    total_lost = sum(order_net.values())

    groups = {}
    for _, _, name, phone, email, city, _, _, stored_key, *_ in rows:
        key = stored_key or compute_customer_key(phone, email, name, city)
        if not key:
            continue
        groups[key] = groups.get(key, 0) + 1
    repeat_customers = sum(1 for cnt in groups.values() if cnt >= 2)

    return {
        "unpicked_orders": len(order_ids),
        "lost_sales": float(total_lost or 0),
        "repeat_customers": repeat_customers,
    }


def _pick_display_name(name_counts: dict) -> str:
    if not name_counts:
        return ""
    return max(name_counts.items(), key=lambda x: (x[1], len(x[0])))[0]


def get_unpicked_customer_groups(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_unpicked_rows(conn, days, start, end)
    if not rows:
        return [], []
    groups = {}
    for row in rows:
        sp_order_no = row[1]
        name = row[2]
        phone = row[3]
        email = row[4]
        city = row[5]
        stored_key = row[8] if len(row) > 8 else None
        key = stored_key or compute_customer_key(phone, email, name, city)
        if not key:
            continue
        group = groups.setdefault(
            key,
            {
                "count": 0,
                "names": {},
                "phones": set(),
                "emails": set(),
                "orders": set(),
            },
        )
        group["count"] += 1
        if name:
            group["names"][name] = group["names"].get(name, 0) + 1
        phone_norm = normalize_phone(phone)
        if phone_norm:
            group["phones"].add(phone_norm)
        if email:
            email_text = str(email).strip()
            if "@" in email_text:
                group["emails"].add(email_text)
            else:
                alt_phone = normalize_phone(email_text)
                if alt_phone:
                    group["phones"].add(alt_phone)
        if sp_order_no:
            group["orders"].add(str(sp_order_no))

    ranked = sorted(groups.items(), key=lambda x: x[1]["count"], reverse=True)
    top = []
    details = []
    for key, info in ranked:
        key_display = key
        if key.startswith("phone:"):
            key_display = key.replace("phone:", "", 1)
        elif key.startswith("email:"):
            key_display = key.replace("email:", "", 1)
        name = _pick_display_name(info["names"]) or key_display
        top.append((name, info["count"]))
        names_list = ", ".join(sorted(info["names"].keys()))
        phones_list = ", ".join(sorted(info["phones"]))
        emails_list = ", ".join(sorted(info["emails"]))
        details.append(
            (
                key_display,
                info["count"],
                names_list,
                phones_list,
                emails_list,
                len(info["orders"]),
            )
        )
        if len(details) >= 50:
            break

    return top[:limit], details[:50]


def get_unpicked_top_items(
    conn: sqlite3.Connection,
    limit: int | None = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_unpicked_rows(conn, days, start, end)
    if not rows:
        return []
    order_ids = [int(r[0]) for r in rows]
    items = _order_items_for_orders(conn, order_ids)
    totals = {}
    for _, sku, qty, cod, addon, adv, addon_adv in items:
        if not sku:
            continue
        entry = totals.get(sku, {"qty": 0.0, "net": 0.0})
        entry["qty"] += float(qty or 0)
        entry["net"] += _net_simple(cod, addon, adv, addon_adv)
        totals[sku] = entry
    ranked = sorted(totals.items(), key=lambda x: x[1]["qty"], reverse=True)
    if limit is None:
        return [(sku, vals["qty"], vals["net"]) for sku, vals in ranked]
    return [(sku, vals["qty"], vals["net"]) for sku, vals in ranked[:limit]]


def get_unpicked_category_totals(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    categorize_sku: Callable[[str], str] | None = None,
):
    rows = get_unpicked_rows(conn, days, start, end)
    order_ids = [int(r[0]) for r in rows]
    items = _order_items_for_orders(conn, order_ids)
    if categorize_sku is None:
        raise ValueError("categorize_sku callback is required for category totals")
    totals = {}
    for _, sku, qty, cod, addon, adv, addon_adv in items:
        if not sku:
            continue
        cat = str(categorize_sku(str(sku)))
        entry = totals.get(cat, {"qty": 0.0, "net": 0.0})
        entry["qty"] += float(qty or 0)
        entry["net"] += _net_simple(cod, addon, adv, addon_adv)
        totals[cat] = entry
    ranked = sorted(totals.items(), key=lambda x: x[1]["qty"], reverse=True)
    return [(cat, vals["qty"], vals["net"]) for cat, vals in ranked]


def get_unpicked_orders_list(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_unpicked_rows(conn, days, start, end)
    order_ids = [int(r[0]) for r in rows]
    items = _order_items_for_orders(conn, order_ids)
    order_net = {}
    for order_id, _, _, cod, addon, adv, addon_adv in items:
        order_net[order_id] = order_net.get(order_id, 0.0) + _net_simple(
            cod, addon, adv, addon_adv
        )
    result = []
    for row in rows:
        (
            order_id,
            sp_order_no,
            name,
            phone,
            email,
            city,
            status,
            created_at,
            _,
            tracking_code,
            picked_up_at,
            delivered_at,
        ) = row
        result.append(
            (
                sp_order_no,
                tracking_code,
                name,
                phone,
                email,
                city,
                status,
                created_at,
                picked_up_at,
                delivered_at,
                order_net.get(order_id, 0.0),
            )
        )
    return result


def get_refund_rows(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    date_clause, params = date_filter_clause("bt.dtposted", days, start, end)
    rows = conn.execute(
        "SELECT br.bank_txn_id, bt.dtposted, bt.amount, bt.payee_name, bt.purpose, "
        "br.invoice_no, br.invoice_no_digits "
        "FROM bank_refunds br "
        "JOIN bank_transactions bt ON bt.id = br.bank_txn_id "
        "WHERE bt.dtposted IS NOT NULL " + date_clause + " ORDER BY bt.dtposted",
        params,
    ).fetchall()
    return rows


def get_refund_total_amount(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
) -> float:
    rows = get_refund_rows(conn, days, start, end)
    total = 0.0
    for _txn_id, _dtposted, amount, _payee_name, _purpose, *_rest in rows:
        try:
            total += float(amount or 0.0)
        except (TypeError, ValueError):
            continue
    return float(total or 0.0)


def build_refund_item_totals(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_refund_rows(conn, days, start, end)
    if not rows:
        return {}

    inv_by_number = {}
    inv_by_digits = {}
    for inv_id, inv_no, note, basis in conn.execute(
        "SELECT id, number, note, basis FROM invoices"
    ).fetchall():
        inv_by_number[str(inv_no or "")] = int(inv_id)
        digits = invoice_digits(inv_no)
        if digits:
            inv_by_digits.setdefault(digits, set()).add(int(inv_id))
        for text in (note, basis):
            inv_text, digits_text = extract_invoice_no_from_text(text or "")
            if digits_text:
                inv_by_digits.setdefault(digits_text, set()).add(int(inv_id))

    order_by_invoice = {
        int(inv_id): int(order_id)
        for inv_id, order_id in conn.execute(
            "SELECT invoice_id, order_id FROM invoice_matches"
        ).fetchall()
    }
    # Without explicit invoice->order matching we can't reliably attribute refunds to SKU/category.
    # Keep customer-level refunds from bank as truth; SKU breakdown requires invoice_matches.
    if not order_by_invoice:
        return {}
    storno_map = {
        int(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT storno_invoice_id, original_invoice_id FROM invoice_storno"
        ).fetchall()
    }
    items_by_order = {}
    for oid, sku, qty in conn.execute(
        "SELECT order_id, product_code, qty FROM order_items"
    ).fetchall():
        if not sku:
            continue
        items_by_order.setdefault(int(oid), []).append((str(sku), float(qty or 0)))

    seen_invoices = set()
    totals = {}
    for _, _, _, _, _, inv_no, inv_digits in rows:
        inv_id = None
        if inv_no and str(inv_no) in inv_by_number:
            inv_id = inv_by_number[str(inv_no)]
        elif (
            inv_digits
            and inv_digits in inv_by_digits
            and len(inv_by_digits[inv_digits]) == 1
        ):
            inv_id = next(iter(inv_by_digits[inv_digits]))
        if inv_id in storno_map:
            inv_id = storno_map[inv_id]
        if not inv_id or inv_id in seen_invoices:
            continue
        seen_invoices.add(inv_id)
        order_id = order_by_invoice.get(inv_id)
        if not order_id:
            continue
        for sku, qty in items_by_order.get(order_id, []):
            totals[sku] = totals.get(sku, 0.0) + qty
    return totals


def get_unpaid_sp_orders_summary(
    conn: sqlite3.Connection,
    start_date: str | None,
    end_date: str | None,
) -> tuple[int, float]:
    delivered_expr = date_expr("o.delivered_at")
    where_period = ""
    params: list[object] = []
    if start_date:
        where_period += f" AND {delivered_expr} >= date(?)"
        params.append(start_date)
    if end_date:
        where_period += f" AND {delivered_expr} <= date(?)"
        params.append(end_date)
    row = conn.execute(
        "SELECT COUNT(*) AS orders_cnt, COALESCE(SUM(sp_expected_amount), 0.0) AS sp_sum "
        "FROM ("
        "  SELECT o.id AS order_id, "
        "  ("
        "    COALESCE(SUM("
        "      COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "      * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "      * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "    ), 0)"
        "    + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0)"
        "  ) AS sp_expected_amount "
        "  FROM orders o "
        "  LEFT JOIN order_items oi ON oi.order_id = o.id "
        "  LEFT JOIN ("
        "    SELECT order_id, "
        "      MAX(COALESCE(discount, 0)) AS order_discount, "
        "      MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "    FROM order_items "
        "    GROUP BY order_id"
        "  ) od ON od.order_id = o.id "
        "  LEFT JOIN payments p ON trim(p.sp_order_no) = trim(o.sp_order_no) "
        "  WHERE o.delivered_at IS NOT NULL AND TRIM(o.delivered_at) != '' "
        "  AND o.status IS NOT NULL "
        "  AND (o.status LIKE '%Isporu%' OR o.status LIKE '%isporu%') "
        "  AND p.id IS NULL "
        "  AND (o.status IS NULL OR (lower(o.status) NOT LIKE '%otkazan%' "
        "    AND lower(o.status) NOT LIKE '%obradi%' "
        "    AND lower(o.status) NOT LIKE '%vraceno%' "
        "    AND lower(o.status) NOT LIKE '%vra\u0107eno%')) "
        + where_period
        + "  GROUP BY o.id "
        "  HAVING sp_expected_amount > 0"
        ") t",
        params,
    ).fetchone()
    if not row:
        return (0, 0.0)
    return (int(row[0] or 0), float(row[1] or 0.0))


def get_unpaid_sp_orders_details(
    conn: sqlite3.Connection,
    start_date: str | None,
    end_date: str | None,
) -> tuple[list[str], list[tuple]]:
    delivered_expr = date_expr("o.delivered_at")
    where_period = ""
    params: list[object] = []
    if start_date:
        where_period += f" AND {delivered_expr} >= date(?)"
        params.append(start_date)
    if end_date:
        where_period += f" AND {delivered_expr} <= date(?)"
        params.append(end_date)
    rows = conn.execute(
        "SELECT "
        "o.sp_order_no, o.customer_name, o.city, o.tracking_code, "
        "o.created_at, o.picked_up_at, o.delivered_at, o.status, "
        "("
        "COALESCE(SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "), 0) + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0)"
        ") AS sp_expected_amount, "
        "COALESCE(SUM(COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0)), 0) "
        "+ COALESCE(MAX(COALESCE(oi.addon_advance, 0)), 0) AS advance_total "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "LEFT JOIN ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "  FROM order_items "
        "  GROUP BY order_id"
        ") od ON od.order_id = o.id "
        "LEFT JOIN payments p ON trim(p.sp_order_no) = trim(o.sp_order_no) "
        "WHERE o.delivered_at IS NOT NULL AND TRIM(o.delivered_at) != '' "
        "AND o.status IS NOT NULL "
        "AND (o.status LIKE '%Isporu%' OR o.status LIKE '%isporu%') "
        "AND p.id IS NULL "
        "AND (o.status IS NULL OR (lower(o.status) NOT LIKE '%otkazan%' "
        "AND lower(o.status) NOT LIKE '%obradi%' "
        "AND lower(o.status) NOT LIKE '%vraceno%' "
        "AND lower(o.status) NOT LIKE '%vra\u0107eno%')) "
        + where_period
        + " GROUP BY o.id "
        "HAVING sp_expected_amount > 0 "
        "ORDER BY o.delivered_at DESC",
        params,
    ).fetchall()
    cols = [
        "sp_order_no",
        "customer_name",
        "city",
        "tracking_code",
        "created_at",
        "picked_up_at",
        "delivered_at",
        "status",
        "sp_expected_amount",
        "advance_total",
    ]
    return cols, rows


def get_pending_sp_orders_summary(conn: sqlite3.Connection) -> tuple[int, float]:
    row = conn.execute(
        "SELECT COUNT(*) AS orders_cnt, COALESCE(SUM(sp_expected_amount), 0.0) AS sp_sum "
        "FROM ("
        "  SELECT o.id AS order_id, "
        "  ("
        "    COALESCE(SUM("
        "      COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "      * (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "      * (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "    ), 0)"
        "    + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0)"
        "  ) AS sp_expected_amount "
        "  FROM orders o "
        "  LEFT JOIN order_items oi ON oi.order_id = o.id "
        "  LEFT JOIN ("
        "    SELECT order_id, "
        "      MAX(COALESCE(discount, 0)) AS order_discount, "
        "      MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "    FROM order_items "
        "    GROUP BY order_id"
        "  ) od ON od.order_id = o.id "
        "  LEFT JOIN payments p ON trim(p.sp_order_no) = trim(o.sp_order_no) "
        "  WHERE (o.delivered_at IS NULL OR TRIM(o.delivered_at) = '') "
        "  AND o.status IS NOT NULL "
        "  AND (lower(o.status) LIKE '%poslat%' OR lower(o.status) LIKE '%obradi%') "
        "  AND p.id IS NULL "
        "  AND (lower(o.status) NOT LIKE '%otkazan%' "
        "    AND lower(o.status) NOT LIKE '%vraceno%' "
        "    AND lower(o.status) NOT LIKE '%vra\u0107eno%') "
        "  GROUP BY o.id "
        "  HAVING sp_expected_amount > 0"
        ") t"
    ).fetchone()
    if not row:
        return (0, 0.0)
    return (int(row[0] or 0), float(row[1] or 0.0))


def get_pending_sp_orders_details(conn: sqlite3.Connection) -> tuple[list[str], list[tuple]]:
    rows = conn.execute(
        "SELECT "
        "o.sp_order_no, o.customer_name, o.city, o.tracking_code, "
        "o.created_at, o.picked_up_at, o.delivered_at, o.status, "
        "("
        "COALESCE(SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "), 0) + COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0)"
        ") AS sp_expected_amount "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "LEFT JOIN ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod "
        "  FROM order_items "
        "  GROUP BY order_id"
        ") od ON od.order_id = o.id "
        "LEFT JOIN payments p ON trim(p.sp_order_no) = trim(o.sp_order_no) "
        "WHERE (o.delivered_at IS NULL OR TRIM(o.delivered_at) = '') "
        "AND o.status IS NOT NULL "
        "AND (lower(o.status) LIKE '%poslat%' OR lower(o.status) LIKE '%obradi%') "
        "AND p.id IS NULL "
        "AND (lower(o.status) NOT LIKE '%otkazan%' "
        "AND lower(o.status) NOT LIKE '%vraceno%' "
        "AND lower(o.status) NOT LIKE '%vra\u0107eno%') "
        "GROUP BY o.id "
        "HAVING sp_expected_amount > 0 "
        "ORDER BY o.created_at DESC"
    ).fetchall()
    cols = [
        "sp_order_no",
        "customer_name",
        "city",
        "tracking_code",
        "created_at",
        "picked_up_at",
        "delivered_at",
        "status",
        "sp_expected_amount",
    ]
    return cols, rows


def get_neto_breakdown_by_orders(
    conn: sqlite3.Connection,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
) -> tuple[list[str], list[tuple]]:
    date_clause, params = date_filter_clause("o.created_at", days, start, end)
    rows = conn.execute(
        "WITH od AS ("
        "  SELECT order_id, "
        "    MAX(COALESCE(discount, 0)) AS order_discount, "
        "    MAX(COALESCE(addon_cod, 0)) AS addon_cod, "
        "    MAX(COALESCE(addon_advance, 0)) AS addon_advance "
        "  FROM order_items "
        "  GROUP BY order_id"
        ") "
        "SELECT "
        "o.sp_order_no, o.customer_name, o.city, o.status, "
        "o.created_at, o.picked_up_at, o.delivered_at, "
        "COALESCE(SUM(COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0)), 0) AS gross_cod_raw, "
        "COALESCE(od.addon_cod, 0) AS gross_addon_raw, "
        "COALESCE(SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(od.order_discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0)"
        "), 0) AS cod_after_discounts, "
        "COALESCE(od.addon_cod, 0) * (1 - COALESCE(od.order_discount, 0) / 100.0) AS addon_after_discounts, "
        "COALESCE(SUM(COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0)), 0) AS advance_raw, "
        "COALESCE(od.addon_advance, 0) AS advance_addon_raw "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "LEFT JOIN od ON od.order_id = o.id "
        "WHERE o.created_at IS NOT NULL "
        + date_clause
        + " GROUP BY o.id "
        "ORDER BY o.created_at DESC",
        params,
    ).fetchall()
    out_rows = []
    for r in rows:
        (
            sp_order_no,
            customer_name,
            city,
            status,
            created_at,
            picked_up_at,
            delivered_at,
            gross_cod_raw,
            gross_addon_raw,
            cod_after_discounts,
            addon_after_discounts,
            advance_raw,
            advance_addon_raw,
        ) = r
        gross_raw = float(gross_cod_raw or 0.0) + float(gross_addon_raw or 0.0)
        gross_after = float(cod_after_discounts or 0.0) + float(
            addon_after_discounts or 0.0
        )
        advance_total = float(advance_raw or 0.0) + float(advance_addon_raw or 0.0)
        cash_sp = gross_after
        shipment_value = gross_after + advance_total
        included = 1
        st = str(status or "")
        if "Vra\u0107eno" in st or "Vraceno" in st:
            included = 0
        out_rows.append(
            (
                sp_order_no,
                customer_name,
                city,
                status,
                created_at,
                picked_up_at,
                delivered_at,
                gross_raw,
                gross_after,
                advance_total,
                cash_sp,
                shipment_value,
                included,
            )
        )
    cols = [
        "sp_order_no",
        "customer_name",
        "city",
        "status",
        "created_at",
        "picked_up_at",
        "delivered_at",
        "gross_raw",
        "cash_sp_after_discounts",
        "advance_total",
        "shipment_value_calc",
        "cash_sp_calc",
        "included_in_kpis",
    ]
    return cols, out_rows


def report_refund_items_category(
    conn: sqlite3.Connection,
    category: str,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    return_rows: bool = False,
    categorize_sku: Callable[[str], str] | None = None,
):
    totals = build_refund_item_totals(conn, days, start, end)
    rows = []
    if categorize_sku is None:
        raise ValueError("categorize_sku callback is required for refund category report")
    for sku, qty in totals.items():
        cat = str(categorize_sku(str(sku)))
        if cat != category:
            continue
        rows.append((sku, qty, cat))
    rows.sort(key=lambda x: x[1], reverse=True)
    if return_rows:
        return ["sku", "qty_refund", "category"], rows
    return None


def get_refund_top_customers(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    rows = get_refund_rows(conn, days, start, end)
    if not rows:
        return []
    inv_by_number = {}
    inv_by_digits = {}
    for inv_id, inv_no, name, note, basis in conn.execute(
        "SELECT id, number, customer_name, note, basis FROM invoices"
    ).fetchall():
        inv_by_number[str(inv_no or "")] = (int(inv_id), name)
        digits = invoice_digits(inv_no)
        if digits:
            inv_by_digits.setdefault(digits, set()).add((int(inv_id), name))
        for text in (note, basis):
            inv_text, digits_text = extract_invoice_no_from_text(text or "")
            if digits_text:
                inv_by_digits.setdefault(digits_text, set()).add((int(inv_id), name))

    order_by_invoice = {
        int(inv_id): int(order_id)
        for inv_id, order_id in conn.execute(
            "SELECT invoice_id, order_id FROM invoice_matches"
        ).fetchall()
    }
    order_name = {
        int(oid): name
        for oid, name in conn.execute("SELECT id, customer_name FROM orders").fetchall()
    }

    totals = {}
    amounts = {}
    for _, _, amount, payee_name, _, inv_no, inv_digits in rows:
        key = None
        if inv_no and str(inv_no) in inv_by_number:
            inv_id, inv_name = inv_by_number[str(inv_no)]
            order_id = order_by_invoice.get(inv_id)
            key = order_name.get(order_id) or inv_name
        elif (
            inv_digits
            and inv_digits in inv_by_digits
            and len(inv_by_digits[inv_digits]) == 1
        ):
            inv_id, inv_name = next(iter(inv_by_digits[inv_digits]))
            order_id = order_by_invoice.get(inv_id)
            key = order_name.get(order_id) or inv_name
        if not key:
            key = payee_name or "Nepoznato"
        totals[key] = totals.get(key, 0) + 1
        amounts[key] = amounts.get(key, 0.0) + float(amount or 0)
    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return [(name, cnt, amounts.get(name, 0.0)) for name, cnt in ranked[:limit]]


def get_refund_top_items(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
):
    totals = build_refund_item_totals(conn, days, start, end)
    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return ranked[:limit]


def get_refund_top_categories(
    conn: sqlite3.Connection,
    limit: int = 5,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    categorize_sku: Callable[[str], str] | None = None,
):
    totals = build_refund_item_totals(conn, days, start, end)
    if not totals:
        return []
    if categorize_sku is None:
        raise ValueError("categorize_sku callback is required for category totals")
    by_cat = {}
    for sku, qty in totals.items():
        cat = str(categorize_sku(str(sku)))
        by_cat[cat] = by_cat.get(cat, 0.0) + float(qty or 0)
    ranked = sorted(by_cat.items(), key=lambda x: x[1], reverse=True)
    return ranked[:limit]


def get_needs_invoice_orders(conn: sqlite3.Connection, limit: int = 50):
    rows = conn.execute(
        "SELECT o.id, o.sp_order_no, o.customer_name, o.picked_up_at, o.created_at, o.status, "
        "MAX(oi.cod_amount), MIN(oi.cod_amount), SUM(oi.cod_amount), "
        "MAX(oi.addon_cod), MIN(oi.addon_cod), SUM(oi.addon_cod), "
        "MAX(oi.advance_amount), MIN(oi.advance_amount), SUM(oi.advance_amount), "
        "MAX(oi.addon_advance), MIN(oi.addon_advance), SUM(oi.addon_advance) "
        "FROM order_flags f "
        "JOIN orders o ON o.id = f.order_id "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE f.flag = 'needs_invoice' "
        "GROUP BY o.id "
        "ORDER BY o.picked_up_at "
        "LIMIT ?",
        (limit,),
    ).fetchall()

    order_ids = [int(row[0]) for row in rows]
    net_map = build_order_net_map(conn, order_ids)

    inv_rows = conn.execute(
        "SELECT i.id, i.turnover, i.amount_due, "
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
            "date": normalize_date(row[1]),
            "amount": row[2],
            "matched": bool(row[3]),
        }
        invoices.append(inv)
        if inv["date"]:
            invoices_by_date.setdefault(inv["date"], []).append(inv)
        else:
            invoices_no_date.append(inv)

    def amount_exact(a, b) -> bool:
        return amount_exact_strict(a, b)

    def date_in_window(d1, d2, days_back: int = 10, days_forward: int = 10) -> bool:
        if not d1 or not d2:
            return False
        delta = (d2 - d1).days
        return -days_back <= delta <= days_forward

    results = []
    for row in rows:
        order_id = int(row[0])
        sp_no = row[1]
        name = row[2]
        picked_up_at = row[3]
        created_at = row[4]
        status = row[5]
        display_date = picked_up_at or created_at
        if is_cancelled_status(status) or is_in_progress_status(status):
            reason = "Otkazano"
            results.append((sp_no, name, display_date, reason))
            continue
        amount = net_map.get(order_id)
        order_date = normalize_date(display_date)

        if amount is None or abs(amount) < 0.01:
            continue

        if order_date:
            date_candidates = []
            for offset in range(-10, 11):
                date_candidates.extend(
                    invoices_by_date.get(order_date + timedelta(days=offset), [])
                )
            date_candidates.extend(invoices_no_date)
        else:
            date_candidates = invoices

        amount_candidates = [
            inv for inv in date_candidates if amount_exact(amount, inv.get("amount"))
        ]
        unmatched_candidates = [
            inv for inv in amount_candidates if not inv.get("matched")
        ]
        matched_candidates = [inv for inv in amount_candidates if inv.get("matched")]

        recent = False
        if order_date:
            try:
                recent = (date.today() - order_date).days <= 3
            except Exception:
                recent = False

        if not order_date:
            if amount_candidates:
                reason = "Nema datuma (ima iznos poklapanje)"
            else:
                reason = "Nema datuma i nema iznos poklapanja"
        elif recent:
            continue
        elif not date_candidates:
            reason = "Nema racuna u -10/+10 dana"
        elif not amount_candidates:
            reason = "Nema racuna sa iznosom u -10/+10 dana"
        elif unmatched_candidates:
            reason = "Ima kandidata po datumu/iznosu (manual)"
        elif matched_candidates:
            other_orders = [
                matched_map.get(inv["id"])
                for inv in matched_candidates
                if matched_map.get(inv["id"])
            ]
            if other_orders:
                reason = f"Racun uparen sa SP: {', '.join(sorted(set(other_orders)))}"
            else:
                reason = "Racun vec uparen"
        else:
            reason = "Nema kandidata"

        results.append((sp_no, name, display_date, reason))

    return results


def get_unmatched_orders_list(conn: sqlite3.Connection, limit: int = 50):
    cutoff_expr = date_expr("COALESCE(o.picked_up_at, o.created_at)")
    rows = conn.execute(
        "SELECT o.sp_order_no, o.customer_name, o.picked_up_at "
        "FROM orders o "
        "LEFT JOIN order_items oi ON oi.order_id = o.id "
        "WHERE o.id NOT IN (SELECT order_id FROM invoice_matches) "
        "AND o.id NOT IN (SELECT order_id FROM order_flags WHERE flag = 'needs_invoice') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%otkazan%') "
        "AND (o.status IS NULL OR lower(o.status) NOT LIKE '%obradi%') "
        f"AND {cutoff_expr} <= date('now', '-3 day') "
        "GROUP BY o.id "
        "HAVING SUM("
        "COALESCE(oi.qty, 0) * COALESCE(oi.cod_amount, 0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "* (1 - COALESCE(oi.extra_discount, 0) / 100.0) "
        "+ COALESCE(oi.qty, 0) * COALESCE(oi.addon_cod, 0) "
        "* (1 - COALESCE(oi.discount, 0) / 100.0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.advance_amount, 0) "
        "- COALESCE(oi.qty, 0) * COALESCE(oi.addon_advance, 0)"
        ") != 0 "
        "ORDER BY o.picked_up_at "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    return rows
