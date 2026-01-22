from __future__ import annotations

import hashlib
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .import_common import append_reject, start_import


def _canon_col(name: Any) -> str:
    text = str(name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch for ch in text if ch.isalnum() or ch.isspace()).strip()


def _parse_dt(value: Any) -> str | None:
    if value is None or value == "":
        return None
    ts = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    if hasattr(ts, "to_pydatetime"):
        dt = ts.to_pydatetime()
    else:
        dt = ts
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def _receipt_key(client_code: str | None, created_at: str | None) -> str:
    base = f"{(client_code or '').strip()}|{(created_at or '').strip()}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def import_sp_prijem(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    file_hash: Callable[[Path], str],
) -> None:
    df = pd.read_excel(path, engine="openpyxl")
    colmap = {_canon_col(c): c for c in df.columns}

    def col(key: str) -> Any:
        return colmap.get(_canon_col(key))

    required = {
        "client_code": col("Šifra klijenta"),
        "sku": col("Šifra proizvoda"),
        "product_name": col("Ime proizvoda"),
        "sent_qty": col("Poslata količina"),
        "arrived_qty": col("Pristigla količina"),
        "created_at": col("Datum dodavanja"),
        "verified_at": col("Datum verifikacije"),
        "status": col("Status"),
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        append_reject(
            rejects,
            "SP-Prijemi",
            path.name,
            None,
            "missing_columns",
            ",".join(missing),
        )
        return

    import_id = start_import(conn, "SP-Prijemi", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "SP-Prijemi", path.name, None, "file_already_imported", "")
        return

    try:
        client_vals = (
            df[required["client_code"]].dropna().astype(str).map(str.strip).unique().tolist()
        )
        created_vals = df[required["created_at"]].dropna().unique().tolist()
        verified_vals = df[required["verified_at"]].dropna().unique().tolist()
        status_vals = (
            df[required["status"]].dropna().astype(str).map(str.strip).unique().tolist()
        )

        client_code = client_vals[0] if client_vals else None
        created_at = _parse_dt(created_vals[0] if created_vals else None)
        verified_at = _parse_dt(verified_vals[0] if verified_vals else None)
        status = status_vals[0] if status_vals else None

        receipt_key = _receipt_key(client_code, created_at or verified_at)
        latest_digest = file_hash(path)

        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO sp_prijemi_receipts "
            "(receipt_key, client_code, created_at, verified_at, status, "
            "latest_file_hash, latest_file_name, import_run_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(receipt_key) DO UPDATE SET "
            "client_code=excluded.client_code, created_at=excluded.created_at, "
            "verified_at=excluded.verified_at, status=excluded.status, "
            "latest_file_hash=excluded.latest_file_hash, latest_file_name=excluded.latest_file_name, "
            "import_run_id=excluded.import_run_id, updated_at=excluded.updated_at",
            (
                receipt_key,
                client_code,
                created_at,
                verified_at,
                status,
                latest_digest,
                path.name,
                import_id,
            ),
        )

        conn.execute("DELETE FROM sp_prijemi_lines WHERE receipt_key = ?", (receipt_key,))

        for idx, row in df.iterrows():
            sku = str(row.get(required["sku"], "")).strip()
            if not sku:
                continue
            product_name = str(row.get(required["product_name"], "")).strip() or None
            sent_qty = row.get(required["sent_qty"], None)
            arrived_qty = row.get(required["arrived_qty"], None)
            try:
                sent_qty = float(sent_qty) if sent_qty not in (None, "") else None
            except Exception:
                sent_qty = None
            try:
                arrived_qty = float(arrived_qty) if arrived_qty not in (None, "") else None
            except Exception:
                arrived_qty = None

            conn.execute(
                "INSERT INTO sp_prijemi_lines "
                "(receipt_key, sku, product_name, sent_qty, arrived_qty, import_run_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (receipt_key, sku, product_name, sent_qty, arrived_qty, import_id),
            )

        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.execute("DELETE FROM import_runs WHERE id = ?", (import_id,))
        conn.commit()
        append_reject(
            rejects,
            "SP-Prijemi",
            path.name,
            None,
            "import_failed",
            str(exc),
        )
        raise


def import_sp_prijemi_folder(
    conn: sqlite3.Connection,
    root: Path,
    rejects: list | None = None,
    *,
    file_hash: Callable[[Path], str],
) -> None:
    paths = [p for p in root.rglob("*.xlsx") if p.is_file()] if root.exists() else []
    paths.sort(key=lambda p: p.stat().st_mtime)
    for path in paths:
        import_sp_prijem(conn, path, rejects, file_hash=file_hash)
