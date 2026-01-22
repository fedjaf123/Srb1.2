from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .import_common import append_reject, start_import


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def import_kartice_events_csv(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    file_hash: Callable[[Path], str],
) -> None:
    df = pd.read_csv(path)
    import_id = start_import(conn, "Kartice-Events-CSV", path, len(df), file_hash=file_hash)
    if import_id is None:
        append_reject(
            rejects,
            "Kartice-Events-CSV",
            path.name,
            None,
            "file_already_imported",
            "",
        )
        return

    required = [
        "SKU",
        "Artikal",
        "Datum",
        "Broj",
        "Tip",
        "Smer",
        "Opis",
        "Referenca",
        "Prijem kolicina",
        "Prijem vrednost",
        "Izdavanje kolicina",
        "Izdavanje vrednost",
        "Cena",
        "Stanje zaliha kolicina",
        "Stanje zaliha vrednost",
        "Delta kolicina",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        conn.execute("DELETE FROM import_runs WHERE id = ?", (import_id,))
        conn.commit()
        append_reject(
            rejects,
            "Kartice-Events-CSV",
            path.name,
            None,
            "missing_columns",
            ",".join(missing),
        )
        return

    src = path.name
    for idx, row in df.iterrows():
        sku = _norm_text(row.get("SKU"))
        if not sku:
            continue
        event_date = _norm_text(row.get("Datum"))
        broj = _norm_text(row.get("Broj"))
        tip = _norm_text(row.get("Tip"))
        smer = _norm_text(row.get("Smer"))
        referenca = _norm_text(row.get("Referenca"))
        ref_key = referenca or ""

        event_key = _sha1("|".join([sku, event_date, broj, tip, smer, ref_key]))

        def _to_float(v: Any):
            try:
                if v is None or v == "":
                    return None
                return float(v)
            except Exception:
                return None

        values = (
            event_key,
            sku,
            _norm_text(row.get("Artikal")) or None,
            event_date,
            broj or None,
            tip or None,
            smer or None,
            _norm_text(row.get("Opis")) or None,
            referenca or None,
            ref_key,
            _to_float(row.get("Prijem kolicina", None)),
            _to_float(row.get("Prijem vrednost", None)),
            _to_float(row.get("Izdavanje kolicina", None)),
            _to_float(row.get("Izdavanje vrednost", None)),
            _to_float(row.get("Cena", None)),
            _to_float(row.get("Stanje zaliha kolicina", None)),
            _to_float(row.get("Stanje zaliha vrednost", None)),
            _to_float(row.get("Delta kolicina", None)),
            src,
            import_id,
        )
        try:
            conn.execute(
                "INSERT INTO kartice_events ("
                "event_key, sku, item_name, event_date, broj, tip, smer, opis, referenca, ref_key, "
                "prijem_qty, prijem_value, izdavanje_qty, izdavanje_value, cena, stanje_qty, stanje_value, "
                "delta_qty, source_file, import_run_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(event_key) DO UPDATE SET "
                "item_name=excluded.item_name, broj=excluded.broj, tip=excluded.tip, smer=excluded.smer, "
                "opis=excluded.opis, referenca=excluded.referenca, ref_key=excluded.ref_key, "
                "prijem_qty=excluded.prijem_qty, prijem_value=excluded.prijem_value, "
                "izdavanje_qty=excluded.izdavanje_qty, izdavanje_value=excluded.izdavanje_value, "
                "cena=excluded.cena, stanje_qty=excluded.stanje_qty, stanje_value=excluded.stanje_value, "
                "delta_qty=excluded.delta_qty, source_file=excluded.source_file, import_run_id=excluded.import_run_id",
                values,
            )
        except Exception as exc:
            append_reject(
                rejects,
                "Kartice-Events-CSV",
                path.name,
                int(idx) + 1,
                "row_failed",
                f"{exc}",
            )

    conn.commit()
