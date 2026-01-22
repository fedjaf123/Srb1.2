from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ResetSpec:
    key: str
    label: str


RESET_SPECS: list[ResetSpec] = [
    ResetSpec("all_imports", "Svi izvori (samo uvozi)"),
    ResetSpec("sp_orders", "SP Narudžbe"),
    ResetSpec("sp_payments", "SP Uplate"),
    ResetSpec("sp_returns", "SP Preuzimanja"),
    ResetSpec("sp_prijemi", "SP Prijemi"),
    ResetSpec("kartice_events", "Kartice artikala"),
    ResetSpec("minimax", "Minimax"),
    ResetSpec("minimax_items", "Minimax artikli"),
    ResetSpec("bank", "Banka XML"),
]


def _run_ids(conn: sqlite3.Connection, sources: Iterable[str]) -> list[int]:
    sources = list(sources)
    if not sources:
        return []
    placeholders = ",".join("?" for _ in sources)
    rows = conn.execute(
        f"SELECT id FROM import_runs WHERE source IN ({placeholders})",
        tuple(sources),
    ).fetchall()
    return [int(r[0]) for r in rows]


def _delete_import_runs(conn: sqlite3.Connection, run_ids: list[int]) -> None:
    if not run_ids:
        return
    placeholders = ",".join("?" for _ in run_ids)
    conn.execute(f"DELETE FROM import_runs WHERE id IN ({placeholders})", run_ids)


def _delete_app_state_keys(conn: sqlite3.Connection, keys: Iterable[str]) -> None:
    keys = list(keys)
    if not keys:
        return
    placeholders = ",".join("?" for _ in keys)
    conn.execute(f"DELETE FROM app_state WHERE key IN ({placeholders})", keys)


def reset_source(conn: sqlite3.Connection, key: str) -> int:
    """
    Returns number of deleted import_runs.
    Never deletes the DB file; only deletes imported rows per source.
    """
    if key == "all_imports":
        total = 0
        for spec in RESET_SPECS:
            if spec.key == "all_imports":
                continue
            total += reset_source(conn, spec.key)
        return total

    if key == "sp_orders":
        run_ids = _run_ids(conn, ["SP-Narudzbe"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        order_ids = [
            int(r[0])
            for r in conn.execute(
                f"SELECT id FROM orders WHERE import_run_id IN ({placeholders})",
                run_ids,
            ).fetchall()
        ]
        if order_ids:
            ph = ",".join("?" for _ in order_ids)
            conn.execute(f"DELETE FROM invoice_matches WHERE order_id IN ({ph})", order_ids)
            conn.execute(f"DELETE FROM invoice_candidates WHERE order_id IN ({ph})", order_ids)
            conn.execute(f"DELETE FROM order_flags WHERE order_id IN ({ph})", order_ids)
            conn.execute(f"DELETE FROM order_status_history WHERE order_id IN ({ph})", order_ids)
            conn.execute(f"DELETE FROM order_items WHERE order_id IN ({ph})", order_ids)
            conn.execute(f"DELETE FROM orders WHERE id IN ({ph})", order_ids)
        _delete_import_runs(conn, run_ids)
        _delete_app_state_keys(conn, ["last_sp_order_no"])
        conn.commit()
        return len(run_ids)

    if key == "sp_payments":
        run_ids = _run_ids(conn, ["SP-Uplate"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        conn.execute(f"DELETE FROM payments WHERE import_run_id IN ({placeholders})", run_ids)
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "sp_returns":
        run_ids = _run_ids(conn, ["SP-Preuzimanja"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        conn.execute(f"DELETE FROM returns WHERE import_run_id IN ({placeholders})", run_ids)
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "sp_prijemi":
        run_ids = _run_ids(conn, ["SP-Prijemi"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        conn.execute(f"DELETE FROM sp_prijemi_lines WHERE import_run_id IN ({placeholders})", run_ids)
        conn.execute(
            f"DELETE FROM sp_prijemi_receipts WHERE import_run_id IN ({placeholders})",
            run_ids,
        )
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "kartice_events":
        run_ids = _run_ids(conn, ["Kartice-Events-CSV"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        conn.execute(f"DELETE FROM kartice_events WHERE import_run_id IN ({placeholders})", run_ids)
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "minimax":
        run_ids = _run_ids(conn, ["Minimax"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        invoice_ids = [
            int(r[0])
            for r in conn.execute(
                f"SELECT id FROM invoices WHERE import_run_id IN ({placeholders})",
                run_ids,
            ).fetchall()
        ]
        if invoice_ids:
            ph = ",".join("?" for _ in invoice_ids)
            conn.execute(f"DELETE FROM invoice_matches WHERE invoice_id IN ({ph})", invoice_ids)
            conn.execute(f"DELETE FROM invoice_candidates WHERE invoice_id IN ({ph})", invoice_ids)
            conn.execute(
                f"DELETE FROM invoice_storno WHERE storno_invoice_id IN ({ph})",
                invoice_ids,
            )
            conn.execute(
                f"DELETE FROM invoice_storno WHERE original_invoice_id IN ({ph})",
                invoice_ids,
            )
            conn.execute(
                f"DELETE FROM bank_matches WHERE match_type='storno' AND ref_id IN ({ph})",
                invoice_ids,
            )
            conn.execute(f"DELETE FROM invoices WHERE id IN ({ph})", invoice_ids)
            conn.execute("DELETE FROM action_log WHERE ref_type = 'invoice_match'")
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "minimax_items":
        run_ids = _run_ids(conn, ["Minimax-Items"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        conn.execute(f"DELETE FROM minimax_items WHERE import_run_id IN ({placeholders})", run_ids)
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    if key == "bank":
        run_ids = _run_ids(conn, ["Bank-XML"])
        if not run_ids:
            return 0
        placeholders = ",".join("?" for _ in run_ids)
        txn_ids = [
            int(r[0])
            for r in conn.execute(
                f"SELECT id FROM bank_transactions WHERE import_run_id IN ({placeholders})",
                run_ids,
            ).fetchall()
        ]
        if txn_ids:
            ph = ",".join("?" for _ in txn_ids)
            conn.execute(f"DELETE FROM bank_refunds WHERE bank_txn_id IN ({ph})", txn_ids)
            conn.execute(f"DELETE FROM bank_matches WHERE bank_txn_id IN ({ph})", txn_ids)
            conn.execute(f"DELETE FROM bank_transactions WHERE id IN ({ph})", txn_ids)
        _delete_import_runs(conn, run_ids)
        conn.commit()
        return len(run_ids)

    raise ValueError(f"Nepoznat source key: {key}")
