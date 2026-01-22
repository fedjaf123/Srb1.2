from __future__ import annotations

import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable

from .import_common import append_reject, start_import


def _get_text(elem, path: str) -> str:
    if elem is None:
        return ""
    child = elem.find(path)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def import_bank_xml(
    conn: sqlite3.Connection,
    path: Path,
    rejects: list | None = None,
    *,
    file_hash: Callable[[Path], str],
    to_float: Callable[[object], float | None],
) -> None:
    tree = ET.parse(path)
    root = tree.getroot()

    rows = []
    for stmtrs in root.findall(".//stmtrs"):
        stmt_number = _get_text(stmtrs, "stmtnumber")
        for trn in stmtrs.findall(".//stmttrn"):
            rows.append(
                (
                    _get_text(trn, "fitid"),
                    stmt_number,
                    _get_text(trn, "benefit"),
                    _get_text(trn, "dtposted"),
                    to_float(_get_text(trn, "trnamt")),
                    _get_text(trn, "purpose"),
                    _get_text(trn, "purposecode"),
                    _get_text(trn, "payeeinfo/name"),
                    _get_text(trn, "payeeinfo/city"),
                    _get_text(trn, "payeeaccountinfo/acctid"),
                    _get_text(trn, "payeeaccountinfo/bankid"),
                    _get_text(trn, "payeeaccountinfo/bankname"),
                    _get_text(trn, "refnumber"),
                    _get_text(trn, "payeerefnumber"),
                    _get_text(trn, "urgency"),
                    to_float(_get_text(trn, "fee")),
                )
            )

    import_id = start_import(conn, "Bank-XML", path, len(rows), file_hash=file_hash)
    if import_id is None:
        append_reject(rejects, "Bank-XML", path.name, None, "file_already_imported", "")
        return

    for idx, row in enumerate(rows):
        cur = conn.execute(
            "INSERT OR IGNORE INTO bank_transactions ("
            "fitid, stmt_number, benefit, dtposted, amount, purpose, purposecode, "
            "payee_name, payee_city, payee_acctid, payee_bankid, payee_bankname, "
            "refnumber, payeerefnumber, urgency, fee, import_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*row, import_id),
        )
        if cur.rowcount == 0:
            append_reject(
                rejects,
                "Bank-XML",
                path.name,
                int(idx) + 1,
                "bank_txn_duplicate",
                f"fitid={row[0]}",
            )
    conn.commit()

