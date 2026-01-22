from __future__ import annotations

from pathlib import Path
from typing import Any


def format_missing_int_ranges(expected_start: int, present: set[int], expected_end: int) -> str:
    if expected_end < expected_start:
        return ""
    missing = [n for n in range(expected_start, expected_end + 1) if n not in present]
    if not missing:
        return ""
    ranges: list[tuple[int, int]] = []
    cur_start = missing[0]
    cur_end = missing[0]
    for n in missing[1:]:
        if n == cur_end + 1:
            cur_end = n
        else:
            ranges.append((cur_start, cur_end))
            cur_start = n
            cur_end = n
    ranges.append((cur_start, cur_end))
    parts = []
    for a, b in ranges:
        parts.append(str(a) if a == b else f"{a}-{b}")
    return ", ".join(parts)


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


def start_import(conn: Any, source: str, path: Path, row_count: int, *, file_hash) -> int | None:
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
