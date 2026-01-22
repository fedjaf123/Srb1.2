import hashlib
import sqlite3
from pathlib import Path


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def set_task_progress(conn: sqlite3.Connection, task: str, total: int) -> None:
    conn.execute(
        "INSERT INTO task_progress (task, total, processed, updated_at) "
        "VALUES (?, ?, 0, datetime('now')) "
        "ON CONFLICT(task) DO UPDATE SET total = excluded.total, "
        "processed = excluded.processed, updated_at = excluded.updated_at",
        (task, total),
    )
    conn.commit()


def update_task_progress(conn: sqlite3.Connection, task: str, processed: int) -> None:
    conn.execute(
        "UPDATE task_progress SET processed = ?, updated_at = datetime('now') "
        "WHERE task = ?",
        (processed, task),
    )
    conn.commit()


def get_task_progress(conn: sqlite3.Connection, task: str):
    row = conn.execute(
        "SELECT total, processed, updated_at FROM task_progress WHERE task = ?",
        (task,),
    ).fetchone()
    if not row:
        return None
    return {"total": int(row[0]), "processed": int(row[1]), "updated_at": row[2]}


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def set_app_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO app_state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


def get_app_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM app_state WHERE key = ?",
        (key,),
    ).fetchone()
    if not row:
        return None
    return row[0]


def ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if any(row[1] == column for row in cols):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def init_db(conn: sqlite3.Connection, schema_sql: str) -> None:
    conn.executescript(schema_sql)
    ensure_column(conn, "invoice_candidates", "detail", "TEXT")
    ensure_column(conn, "orders", "customer_key", "TEXT")
    ensure_column(conn, "minimax_items", "updated_at", "TEXT")
    ensure_column(conn, "tracking_summary", "last_status", "TEXT")
    ensure_column(conn, "tracking_summary", "last_status_at", "TEXT")
    conn.commit()
