import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from srb_modules.db import (
    connect_db,
    file_hash,
    set_app_state,
    set_task_progress,
    update_task_progress,
)
from srb_modules.import_kartice_events import import_kartice_events_csv
from srb_modules.import_sp_prijemi import import_sp_prijemi_folder


def _ensure_task_progress_table(db_path: Path) -> None:
    conn = connect_db(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS import_runs ("
            "id INTEGER PRIMARY KEY, "
            "source TEXT NOT NULL, "
            "filename TEXT NOT NULL, "
            "file_hash TEXT NOT NULL, "
            "imported_at TEXT NOT NULL, "
            "row_count INTEGER NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_import_runs_file_hash "
            "ON import_runs(file_hash)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS app_state ("
            "key TEXT PRIMARY KEY, "
            "value TEXT"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS task_progress ("
            "task TEXT PRIMARY KEY, "
            "total INTEGER NOT NULL, "
            "processed INTEGER NOT NULL, "
            "updated_at TEXT NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sp_prijemi_receipts ("
            "receipt_key TEXT PRIMARY KEY, "
            "client_code TEXT, "
            "created_at TEXT, "
            "verified_at TEXT, "
            "status TEXT, "
            "latest_file_hash TEXT, "
            "latest_file_name TEXT, "
            "import_run_id INTEGER, "
            "updated_at TEXT NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sp_prijemi_lines ("
            "id INTEGER PRIMARY KEY, "
            "receipt_key TEXT NOT NULL, "
            "sku TEXT NOT NULL, "
            "product_name TEXT, "
            "sent_qty REAL, "
            "arrived_qty REAL, "
            "import_run_id INTEGER"
            ")"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_sp_prijemi_lines_unique "
            "ON sp_prijemi_lines(receipt_key, sku)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kartice_events ("
            "id INTEGER PRIMARY KEY, "
            "event_key TEXT NOT NULL, "
            "sku TEXT NOT NULL, "
            "item_name TEXT, "
            "event_date TEXT NOT NULL, "
            "broj TEXT, "
            "tip TEXT, "
            "smer TEXT, "
            "opis TEXT, "
            "referenca TEXT, "
            "ref_key TEXT NOT NULL, "
            "prijem_qty REAL, "
            "prijem_value REAL, "
            "izdavanje_qty REAL, "
            "izdavanje_value REAL, "
            "cena REAL, "
            "stanje_qty REAL, "
            "stanje_value REAL, "
            "delta_qty REAL, "
            "source_file TEXT, "
            "import_run_id INTEGER"
            ")"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_kartice_events_event_key "
            "ON kartice_events(event_key)"
        )
        conn.commit()
    finally:
        conn.close()


def _latest_file(root: Path, pattern: str) -> Path | None:
    candidates = list(root.rglob(pattern)) if root.exists() else []
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _make_run_dir(parent: Path) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    base = datetime.now().strftime("%d-%m-%Y")
    candidate = parent / base
    if not candidate.exists():
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate
    for i in range(2, 1000):
        alt = parent / f"{base}_{i}"
        if not alt.exists():
            alt.mkdir(parents=True, exist_ok=True)
            return alt
    # Fallback (very unlikely)
    alt = parent / datetime.now().strftime("%d-%m-%Y_%H%M%S")
    alt.mkdir(parents=True, exist_ok=True)
    return alt


def run_regenerate_sku_metrics_process(
    pdf_root: str,
    prijemi_root: str,
    out_dir: str,
    sp_db_path: str,
    progress_db_path: str,
    task_name: str = "regen_metrics",
) -> None:
    pdf_root_p = Path(pdf_root)
    prijemi_root_p = Path(prijemi_root)
    out_dir_p = Path(out_dir)
    sp_db_p = Path(sp_db_path)
    progress_db_p = Path(progress_db_path)

    _ensure_task_progress_table(progress_db_p)
    conn = connect_db(progress_db_p)
    try:
        set_task_progress(conn, task_name, total=5)
        update_task_progress(conn, task_name, 0)

        pdf_path = _latest_file(pdf_root_p, "*.pdf")
        if pdf_path is None:
            legacy_root = Path("Kalkulacije_kartice_art")
            pdf_path = _latest_file(legacy_root, "*.pdf")
        if pdf_path is None:
            pdf_path = _latest_file(Path.cwd(), "*.pdf")
        if pdf_path is None:
            raise RuntimeError(
                f"Nema PDF kartice u folderu '{pdf_root_p}' (ni u root folderu)."
            )
        if not prijemi_root_p.exists():
            raise RuntimeError(f"Nema foldera za SP Prijemi: '{prijemi_root_p}'.")
        if not sp_db_p.exists():
            raise RuntimeError(f"Nema baze: '{sp_db_p}'.")

        extract_script = Path(__file__).resolve().parent.parent / "extract_kalkulacije_kartice.py"
        build_script = Path(__file__).resolve().parent.parent / "build_sku_daily_metrics.py"
        out_dir_p.mkdir(parents=True, exist_ok=True)
        run_dir = _make_run_dir(out_dir_p)

        update_task_progress(conn, task_name, 1)
        cmd1 = [
            sys.executable,
            str(extract_script),
            "--skip-excel",
            "--pdf",
            str(pdf_path),
            "--prijemi",
            str(prijemi_root_p),
            "--out",
            str(run_dir),
        ]
        r1 = subprocess.run(cmd1, capture_output=True, text=True, check=False)
        if r1.returncode != 0:
            raise RuntimeError(
                f"extract_kalkulacije_kartice.py greska:\n{r1.stderr or r1.stdout}"
            )

        try:
            set_app_state(conn, "kartice_pdf_name", pdf_path.name)
            set_app_state(conn, "kartice_pdf_hash", file_hash(pdf_path))
            meta_path = run_dir / "kartice_meta.json"
            if meta_path.exists():
                import json

                data = json.loads(meta_path.read_text(encoding="utf-8"))
                if data.get("range_start_iso"):
                    set_app_state(conn, "kartice_range_start", str(data["range_start_iso"]))
                if data.get("range_end_iso"):
                    set_app_state(conn, "kartice_range_end", str(data["range_end_iso"]))
                if data.get("final_end_iso"):
                    set_app_state(conn, "kartice_final_end", str(data["final_end_iso"]))
        except Exception:
            pass

        update_task_progress(conn, task_name, 2)
        import_sp_prijemi_folder(conn, prijemi_root_p, rejects=None, file_hash=file_hash)

        update_task_progress(conn, task_name, 3)
        events_csv = run_dir / "kartice_events.csv"
        if events_csv.exists():
            import_kartice_events_csv(conn, events_csv, rejects=None, file_hash=file_hash)

        update_task_progress(conn, task_name, 4)
        cmd2 = [
            sys.executable,
            str(build_script),
            "--events",
            str(run_dir / "kartice_events.csv"),
            "--receipts-summary",
            str(run_dir / "sp_prijemi_summary.csv"),
            "--db",
            str(sp_db_p),
            "--out",
            str(run_dir),
        ]
        r2 = subprocess.run(cmd2, capture_output=True, text=True, check=False)
        if r2.returncode != 0:
            raise RuntimeError(
                f"build_sku_daily_metrics.py greska:\n{r2.stderr or r2.stdout}"
            )

        # Copy outputs to root out_dir as "latest" so UI stays stable, while run_dir keeps history.
        try:
            for name in [
                "kartice_events.csv",
                "kartice_sku_summary.csv",
                "kartice_zero_intervali.csv",
                "kartice_meta.json",
                "sp_prijemi_detail.csv",
                "sp_prijemi_summary.csv",
                "sku_daily_metrics.csv",
                "sku_controls_audit.csv",
                "sku_promo_periods.csv",
            ]:
                src = run_dir / name
                if src.exists():
                    shutil.copy2(src, out_dir_p / name)
        except Exception:
            pass

        update_task_progress(conn, task_name, 5)
    finally:
        conn.close()
