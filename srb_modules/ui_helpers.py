from __future__ import annotations

from datetime import date, datetime
from typing import Callable


def parse_user_date(text: str) -> date | None:
    value = (text or "").strip()
    if not value:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def format_user_date(value: date) -> str:
    return value.strftime("%d-%m-%Y")


def add_calendar_picker(
    app,
    ctk,
    tk,
    Calendar,
    parent,
    var,
    parse_user_date: Callable[[str], date | None],
    format_user_date: Callable[[date], str],
    width: int = 120,
):
    row = ctk.CTkFrame(parent, fg_color="transparent")
    entry = ctk.CTkEntry(row, width=width, textvariable=var)
    entry.pack(side="left", padx=(0, 6))

    def open_calendar():
        top = tk.Toplevel(app)
        top.title("Odabir datuma")
        try:
            top.transient(app)
            top.grab_set()
        except Exception:
            pass

        current = parse_user_date(var.get())
        cal = Calendar(
            top,
            selectmode="day",
            date_pattern="dd-mm-y",
            showweeknumbers=False,
        )
        if current is not None:
            try:
                cal.selection_set(current)
                cal.see(current)
            except Exception:
                pass
        cal.pack(padx=10, pady=10)

        btns = tk.Frame(top)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        def on_ok():
            try:
                picked = cal.selection_get()
                var.set(format_user_date(picked))
            except Exception:
                pass
            top.destroy()

        def on_clear():
            var.set("")
            top.destroy()

        tk.Button(btns, text="OK", command=on_ok, width=10).pack(side="left", padx=4)
        tk.Button(btns, text="Ocisti", command=on_clear, width=10).pack(
            side="left", padx=4
        )
        tk.Button(btns, text="Cancel", command=top.destroy, width=10).pack(
            side="right", padx=4
        )

    ctk.CTkButton(row, text="Odaberi", width=90, command=open_calendar).pack(side="left")
    return row, entry


def pick_date_range_dialog(
    app,
    tk,
    Calendar,
    title: str,
    initial_start: date | None,
    initial_end: date | None,
    format_user_date: Callable[[date], str],
) -> tuple[date | None, date | None]:
    result = {"start": initial_start, "end": initial_end}

    top = tk.Toplevel(app)
    top.title(title)
    try:
        top.transient(app)
        top.grab_set()
    except Exception:
        pass

    info = tk.Label(
        top,
        text="Klikni prvo pocetni datum, pa zatim krajnji datum.\n"
        "Možeš mijenjati mjesec/godinu gore na strelicama.",
        justify="left",
    )
    info.pack(anchor="w", padx=10, pady=(10, 4))

    status = tk.Label(top, text="", justify="left")
    status.pack(anchor="w", padx=10, pady=(0, 6))

    cal = Calendar(
        top,
        selectmode="day",
        date_pattern="dd-mm-y",
        showweeknumbers=False,
    )
    if initial_start is not None:
        try:
            cal.selection_set(initial_start)
            cal.see(initial_start)
        except Exception:
            pass
    cal.pack(padx=10, pady=10)

    click_state = {"phase": "start"}

    def update_status():
        s = result["start"]
        e = result["end"]
        s_txt = format_user_date(s) if s else "-"
        e_txt = format_user_date(e) if e else "-"
        status.configure(text=f"Od: {s_txt}    Do: {e_txt}")

    def on_select(_evt=None):
        picked = None
        try:
            picked = cal.selection_get()
        except Exception:
            picked = None
        if picked is None:
            return
        if click_state["phase"] == "start":
            result["start"] = picked
            result["end"] = None
            click_state["phase"] = "end"
        else:
            result["end"] = picked
            click_state["phase"] = "start"
            if result["start"] and result["end"] and result["end"] < result["start"]:
                result["start"], result["end"] = result["end"], result["start"]
        update_status()

    cal.bind("<<CalendarSelected>>", on_select)
    update_status()

    btns = tk.Frame(top)
    btns.pack(fill="x", padx=10, pady=(0, 10))

    def on_ok():
        top.destroy()

    def on_reset():
        result["start"] = None
        result["end"] = None
        click_state["phase"] = "start"
        update_status()

    def on_cancel():
        result["start"] = None
        result["end"] = None
        top.destroy()

    tk.Button(btns, text="OK", command=on_ok, width=10).pack(side="left", padx=4)
    tk.Button(btns, text="Reset", command=on_reset, width=10).pack(
        side="left", padx=4
    )
    tk.Button(btns, text="Cancel", command=on_cancel, width=10).pack(
        side="right", padx=4
    )

    app.wait_window(top)
    return result["start"], result["end"]
