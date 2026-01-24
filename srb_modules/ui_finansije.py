from __future__ import annotations

from datetime import date
from typing import Any, Callable

from .ui_context import UIContext


def build_finansije_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_finansije: Any,
    Figure: Any,
    FigureCanvasTkAgg: Any,
    messagebox: Any,
    unlock_finansije: Callable[[], bool],
    pick_date_range_dialog: Callable[
        [str, date | None, date | None], tuple[date | None, date | None]
    ],
    format_user_date: Callable[[date], str],
    refresh_finansije: Callable[[], None],
    export_unpaid_sp_orders: Callable[[], None],
    export_pending_sp_orders: Callable[[], None],
    export_neto_breakdown: Callable[[], None],
) -> dict[str, Any]:
    frame = ctk.CTkFrame(tab_finansije)
    frame.pack(fill="both", expand=True, padx=12, pady=12)

    lock_overlay = ctk.CTkFrame(frame)
    lock_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
    ctk.CTkLabel(
        lock_overlay,
        text="Finansije su zaključane.\nOtključaj lozinkom (ista kao za Reset podataka).",
        justify="center",
        font=ctk.CTkFont(size=18, weight="bold"),
    ).pack(pady=(40, 12))

    def _try_unlock():
        ok = False
        try:
            ok = bool(unlock_finansije())
        except Exception:
            ok = False
        if ok:
            try:
                lock_overlay.place_forget()
            except Exception:
                pass
            refresh_finansije()
        else:
            messagebox.showerror("Greška", "Finansije ostaju zaključane.")

    ctk.CTkButton(lock_overlay, text="Otključaj Finansije", command=_try_unlock).pack(
        pady=(0, 20)
    )
    try:
        lock_overlay.lift()
    except Exception:
        pass

    controls = ctk.CTkFrame(frame)
    controls.pack(fill="x", padx=6, pady=(6, 6))

    def _fmt_range(s: date | None, e: date | None) -> str:
        if not s and not e:
            return "Svo vrijeme"
        if s and e:
            return f"{format_user_date(s)} – {format_user_date(e)}"
        if e:
            return f"do {format_user_date(e)}"
        return f"od {format_user_date(s)}"

    period_label_var = ctk.StringVar(value=_fmt_range(None, None))
    compare_label_var = ctk.StringVar(value="-")
    quick_period_var = ctk.StringVar(value="12 mjeseci")

    def _set_quick_period(choice: str) -> None:
        mapping = {
            "Svo vrijeme": None,
            "1 mjesec": 30,
            "3 mjeseca": 90,
            "6 mjeseci": 180,
            "12 mjeseci": 360,
            "24 mjeseca": 720,
        }
        if choice == "Custom":
            return
        ctx.state["fin_period_custom"] = False
        ctx.state["fin_period_start"] = None
        ctx.state["fin_period_end"] = None
        ctx.state["fin_period_days"] = mapping.get(choice)
        period_label_var.set(choice)
        refresh_finansije()

    def pick_main_period():
        start = ctx.state.get("fin_period_start")
        end = ctx.state.get("fin_period_end")
        start, end = pick_date_range_dialog("Finansije: period", start, end)
        ctx.state["fin_period_start"] = start
        ctx.state["fin_period_end"] = end
        ctx.state["fin_period_custom"] = True
        period_label_var.set(_fmt_range(start, end))
        try:
            quick_period_var.set("Custom")
        except Exception:
            pass
        refresh_finansije()

    def pick_compare_period():
        start = ctx.state.get("fin_compare_start")
        end = ctx.state.get("fin_compare_end")
        start, end = pick_date_range_dialog("Finansije: poređenje", start, end)
        ctx.state["fin_compare_start"] = start
        ctx.state["fin_compare_end"] = end
        ctx.state["fin_compare_custom"] = True
        compare_label_var.set(_fmt_range(start, end) if (start or end) else "-")
        refresh_finansije()

    def clear_compare():
        ctx.state["fin_compare_start"] = None
        ctx.state["fin_compare_end"] = None
        ctx.state["fin_compare_custom"] = False
        compare_label_var.set("-")
        refresh_finansije()

    ctk.CTkButton(controls, text="Odaberi period", command=pick_main_period).pack(
        side="left", padx=6
    )
    ctk.CTkOptionMenu(
        controls,
        variable=quick_period_var,
        values=[
            "Custom",
            "Svo vrijeme",
            "1 mjesec",
            "3 mjeseca",
            "6 mjeseci",
            "12 mjeseci",
            "24 mjeseca",
        ],
        command=_set_quick_period,
        width=130,
    ).pack(side="left", padx=(6, 6))
    ctk.CTkLabel(controls, textvariable=period_label_var).pack(side="left", padx=10)

    ctk.CTkButton(controls, text="Poredi period", command=pick_compare_period).pack(
        side="left", padx=(24, 6)
    )
    ctk.CTkLabel(controls, textvariable=compare_label_var).pack(side="left", padx=10)
    ctk.CTkButton(controls, text="Reset poređenje", command=clear_compare).pack(
        side="left", padx=(12, 6)
    )

    kpi = ctk.CTkFrame(frame)
    kpi.pack(fill="x", padx=6, pady=(0, 6))

    table = ctk.CTkFrame(kpi, fg_color="transparent")
    table.pack(fill="x", padx=10, pady=(10, 10))
    table.grid_columnconfigure(0, weight=2)
    table.grid_columnconfigure(1, weight=1)
    table.grid_columnconfigure(2, weight=1)
    table.grid_columnconfigure(3, weight=1)

    def _h(text: str, col: int):
        ctk.CTkLabel(table, text=text, font=ctk.CTkFont(size=12, weight="bold")).grid(
            row=0, column=col, sticky="w", padx=6, pady=(0, 6)
        )

    _h("", 0)
    _h("Period", 1)
    _h("Poređenje", 2)
    _h("Δ", 3)

    def _metric_row(row: int, title: str, *, big: bool = False):
        ctk.CTkLabel(table, text=title).grid(
            row=row, column=0, sticky="w", padx=6, pady=2
        )
        font_a = ctk.CTkFont(size=20 if big else 16, weight="bold" if big else "normal")
        font_b = ctk.CTkFont(size=18 if big else 16, weight="bold" if big else "normal")
        a = ctk.CTkLabel(table, text="0", font=font_a)
        b = ctk.CTkLabel(table, text="-", font=font_b)
        d = ctk.CTkLabel(table, text="", font=ctk.CTkFont(size=14))
        a.grid(row=row, column=1, sticky="w", padx=6, pady=2)
        b.grid(row=row, column=2, sticky="w", padx=6, pady=2)
        d.grid(row=row, column=3, sticky="w", padx=6, pady=2)
        return a, b, d

    lbl_net_revenue, lbl_net_revenue_cmp, lbl_net_revenue_delta = _metric_row(
        1, "Bruto prihod (SP cash)", big=True
    )
    ctk.CTkButton(table, text="Export cash breakdown", command=export_neto_breakdown).grid(
        row=1, column=0, sticky="e", padx=6, pady=2
    )
    lbl_expenses, lbl_expenses_cmp, lbl_expenses_delta = _metric_row(
        2, "Troškovi (banka)"
    )
    lbl_refunds, lbl_refunds_cmp, lbl_refunds_delta = _metric_row(3, "Povrati (banka)")
    lbl_net_profit, lbl_net_profit_cmp, lbl_net_profit_delta = _metric_row(
        4, "Neto (Bruto cash - Troškovi)", big=True
    )

    ctk.CTkLabel(
        kpi,
        text="Bruto prihod (SP cash) = COD (+dodatno) poslije popusta; 'Vraćeno/Vraceno' se ne računa.",
    ).pack(anchor="w", padx=10, pady=(0, 6))

    unpaid = ctk.CTkFrame(frame)
    unpaid.pack(fill="x", padx=6, pady=(0, 6))
    ctk.CTkLabel(
        unpaid, text="Neuplaćeno od SP (all-time, isporučeno, bez SP uplate)"
    ).pack(anchor="w", padx=10, pady=(10, 2))
    unpaid_row = ctk.CTkFrame(unpaid, fg_color="transparent")
    unpaid_row.pack(fill="x", padx=10, pady=(0, 10))
    unpaid_var = ctk.StringVar(value="0")
    ctk.CTkLabel(
        unpaid_row, textvariable=unpaid_var, font=ctk.CTkFont(size=20, weight="bold")
    ).pack(side="left")
    ctk.CTkButton(unpaid_row, text="Export detaljno", command=export_unpaid_sp_orders).pack(
        side="left", padx=12
    )

    pending = ctk.CTkFrame(frame)
    pending.pack(fill="x", padx=6, pady=(0, 6))
    ctk.CTkLabel(pending, text="Na čekanju (all-time, poslato/u obradi, bez SP uplate)").pack(
        anchor="w", padx=10, pady=(10, 2)
    )
    pending_row = ctk.CTkFrame(pending, fg_color="transparent")
    pending_row.pack(fill="x", padx=10, pady=(0, 10))
    pending_var = ctk.StringVar(value="0")
    ctk.CTkLabel(
        pending_row, textvariable=pending_var, font=ctk.CTkFont(size=20, weight="bold")
    ).pack(side="left")
    ctk.CTkButton(pending_row, text="Export detaljno", command=export_pending_sp_orders).pack(
        side="left", padx=12
    )

    charts = ctk.CTkFrame(frame)
    charts.pack(fill="both", expand=True, padx=6, pady=(0, 6))
    ctk.CTkLabel(charts, text="Grafika (po mjesecu): Prihodi / Rashodi / Neto").pack(
        anchor="w", padx=10, pady=(10, 4)
    )
    fig_monthly = Figure(figsize=(8, 3), dpi=100)
    ax_monthly = fig_monthly.add_subplot(111)
    canvas_monthly = FigureCanvasTkAgg(fig_monthly, master=charts)
    canvas_monthly.get_tk_widget().pack(
        side="top", fill="both", expand=True, padx=6, pady=(0, 6)
    )

    def show_help():
        messagebox.showinfo(
            "Info",
            "Period u Finansije je nezavisan od Dashboard filtera.\n"
            "Odaberi period ili quick mjeseci, i (opcionalno) poređenje.",
        )

    ctk.CTkButton(frame, text="Pomoć", command=show_help).pack(
        anchor="w", padx=6, pady=(8, 0)
    )

    try:
        lock_overlay.lift()
    except Exception:
        pass

    return {
        "lock_overlay": lock_overlay,
        "lbl_net_revenue": lbl_net_revenue,
        "lbl_net_revenue_cmp": lbl_net_revenue_cmp,
        "lbl_net_revenue_delta": lbl_net_revenue_delta,
        "lbl_expenses": lbl_expenses,
        "lbl_expenses_cmp": lbl_expenses_cmp,
        "lbl_expenses_delta": lbl_expenses_delta,
        "lbl_refunds": lbl_refunds,
        "lbl_refunds_cmp": lbl_refunds_cmp,
        "lbl_refunds_delta": lbl_refunds_delta,
        "lbl_net_profit": lbl_net_profit,
        "lbl_net_profit_cmp": lbl_net_profit_cmp,
        "lbl_net_profit_delta": lbl_net_profit_delta,
        "unpaid_var": unpaid_var,
        "pending_var": pending_var,
        "period_label_var": period_label_var,
        "compare_label_var": compare_label_var,
        "ax_monthly": ax_monthly,
        "canvas_monthly": canvas_monthly,
    }

