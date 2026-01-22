from __future__ import annotations

from datetime import date
from typing import Any, Callable

from .ui_context import UIContext


def build_finansije_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_finansije: Any,
    messagebox: Any,
    pick_date_range_dialog: Callable[
        [str, date | None, date | None], tuple[date | None, date | None]
    ],
    format_user_date: Callable[[date], str],
    refresh_finansije: Callable[[], None],
    export_unpaid_sp_orders: Callable[[], None],
) -> dict[str, Any]:
    frame = ctk.CTkFrame(tab_finansije)
    frame.pack(fill="both", expand=True, padx=12, pady=12)

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

    def pick_main_period():
        start = ctx.state.get("fin_period_start")
        end = ctx.state.get("fin_period_end")
        start, end = pick_date_range_dialog("Finansije: period", start, end)
        ctx.state["fin_period_start"] = start
        ctx.state["fin_period_end"] = end
        ctx.state["fin_period_custom"] = True
        period_label_var.set(_fmt_range(start, end))
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
    ctk.CTkLabel(controls, textvariable=period_label_var).pack(side="left", padx=10)

    ctk.CTkButton(
        controls, text="Poredi period", command=pick_compare_period
    ).pack(side="left", padx=(24, 6))
    ctk.CTkLabel(controls, textvariable=compare_label_var).pack(side="left", padx=10)
    ctk.CTkButton(controls, text="Reset poređenje", command=clear_compare).pack(
        side="left", padx=(12, 6)
    )

    kpi = ctk.CTkFrame(frame)
    kpi.pack(fill="x", padx=6, pady=(0, 6))

    row = ctk.CTkFrame(kpi, fg_color="transparent")
    row.pack(fill="x", padx=10, pady=(10, 4))
    ctk.CTkLabel(row, text="Neto prihod (period)").pack(side="left")
    lbl_net_revenue = ctk.CTkLabel(row, text="0", font=ctk.CTkFont(size=22, weight="bold"))
    lbl_net_revenue.pack(side="left", padx=12)

    row2 = ctk.CTkFrame(kpi, fg_color="transparent")
    row2.pack(fill="x", padx=10, pady=(0, 10))
    ctk.CTkLabel(row2, text="Neto prihod (poređenje)").pack(side="left")
    lbl_net_revenue_cmp = ctk.CTkLabel(
        row2, text="-", font=ctk.CTkFont(size=18, weight="bold")
    )
    lbl_net_revenue_cmp.pack(side="left", padx=12)
    lbl_net_revenue_delta = ctk.CTkLabel(row2, text="", font=ctk.CTkFont(size=16))
    lbl_net_revenue_delta.pack(side="left", padx=12)

    unpaid = ctk.CTkFrame(frame)
    unpaid.pack(fill="x", padx=6, pady=(0, 6))
    ctk.CTkLabel(unpaid, text="Neuplaćeno od SP (isporučeno, bez SP uplate)").pack(
        anchor="w", padx=10, pady=(10, 2)
    )
    unpaid_row = ctk.CTkFrame(unpaid, fg_color="transparent")
    unpaid_row.pack(fill="x", padx=10, pady=(0, 10))
    unpaid_var = ctk.StringVar(value="0")
    ctk.CTkLabel(unpaid_row, textvariable=unpaid_var, font=ctk.CTkFont(size=20, weight="bold")).pack(
        side="left"
    )
    ctk.CTkButton(
        unpaid_row,
        text="Export detaljno",
        command=export_unpaid_sp_orders,
    ).pack(side="left", padx=12)

    def show_help():
        messagebox.showinfo(
            "Info",
            "Period u Finansije je nezavisan od Dashboard filtera.\n"
            "Odaberi period i (opcionalno) poređenje, pa klikni Osvježi ako želiš pun refresh.",
        )

    ctk.CTkButton(frame, text="Pomoć", command=show_help).pack(
        anchor="w", padx=6, pady=(8, 0)
    )

    return {
        "lbl_net_revenue": lbl_net_revenue,
        "lbl_net_revenue_cmp": lbl_net_revenue_cmp,
        "lbl_net_revenue_delta": lbl_net_revenue_delta,
        "unpaid_var": unpaid_var,
        "period_label_var": period_label_var,
        "compare_label_var": compare_label_var,
    }
