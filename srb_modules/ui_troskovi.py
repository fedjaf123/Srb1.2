from __future__ import annotations

from typing import Any, Callable

from .ui_context import UIContext


def build_troskovi_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_troskovi: Any,
    Figure: Any,
    FigureCanvasTkAgg: Any,
    refresh_expenses: Callable[[], None],
):
    expenses_frame = ctk.CTkFrame(tab_troskovi)
    expenses_frame.pack(fill="both", expand=True, padx=10, pady=10)
    expenses_header = ctk.CTkFrame(expenses_frame)
    expenses_header.pack(fill="x", padx=6, pady=(0, 6))
    ctk.CTkLabel(expenses_header, text="Prikazi period:").pack(side="left", padx=(6, 4))
    expense_period_var = ctk.StringVar(value="Svo vrijeme")

    def on_expense_period_change(choice: str):
        mapping = {
            "Svo vrijeme": None,
            "3 mjeseca": 90,
            "6 mjeseci": 180,
            "12 mjeseci": 360,
            "24 mjeseca": 720,
        }
        ctx.state["expense_period_days"] = mapping.get(choice)
        ctx.state["expense_period_start"] = None
        ctx.state["expense_period_end"] = None
        refresh_expenses()

    ctk.CTkOptionMenu(
        expenses_header,
        values=["Svo vrijeme", "3 mjeseca", "6 mjeseci", "12 mjeseci", "24 mjeseca"],
        variable=expense_period_var,
        command=on_expense_period_change,
    ).pack(side="left", padx=4)

    ctk.CTkLabel(expenses_header, text="Godina:").pack(side="left", padx=(12, 4))
    expense_year_var = ctk.StringVar(value="Sve")

    def on_expense_year_change(choice: str):
        ctx.state["expense_year"] = None if choice == "Sve" else choice
        refresh_expenses()

    expense_year_menu = ctk.CTkOptionMenu(
        expenses_header,
        values=["Sve"],
        variable=expense_year_var,
        command=on_expense_year_change,
        width=90,
    )
    expense_year_menu.pack(side="left", padx=4)

    ctk.CTkLabel(expenses_header, text="Mjesec:").pack(side="left", padx=(12, 4))
    expense_month_var = ctk.StringVar(value="Sve")

    def on_expense_month_change(choice: str):
        ctx.state["expense_month"] = None if choice == "Sve" else choice
        refresh_expenses()

    expense_month_menu = ctk.CTkOptionMenu(
        expenses_header,
        values=["Sve"] + [f"{i:02d}" for i in range(1, 13)],
        variable=expense_month_var,
        command=on_expense_month_change,
        width=70,
    )
    expense_month_menu.pack(side="left", padx=4)

    ctk.CTkLabel(expenses_header, text="Top:").pack(side="left", padx=(12, 4))
    expense_top_var = ctk.StringVar(value="5")

    def on_expense_top_change(choice: str):
        try:
            ctx.state["expense_top_n"] = int(choice)
        except ValueError:
            ctx.state["expense_top_n"] = 5
        refresh_expenses()

    ctk.CTkOptionMenu(
        expenses_header,
        values=["5", "10"],
        variable=expense_top_var,
        command=on_expense_top_change,
        width=70,
    ).pack(side="left", padx=4)

    expense_total_var = ctk.StringVar(value="Ukupno: 0")
    ctk.CTkLabel(expenses_header, textvariable=expense_total_var).pack(
        side="left", padx=12
    )

    expenses_charts = ctk.CTkFrame(expenses_frame)
    expenses_charts.pack(fill="both", expand=True, padx=6, pady=6)

    fig_expenses_top = Figure(figsize=(4, 3), dpi=100)
    ax_expenses_top = fig_expenses_top.add_subplot(111)
    canvas_expenses_top = FigureCanvasTkAgg(fig_expenses_top, master=expenses_charts)
    canvas_expenses_top.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_expenses_month = Figure(figsize=(4, 3), dpi=100)
    ax_expenses_month = fig_expenses_month.add_subplot(111)
    canvas_expenses_month = FigureCanvasTkAgg(fig_expenses_month, master=expenses_charts)
    canvas_expenses_month.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    return {
        "expense_period_var": expense_period_var,
        "expense_year_var": expense_year_var,
        "expense_year_menu": expense_year_menu,
        "expense_month_var": expense_month_var,
        "expense_top_var": expense_top_var,
        "expense_total_var": expense_total_var,
        "ax_expenses_top": ax_expenses_top,
        "canvas_expenses_top": canvas_expenses_top,
        "ax_expenses_month": ax_expenses_month,
        "canvas_expenses_month": canvas_expenses_month,
    }

