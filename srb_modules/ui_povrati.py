from __future__ import annotations

from typing import Any, Callable

from .ui_context import UIContext


def build_povrati_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_povrati: Any,
    Figure: Any,
    FigureCanvasTkAgg: Any,
    run_export_refunds_full: Callable[[], None],
):
    returns_frame = ctk.CTkFrame(tab_povrati)
    returns_frame.pack(fill="both", expand=True, padx=10, pady=10)
    returns_ops = ctk.CTkFrame(returns_frame)
    returns_ops.pack(fill="x", padx=6, pady=(0, 6))
    ctk.CTkButton(
        returns_ops,
        text="Export povrati (detaljno)",
        command=run_export_refunds_full,
    ).pack(side="left", padx=4)

    fig_ref_customers = Figure(figsize=(4, 3), dpi=100)
    ax_ref_customers = fig_ref_customers.add_subplot(111)
    canvas_ref_customers = FigureCanvasTkAgg(fig_ref_customers, master=returns_frame)
    canvas_ref_customers.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_ref_items = Figure(figsize=(4, 3), dpi=100)
    ax_ref_items = fig_ref_items.add_subplot(111)
    canvas_ref_items = FigureCanvasTkAgg(fig_ref_items, master=returns_frame)
    canvas_ref_items.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_ref_categories = Figure(figsize=(4, 3), dpi=100)
    ax_ref_categories = fig_ref_categories.add_subplot(111)
    canvas_ref_categories = FigureCanvasTkAgg(fig_ref_categories, master=returns_frame)
    canvas_ref_categories.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    return {
        "ax_ref_customers": ax_ref_customers,
        "canvas_ref_customers": canvas_ref_customers,
        "ax_ref_items": ax_ref_items,
        "canvas_ref_items": canvas_ref_items,
        "ax_ref_categories": ax_ref_categories,
        "canvas_ref_categories": canvas_ref_categories,
    }

