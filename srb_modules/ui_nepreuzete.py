from __future__ import annotations

from typing import Any, Callable

from .ui_context import UIContext


def build_nepreuzete_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_nepreuzete: Any,
    Figure: Any,
    FigureCanvasTkAgg: Any,
    refresh_unpicked_charts: Callable[[], None],
    run_export_unpicked_full: Callable[[], None],
    run_dexpress_tracking: Callable[[], None],
):
    nepreuzete_frame = ctk.CTkFrame(tab_nepreuzete)
    nepreuzete_frame.pack(fill="both", expand=True, padx=10, pady=10)
    nepreuzete_ops = ctk.CTkFrame(nepreuzete_frame)
    nepreuzete_ops.pack(fill="x", padx=6, pady=(0, 6))
    unpicked_period_var = ctk.StringVar(value="Svo vrijeme")
    ctk.CTkLabel(nepreuzete_ops, text="Period:").pack(side="left", padx=(6, 4))

    def on_unpicked_period_change(choice: str):
        mapping = {
            "Svo vrijeme": None,
            "3 mjeseca": 90,
            "6 mjeseci": 180,
            "12 mjeseci": 360,
            "24 mjeseca": 720,
        }
        ctx.state["unpicked_period_days"] = mapping.get(choice)
        ctx.state["unpicked_period_start"] = None
        ctx.state["unpicked_period_end"] = None
        refresh_unpicked_charts()

    ctk.CTkOptionMenu(
        nepreuzete_ops,
        values=["Svo vrijeme", "3 mjeseca", "6 mjeseci", "12 mjeseci", "24 mjeseca"],
        variable=unpicked_period_var,
        command=on_unpicked_period_change,
    ).pack(side="left", padx=4)

    ctk.CTkLabel(nepreuzete_ops, text="Batch:").pack(side="left", padx=(12, 4))
    tracking_batch_var = ctk.StringVar(value="20")
    ctk.CTkEntry(nepreuzete_ops, width=60, textvariable=tracking_batch_var).pack(
        side="left", padx=4
    )
    tracking_force_var = ctk.BooleanVar(value=False)
    ctk.CTkCheckBox(
        nepreuzete_ops, text="Force refresh", variable=tracking_force_var
    ).pack(side="left", padx=6)

    ctk.CTkButton(
        nepreuzete_ops,
        text="Export nepreuzete (detaljno)",
        command=run_export_unpicked_full,
    ).pack(side="left", padx=4)
    ctk.CTkButton(
        nepreuzete_ops,
        text="Dexpress analiza",
        command=run_dexpress_tracking,
    ).pack(side="left", padx=4)

    lbl_unpicked_total = ctk.CTkLabel(nepreuzete_ops, text="Nepreuzete: 0")
    lbl_unpicked_total.pack(side="left", padx=12)
    lbl_unpicked_lost = ctk.CTkLabel(nepreuzete_ops, text="Izgubljena prodaja: 0")
    lbl_unpicked_lost.pack(side="left", padx=12)
    lbl_unpicked_repeat = ctk.CTkLabel(nepreuzete_ops, text="Kupci 2+ nepreuzetih: 0")
    lbl_unpicked_repeat.pack(side="left", padx=12)

    tracking_status_var = ctk.StringVar(value="Dexpress: spremno")
    ctk.CTkLabel(nepreuzete_ops, textvariable=tracking_status_var).pack(
        side="left", padx=12
    )

    nepreuzete_left = ctk.CTkFrame(nepreuzete_frame)
    nepreuzete_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    nepreuzete_right = ctk.CTkFrame(nepreuzete_frame)
    nepreuzete_right.pack(side="left", fill="y", padx=6, pady=6)
    ctk.CTkLabel(nepreuzete_right, text="Tracking summary").pack(
        anchor="w", padx=6, pady=(6, 2)
    )
    txt_tracking_summary = ctk.CTkTextbox(nepreuzete_right, height=360, width=360)
    txt_tracking_summary.pack(fill="both", expand=False, padx=6, pady=(0, 6))
    ctk.CTkLabel(nepreuzete_right, text="Nepreuzete narudzbe (tracking)").pack(
        anchor="w", padx=6, pady=(6, 2)
    )
    txt_nepreuzete_orders = ctk.CTkTextbox(nepreuzete_right, height=260, width=360)
    txt_nepreuzete_orders.pack(fill="both", expand=False, padx=6, pady=(0, 6))

    fig_unpicked_customers = Figure(figsize=(4, 3), dpi=100)
    ax_unpicked_customers = fig_unpicked_customers.add_subplot(111)
    canvas_unpicked_customers = FigureCanvasTkAgg(
        fig_unpicked_customers, master=nepreuzete_left
    )
    canvas_unpicked_customers.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    fig_unpicked_items = Figure(figsize=(4, 3), dpi=100)
    ax_unpicked_items = fig_unpicked_items.add_subplot(111)
    canvas_unpicked_items = FigureCanvasTkAgg(
        fig_unpicked_items, master=nepreuzete_left
    )
    canvas_unpicked_items.get_tk_widget().pack(
        side="left", fill="both", expand=True, padx=6, pady=6
    )

    return {
        "unpicked_period_var": unpicked_period_var,
        "tracking_batch_var": tracking_batch_var,
        "tracking_force_var": tracking_force_var,
        "tracking_status_var": tracking_status_var,
        "lbl_unpicked_total": lbl_unpicked_total,
        "lbl_unpicked_lost": lbl_unpicked_lost,
        "lbl_unpicked_repeat": lbl_unpicked_repeat,
        "txt_tracking_summary": txt_tracking_summary,
        "txt_nepreuzete_orders": txt_nepreuzete_orders,
        "ax_unpicked_customers": ax_unpicked_customers,
        "canvas_unpicked_customers": canvas_unpicked_customers,
        "ax_unpicked_items": ax_unpicked_items,
        "canvas_unpicked_items": canvas_unpicked_items,
    }

