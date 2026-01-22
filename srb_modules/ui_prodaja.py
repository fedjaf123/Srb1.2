from __future__ import annotations

from datetime import date
from typing import Any, Callable

from .ui_context import UIContext


def build_prodaja_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_prodaja: Any,
    Figure: Any,
    FigureCanvasTkAgg: Any,
    messagebox: Any,
    add_calendar_picker: Callable[..., Any],
    pick_date_range_dialog: Callable[
        [str, date | None, date | None], tuple[date | None, date | None]
    ],
    parse_user_date: Callable[[str], date | None],
    format_user_date: Callable[[date], str],
    refresh_prodaja_pregled: Callable[[], None],
    refresh_trending: Callable[[], None],
    refresh_trending_chart: Callable[[], None],
    refresh_snizenja: Callable[[], None],
    export_oos_gubitci_excel: Callable[[], None],
) -> dict[str, Any]:
    prodaja_tabs = ctk.CTkTabview(tab_prodaja)
    prodaja_tabs.pack(fill="both", expand=True, padx=10, pady=10)
    tab_prodaja_pregled = prodaja_tabs.add("Out of stock gubitci")
    tab_prodaja_trending = prodaja_tabs.add("Trending proizvodi")
    tab_prodaja_snizenja = prodaja_tabs.add("Analiza sniženja")

    widgets: dict[str, Any] = {
        "prodaja_tabs": prodaja_tabs,
        "tab_prodaja_pregled": tab_prodaja_pregled,
        "tab_prodaja_trending": tab_prodaja_trending,
        "tab_prodaja_snizenja": tab_prodaja_snizenja,
    }

    # --- Pregled (Out of stock gubitci) ---
    pregled_ops = ctk.CTkFrame(tab_prodaja_pregled)
    pregled_ops.pack(fill="x", padx=10, pady=(10, 6))
    ctk.CTkLabel(pregled_ops, text="Period:").pack(side="left", padx=(6, 4))
    prodaja_period_var = ctk.StringVar(value="12 mjeseci")
    period_mapping = {
        "Svo vrijeme": None,
        "3 mjeseca": 90,
        "6 mjeseci": 180,
        "12 mjeseci": 360,
        "24 mjeseca": 720,
    }
    prodaja_custom_from_var = ctk.StringVar(value="")
    prodaja_custom_to_var = ctk.StringVar(value="")
    prodaja_period_summary_var = ctk.StringVar(value="")

    def on_prodaja_period_change(choice: str):
        ctx.state["prodaja_period_days"] = period_mapping.get(choice)
        ctx.state["prodaja_period_start"] = None
        ctx.state["prodaja_period_end"] = None
        prodaja_custom_from_var.set("")
        prodaja_custom_to_var.set("")
        refresh_prodaja_pregled()

    ctk.CTkOptionMenu(
        pregled_ops,
        values=list(period_mapping.keys()),
        variable=prodaja_period_var,
        command=on_prodaja_period_change,
    ).pack(side="left", padx=4)

    ctk.CTkLabel(pregled_ops, text="Od (DD-MM-YYYY):").pack(side="left", padx=(12, 4))
    row_from, ent_from = add_calendar_picker(
        pregled_ops, prodaja_custom_from_var, width=120
    )
    row_from.pack(side="left", padx=4)
    ctk.CTkLabel(pregled_ops, text="Do (DD-MM-YYYY):").pack(side="left", padx=(12, 4))
    row_to, ent_to = add_calendar_picker(pregled_ops, prodaja_custom_to_var, width=120)
    row_to.pack(side="left", padx=4)

    def apply_prodaja_custom():
        start = parse_user_date(prodaja_custom_from_var.get())
        end = parse_user_date(prodaja_custom_to_var.get())
        if start is None or end is None:
            messagebox.showerror("Greska", "Unesi ispravan period (DD-MM-YYYY).")
            return
        if end < start:
            messagebox.showerror("Greska", "Datum završetka mora biti poslije početka.")
            return
        ctx.state["prodaja_period_days"] = None
        ctx.state["prodaja_period_start"] = start
        ctx.state["prodaja_period_end"] = end
        refresh_prodaja_pregled()

    ctk.CTkButton(pregled_ops, text="Primijeni period", command=apply_prodaja_custom).pack(
        side="left", padx=(12, 4)
    )
    ctk.CTkLabel(pregled_ops, textvariable=prodaja_period_summary_var).pack(
        side="left", padx=12
    )

    pregled_body = ctk.CTkFrame(tab_prodaja_pregled)
    pregled_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    pregled_left = ctk.CTkFrame(pregled_body)
    pregled_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    pregled_right = ctk.CTkFrame(pregled_body)
    pregled_right.pack(side="left", fill="both", expand=True, padx=6, pady=6)

    pregled_left_header = ctk.CTkFrame(pregled_left)
    pregled_left_header.pack(fill="x", padx=6, pady=(6, 6))
    ctk.CTkLabel(pregled_left_header, text="Prikaz:").pack(side="left", padx=(0, 6))
    pregled_scope_var = ctk.StringVar(value="Period")
    ctk.CTkOptionMenu(
        pregled_left_header,
        values=["Period", "Sve vrijeme"],
        variable=pregled_scope_var,
        command=lambda _v: refresh_prodaja_pregled(),
    ).pack(side="left", padx=4)
    ctk.CTkLabel(pregled_left_header, text="Top:").pack(side="left", padx=(12, 6))
    pregled_topn_var = ctk.StringVar(value="5")
    ctk.CTkOptionMenu(
        pregled_left_header,
        values=["5", "10"],
        variable=pregled_topn_var,
        command=lambda _v: refresh_prodaja_pregled(),
        width=80,
    ).pack(side="left", padx=4)

    ctk.CTkLabel(pregled_left, text="Potencijalni OOS gubitci (neto):").pack(
        anchor="w", padx=6, pady=(0, 4)
    )
    pregled_top_txt = ctk.CTkTextbox(pregled_left, height=360)
    pregled_top_txt.pack(fill="both", expand=True, padx=6, pady=(0, 6))
    pregled_top_txt.configure(state="disabled")

    pregled_right_header = ctk.CTkFrame(pregled_right)
    pregled_right_header.pack(fill="x", padx=6, pady=(6, 6))
    ctk.CTkLabel(pregled_right_header, text="Artikal/SKU:").pack(
        side="left", padx=(0, 6)
    )
    pregled_search_var = ctk.StringVar(value="")
    ent_pregled_search = ctk.CTkEntry(
        pregled_right_header, textvariable=pregled_search_var, width=220
    )
    ent_pregled_search.pack(side="left", padx=4)
    pregled_selected_sku_var = ctk.StringVar(value="")
    sku_menu = ctk.CTkOptionMenu(
        pregled_right_header,
        values=[""],
        variable=pregled_selected_sku_var,
        width=160,
    )
    sku_menu.pack(side="left", padx=(12, 4))
    ctk.CTkButton(
        pregled_right_header,
        text="Prikazi",
        command=lambda: refresh_prodaja_pregled(),
        width=100,
    ).pack(side="left", padx=4)

    ctk.CTkLabel(pregled_right, text="Detalji po artiklu:").pack(
        anchor="w", padx=6, pady=(0, 4)
    )
    pregled_details_txt = ctk.CTkTextbox(pregled_right, height=360)
    pregled_details_txt.pack(fill="both", expand=True, padx=6, pady=(0, 6))
    pregled_details_txt.configure(state="disabled")

    ctk.CTkButton(
        pregled_ops, text="Export Excel (OOS)", command=export_oos_gubitci_excel
    ).pack(side="left", padx=(12, 4))

    widgets.update(
        {
            "prodaja_period_var": prodaja_period_var,
            "period_mapping": period_mapping,
            "prodaja_custom_from_var": prodaja_custom_from_var,
            "prodaja_custom_to_var": prodaja_custom_to_var,
            "prodaja_period_summary_var": prodaja_period_summary_var,
            "row_from": row_from,
            "ent_from": ent_from,
            "row_to": row_to,
            "ent_to": ent_to,
            "pregled_scope_var": pregled_scope_var,
            "pregled_topn_var": pregled_topn_var,
            "pregled_top_txt": pregled_top_txt,
            "pregled_search_var": pregled_search_var,
            "ent_pregled_search": ent_pregled_search,
            "pregled_selected_sku_var": pregled_selected_sku_var,
            "sku_menu": sku_menu,
            "pregled_details_txt": pregled_details_txt,
        }
    )

    # --- Trending proizvodi ---
    trending_ops = ctk.CTkFrame(tab_prodaja_trending)
    trending_ops.pack(fill="x", padx=10, pady=(10, 6))

    trending_from_var = ctk.StringVar(value="")
    trending_to_var = ctk.StringVar(value="")
    trending_period_var = ctk.StringVar(value="Period: -")
    trending_topn_var = ctk.StringVar(value="5")
    trending_summary_var = ctk.StringVar(value="")

    def update_trending_period_label() -> None:
        start = parse_user_date(trending_from_var.get())
        end = parse_user_date(trending_to_var.get())
        if start and end and end >= start:
            trending_period_var.set(
                f"Period: {start.strftime('%d-%m-%Y')} - {end.strftime('%d-%m-%Y')}"
            )
        else:
            trending_period_var.set("Period: -")

    def pick_trending_period():
        cur_start = parse_user_date(trending_from_var.get())
        cur_end = parse_user_date(trending_to_var.get())
        s, e = pick_date_range_dialog("Odaberi period", cur_start, cur_end)
        if s and e:
            trending_from_var.set(format_user_date(s))
            trending_to_var.set(format_user_date(e))
        update_trending_period_label()
        refresh_trending()

    ctk.CTkButton(trending_ops, text="Odaberi period", command=pick_trending_period).pack(
        side="left", padx=(6, 6)
    )
    ctk.CTkLabel(trending_ops, textvariable=trending_period_var).pack(
        side="left", padx=(0, 12)
    )
    ctk.CTkLabel(trending_ops, text="Top:").pack(side="left", padx=(6, 4))
    ctk.CTkOptionMenu(
        trending_ops,
        values=["5", "10"],
        variable=trending_topn_var,
        width=90,
        command=lambda _v: refresh_trending(),
    ).pack(side="left", padx=4)
    ctk.CTkButton(trending_ops, text="Primijeni", command=lambda: refresh_trending()).pack(
        side="left", padx=(12, 4)
    )
    ctk.CTkLabel(trending_ops, textvariable=trending_summary_var).pack(
        side="left", padx=12
    )

    trending_body = ctk.CTkFrame(tab_prodaja_trending)
    trending_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    trending_left = ctk.CTkFrame(trending_body)
    trending_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    trending_right = ctk.CTkFrame(trending_body)
    trending_right.pack(side="left", fill="both", expand=True, padx=6, pady=6)

    ctk.CTkLabel(trending_left, text="Trending lista (potražnja):").pack(
        anchor="w", padx=6, pady=(6, 4)
    )
    trending_list_txt = ctk.CTkTextbox(trending_left, height=520)
    trending_list_txt.pack(fill="both", expand=True, padx=6, pady=(0, 6))
    trending_list_txt.configure(state="disabled")

    trending_right_header = ctk.CTkFrame(trending_right)
    trending_right_header.pack(fill="x", padx=6, pady=(6, 6))
    ctk.CTkLabel(trending_right_header, text="SKU / Artikal:").pack(
        side="left", padx=(0, 6)
    )
    trending_search_var = ctk.StringVar(value="")
    ent_trending_search = ctk.CTkEntry(
        trending_right_header, textvariable=trending_search_var, width=240
    )
    ent_trending_search.pack(side="left", padx=4)
    trending_selected_sku_var = ctk.StringVar(value="")
    trending_sku_menu = ctk.CTkOptionMenu(
        trending_right_header,
        values=[""],
        variable=trending_selected_sku_var,
        width=170,
    )
    trending_sku_menu.pack(side="left", padx=(12, 4))

    trending_chart_title_var = ctk.StringVar(value="Graf (odaberi SKU)")
    ctk.CTkLabel(trending_right, textvariable=trending_chart_title_var).pack(
        anchor="w", padx=6, pady=(0, 4)
    )

    fig_trend = Figure(figsize=(5, 3), dpi=100)
    ax_trend = fig_trend.add_subplot(111)
    ax_trend.grid(True, color="#d0d0d0", linewidth=0.8)
    canvas_trend = FigureCanvasTkAgg(fig_trend, master=trending_right)
    canvas_trend.get_tk_widget().pack(fill="both", expand=True, padx=6, pady=(0, 6))

    def _on_trending_search_change(*_args):
        refresh_trending()

    def _on_trending_sku_change(*_args):
        refresh_trending_chart()

    trending_search_var.trace_add("write", _on_trending_search_change)
    trending_selected_sku_var.trace_add("write", _on_trending_sku_change)

    widgets.update(
        {
            "trending_from_var": trending_from_var,
            "trending_to_var": trending_to_var,
            "trending_period_var": trending_period_var,
            "trending_topn_var": trending_topn_var,
            "trending_summary_var": trending_summary_var,
            "trending_list_txt": trending_list_txt,
            "trending_search_var": trending_search_var,
            "ent_trending_search": ent_trending_search,
            "trending_selected_sku_var": trending_selected_sku_var,
            "trending_sku_menu": trending_sku_menu,
            "trending_chart_title_var": trending_chart_title_var,
            "ax_trend": ax_trend,
            "canvas_trend": canvas_trend,
            "update_trending_period_label": update_trending_period_label,
        }
    )

    # --- Analiza sniženja ---
    snizenja_ops = ctk.CTkFrame(tab_prodaja_snizenja)
    snizenja_ops.pack(fill="x", padx=10, pady=(10, 6))
    sniz_from_var = ctk.StringVar(value="")
    sniz_to_var = ctk.StringVar(value="")
    pre_window_var = ctk.StringVar(value="2")
    sniz_summary_var = ctk.StringVar(value="")
    ctk.CTkLabel(snizenja_ops, text="Od (DD-MM-YYYY):").pack(side="left", padx=(6, 4))
    srow_from, sent_from = add_calendar_picker(snizenja_ops, sniz_from_var, width=120)
    srow_from.pack(side="left", padx=4)
    ctk.CTkLabel(snizenja_ops, text="Do (DD-MM-YYYY):").pack(side="left", padx=(12, 4))
    srow_to, sent_to = add_calendar_picker(snizenja_ops, sniz_to_var, width=120)
    srow_to.pack(side="left", padx=4)
    ctk.CTkLabel(snizenja_ops, text="Pre-period (mjeseci):").pack(side="left", padx=(12, 4))
    ctk.CTkOptionMenu(
        snizenja_ops, values=["1", "2", "3", "4", "5"], variable=pre_window_var
    ).pack(side="left", padx=4)

    sniz_body = ctk.CTkFrame(tab_prodaja_snizenja)
    sniz_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    sniz_txt = ctk.CTkTextbox(sniz_body)
    sniz_txt.pack(fill="both", expand=True, padx=6, pady=6)
    sniz_txt.configure(state="disabled")

    ctk.CTkButton(snizenja_ops, text="Primijeni", command=refresh_snizenja).pack(
        side="left", padx=(12, 4)
    )
    ctk.CTkLabel(snizenja_ops, textvariable=sniz_summary_var).pack(side="left", padx=12)

    widgets.update(
        {
            "sniz_from_var": sniz_from_var,
            "sniz_to_var": sniz_to_var,
            "pre_window_var": pre_window_var,
            "sniz_summary_var": sniz_summary_var,
            "sniz_txt": sniz_txt,
            "srow_from": srow_from,
            "sent_from": sent_from,
            "srow_to": srow_to,
            "sent_to": sent_to,
        }
    )

    return widgets
