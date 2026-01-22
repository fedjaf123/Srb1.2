from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .ui_context import UIContext


@dataclass(frozen=True)
class ProdajaLogicDeps:
    messagebox: Any
    format_amount: Callable[[Any], str]
    load_sku_daily_dataframe: Callable[[], pd.DataFrame]
    load_promo_dataframe: Callable[[], pd.DataFrame]
    sku_name_map: Callable[[], dict[str, str]]
    resolve_prodaja_period: Callable[[], tuple[date | None, date | None]]
    filter_daily_by_period: Callable[[pd.DataFrame, date | None, date | None], pd.DataFrame]
    aggregate_by_sku: Callable[[pd.DataFrame], pd.DataFrame]
    render_textbox: Callable[[Any, list[str]], None]
    parse_user_date: Callable[[str], date | None]
    format_user_date: Callable[[date], str]


def init_prodaja_logic(ctx: UIContext, deps: ProdajaLogicDeps) -> dict[str, Callable[[], None]]:
    def _get_widgets() -> dict[str, Any] | None:
        w = ctx.state.get("prodaja_widgets")
        return w if isinstance(w, dict) else None

    def refresh_prodaja_pregled() -> None:
        w = _get_widgets()
        if not w:
            return

        prodaja_period_summary_var = w["prodaja_period_summary_var"]
        pregled_scope_var = w["pregled_scope_var"]
        pregled_topn_var = w["pregled_topn_var"]
        pregled_top_txt = w["pregled_top_txt"]
        pregled_search_var = w["pregled_search_var"]
        pregled_selected_sku_var = w["pregled_selected_sku_var"]
        sku_menu = w["sku_menu"]
        pregled_details_txt = w["pregled_details_txt"]

        daily = deps.load_sku_daily_dataframe()
        name_map = deps.sku_name_map()
        start, end = deps.resolve_prodaja_period()
        if start and end:
            prodaja_period_summary_var.set(f"Period: {start.isoformat()} - {end.isoformat()}")
        elif start:
            prodaja_period_summary_var.set(f"Period: od {start.isoformat()}")
        elif end:
            prodaja_period_summary_var.set(f"Period: do {end.isoformat()}")
        else:
            prodaja_period_summary_var.set("Period: svi podaci")

        if daily.empty:
            deps.render_textbox(pregled_top_txt, ["Nema podataka. Pokreni metrike."])
            deps.render_textbox(pregled_details_txt, ["Nema podataka."])
            return

        filtered = deps.filter_daily_by_period(daily, start, end)
        agg_period = deps.aggregate_by_sku(filtered)
        agg_all = deps.aggregate_by_sku(daily)

        def fmt_row(idx: int, row: dict[str, Any]) -> str:
            sku = str(row.get("sku") or "")
            art = name_map.get(sku, "")
            lost_val = deps.format_amount(row.get("lost_value", 0))
            lost_qty = float(row.get("lost_qty", 0) or 0)
            oos_days = int(row.get("oos_days", 0) or 0)
            net_qty = float(row.get("net_qty", 0) or 0)
            return (
                f"{idx}. {sku} - {art}\n"
                f"   Gubitak: {lost_val} | Izgubljeno: {lost_qty:.2f} kom | OOS dana: {oos_days} | Prodato: {net_qty:.2f}"
            )

        scope = str(pregled_scope_var.get() or "").strip()
        try:
            top_n = int(str(pregled_topn_var.get() or "").strip() or "5")
        except Exception:
            top_n = 5
        top_n = 5 if top_n not in (5, 10) else top_n

        agg_src = agg_period if scope == "Period" else agg_all
        if agg_src.empty:
            msg = "Nema podataka za odabrani period." if scope == "Period" else "Nema dostupnih podataka."
            deps.render_textbox(pregled_top_txt, [msg])
        else:
            top = agg_src.sort_values("lost_value", ascending=False).head(top_n).to_dict("records")
            deps.render_textbox(pregled_top_txt, [fmt_row(i + 1, r) for i, r in enumerate(top)])

        search = str(pregled_search_var.get() or "").strip().lower()
        all_skus = sorted(set(daily["sku"].astype(str).str.strip().tolist()))
        if search:
            matches = []
            for sku in all_skus:
                name = name_map.get(sku, "")
                if search in sku.lower() or (name and search in name.lower()):
                    matches.append(sku)
            matches = matches[:50]
        else:
            matches = all_skus[:50]
        if not matches:
            matches = [""]
        try:
            sku_menu.configure(values=matches)
        except Exception:
            pass
        if str(pregled_selected_sku_var.get() or "") not in matches:
            pregled_selected_sku_var.set(matches[0])

        sel = str(pregled_selected_sku_var.get() or "").strip()
        if not sel:
            deps.render_textbox(
                pregled_details_txt, ["Unesi SKU ili naziv (pretraga) pa izaberi artikal."]
            )
            return

        sku_name = name_map.get(sel, "")
        sku_df = daily.loc[daily["sku"].astype(str).str.strip() == sel].copy()
        if sku_df.empty:
            deps.render_textbox(pregled_details_txt, [f"Nema podataka za {sel}."])
            return

        agg_sel = deps.aggregate_by_sku(sku_df)
        row = agg_sel.iloc[0].to_dict() if not agg_sel.empty else {}
        lost_val = deps.format_amount(row.get("lost_value", 0))
        lost_qty = float(row.get("lost_qty", 0) or 0)
        oos_days = int(row.get("oos_days", 0) or 0)
        net_qty = float(row.get("net_qty", 0) or 0)
        demand_qty = float(row.get("demand_qty", 0) or 0)

        last_price = (
            pd.to_numeric(sku_df.get("sp_unit_net_price", 0), errors="coerce")
            .fillna(0)
            .loc[lambda s: s > 0]
            .tail(1)
        )
        last_price_val = float(last_price.iloc[0]) if len(last_price) else 0.0

        sku_df = sku_df.sort_values("date_dt")
        last_rows = sku_df.tail(14)
        lines = [
            f"{sel} - {sku_name}",
            f"OOS dana: {oos_days} | Prodato (net): {net_qty:.2f} kom",
            f"Potraznja baseline (sum): {demand_qty:.2f} | Izgubljeno: {lost_qty:.2f} kom",
            f"Procijenjeni gubitak (neto): {lost_val}",
        ]
        if last_price_val > 0:
            lines.append(f"Zadnja poznata cijena (net): {last_price_val:.2f}")
        lines.append("")
        lines.append("Zadnjih 14 dana (datum | stock_eod | OOS | net_prod | izgubljeno):")
        for _, r in last_rows.iterrows():
            lines.append(
                f"{str(r.get('date') or '')} | "
                f"{float(r.get('stock_eod_qty') or 0):.0f} | "
                f"{int(r.get('oos_flag') or 0)} | "
                f"{float(r.get('net_sales_qty') or 0):.2f} | "
                f"{float(r.get('lost_sales_qty') or 0):.2f}"
            )
        deps.render_textbox(pregled_details_txt, lines)

    def export_oos_gubitci_excel() -> None:
        w = _get_widgets()
        if not w:
            return
        pregled_scope_var = w["pregled_scope_var"]

        daily = deps.load_sku_daily_dataframe()
        if daily.empty:
            deps.messagebox.showinfo("Info", "Nema podataka. Pokreni metrike.")
            return

        start, end = deps.resolve_prodaja_period()
        scope = str(pregled_scope_var.get() or "").strip()
        df = deps.filter_daily_by_period(daily, start, end) if scope == "Period" else daily.copy()

        if df.empty:
            deps.messagebox.showinfo("Info", "Nema podataka za odabrani period.")
            return

        agg = (
            df.groupby("sku", dropna=False)
            .agg(
                OOS_dani=("oos_flag", "sum"),
                Potraznja=("demand_baseline_qty", "sum"),
                Izgubljeno_kom=("lost_sales_qty", "sum"),
                Procijenjeni_gubitak_neto=("lost_sales_value_est", "sum"),
            )
            .reset_index()
        )
        name_map = deps.sku_name_map()
        agg.insert(1, "Artikal", agg["sku"].astype(str).map(lambda s: name_map.get(str(s).strip(), "")))
        agg = agg.sort_values("Procijenjeni_gubitak_neto", ascending=False)

        exports_dir = Path("exports")
        exports_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if scope == "Period" and start and end:
            label = f"period_{start.isoformat()}_{end.isoformat()}"
        elif scope == "Period" and start:
            label = f"od_{start.isoformat()}"
        elif scope == "Period" and end:
            label = f"do_{end.isoformat()}"
        else:
            label = "sve_vrijeme"
        out_path = exports_dir / f"oos_gubitci_{label}_{stamp}.xlsx"
        try:
            agg.to_excel(out_path, index=False, engine="openpyxl")
        except Exception as exc:
            deps.messagebox.showerror("Greska", f"Ne mogu sacuvati Excel: {exc}")
            return
        try:
            import os

            os.startfile(str(out_path))  # type: ignore[attr-defined]
        except Exception:
            deps.messagebox.showinfo("Info", f"Sacuvano: {out_path}")

    def _resolve_trending_period(daily: pd.DataFrame) -> tuple[date | None, date | None]:
        w = _get_widgets()
        if not w:
            return None, None
        trending_from_var = w["trending_from_var"]
        trending_to_var = w["trending_to_var"]
        start = deps.parse_user_date(trending_from_var.get())
        end = deps.parse_user_date(trending_to_var.get())
        if start is None or end is None or end < start:
            max_date = daily["date_dt"].max()
            if pd.isna(max_date):
                return None, None
            end = max_date
            start = end - timedelta(days=29)
            trending_from_var.set(deps.format_user_date(start))
            trending_to_var.set(deps.format_user_date(end))
        return start, end

    def _sku_suggestions(query: str, skus: list[str], name_map: dict[str, str]) -> list[str]:
        import difflib

        q = (query or "").strip().lower()
        if not q:
            return skus[:50]
        exact = [s for s in skus if s.lower() == q]
        starts = [s for s in skus if s.lower().startswith(q) and s.lower() != q]
        contains = [
            s
            for s in skus
            if q in s.lower() or (name_map.get(s, "") and q in name_map.get(s, "").lower())
        ]
        close = difflib.get_close_matches(q, skus, n=20, cutoff=0.6)
        out: list[str] = []
        for grp in (exact, starts, contains, close):
            for s in grp:
                if s not in out:
                    out.append(s)
        return out[:50] if out else skus[:50]

    def refresh_trending_chart() -> None:
        w = _get_widgets()
        if not w:
            return
        ax_trend = w["ax_trend"]
        canvas_trend = w["canvas_trend"]
        trending_chart_title_var = w["trending_chart_title_var"]
        trending_selected_sku_var = w["trending_selected_sku_var"]
        update_trending_period_label = w.get("update_trending_period_label")

        daily = deps.load_sku_daily_dataframe()
        if daily.empty:
            ax_trend.clear()
            ax_trend.grid(True, color="#d0d0d0", linewidth=0.8)
            trending_chart_title_var.set("Graf (nema podataka)")
            canvas_trend.draw_idle()
            return

        name_map = deps.sku_name_map()
        sku = str(trending_selected_sku_var.get() or "").strip()
        start, end = _resolve_trending_period(daily)
        if callable(update_trending_period_label):
            update_trending_period_label()
        if not sku or start is None or end is None:
            ax_trend.clear()
            ax_trend.grid(True, color="#d0d0d0", linewidth=0.8)
            trending_chart_title_var.set("Graf (odaberi SKU)")
            canvas_trend.draw_idle()
            return

        days_len = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days_len - 1)

        cur = deps.filter_daily_by_period(daily, start, end)
        prev = deps.filter_daily_by_period(daily, prev_start, prev_end)

        cur_s = cur.loc[cur["sku"] == sku].groupby("date_dt")["demand_baseline_qty"].sum()
        prev_s = prev.loc[prev["sku"] == sku].groupby("date_dt")["demand_baseline_qty"].sum()

        cur_dates = [start + timedelta(days=i) for i in range(days_len)]
        prev_dates = [prev_start + timedelta(days=i) for i in range(days_len)]
        cur_vals = [float(cur_s.get(d, 0.0)) for d in cur_dates]
        prev_vals = [float(prev_s.get(d, 0.0)) for d in prev_dates]

        cur_cum: list[float] = []
        prev_cum: list[float] = []
        run = 0.0
        for v in cur_vals:
            run += v
            cur_cum.append(run)
        run = 0.0
        for v in prev_vals:
            run += v
            prev_cum.append(run)

        x = list(range(1, days_len + 1))
        ax_trend.clear()
        ax_trend.grid(True, color="#d0d0d0", linewidth=0.8)
        ax_trend.plot(x, cur_cum, color="#1f7a1f", linewidth=3)
        ax_trend.plot(x, prev_cum, color="#d97a00", linewidth=3)
        ax_trend.set_xlim(1, days_len)
        ax_trend.set_ylabel("Kumulativna potražnja")
        ax_trend.set_xlabel("Dan u periodu")
        trending_chart_title_var.set(
            f"{sku} - {name_map.get(sku,'')} | {start.strftime('%d-%m-%Y')} - {end.strftime('%d-%m-%Y')} (zeleno) vs prethodni period (narandžasto)"
        )
        canvas_trend.draw_idle()

    def refresh_trending() -> None:
        w = _get_widgets()
        if not w:
            return
        trending_topn_var = w["trending_topn_var"]
        trending_summary_var = w["trending_summary_var"]
        trending_list_txt = w["trending_list_txt"]
        trending_search_var = w["trending_search_var"]
        trending_selected_sku_var = w["trending_selected_sku_var"]
        trending_sku_menu = w["trending_sku_menu"]
        update_trending_period_label = w.get("update_trending_period_label")

        daily = deps.load_sku_daily_dataframe()
        name_map = deps.sku_name_map()
        if daily.empty:
            deps.render_textbox(trending_list_txt, ["Nema podataka."])
            return

        start, end = _resolve_trending_period(daily)
        if callable(update_trending_period_label):
            update_trending_period_label()
        if start is None or end is None:
            deps.render_textbox(trending_list_txt, ["Nema datuma u podacima."])
            return

        days_len = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days_len - 1)
        trending_summary_var.set(
            f"Poređenje: {start.isoformat()} - {end.isoformat()} vs {prev_start.isoformat()} - {prev_end.isoformat()}"
        )

        cur = deps.filter_daily_by_period(daily, start, end)
        prev = deps.filter_daily_by_period(daily, prev_start, prev_end)
        cur_sum = cur.groupby("sku")["demand_baseline_qty"].sum()
        prev_sum = prev.groupby("sku")["demand_baseline_qty"].sum()

        rows = []
        for sku in set(cur_sum.index).union(prev_sum.index):
            cval = float(cur_sum.get(sku, 0.0))
            pval = float(prev_sum.get(sku, 0.0))
            delta = cval - pval
            pct = (delta / pval) if pval >= 1.0 else None
            score = pct if pct is not None else delta
            rows.append((str(sku), name_map.get(str(sku), ""), cval, pval, delta, pct, score))
        rows.sort(key=lambda x: x[6], reverse=True)

        def fmt(idx: int, row: tuple) -> str:
            sku, art, curv, prevv, delta, pct, _ = row
            pct_txt = f"{pct*100:.1f}%" if isinstance(pct, (int, float)) and pct is not None else "-"
            return (
                f"{idx}. {sku} - {art}\n"
                f"   Potražnja: {curv:.2f} | Prethodno: {prevv:.2f} | Delta: {delta:.2f} | %: {pct_txt}"
            )

        try:
            top_n = int(trending_topn_var.get() or "5")
        except Exception:
            top_n = 5
        top_n = 5 if top_n not in (5, 10) else top_n
        deps.render_textbox(trending_list_txt, [fmt(i + 1, r) for i, r in enumerate(rows[:top_n])])

        all_skus = sorted(set(daily["sku"].astype(str).str.strip().tolist()))
        suggestions = _sku_suggestions(trending_search_var.get(), all_skus, name_map)
        if not suggestions:
            suggestions = [""]
        try:
            trending_sku_menu.configure(values=suggestions)
        except Exception:
            pass
        if str(trending_selected_sku_var.get() or "") not in suggestions:
            q = str(trending_search_var.get() or "").strip()
            trending_selected_sku_var.set(q if q in suggestions else suggestions[0])

        refresh_trending_chart()

    def refresh_snizenja() -> None:
        w = _get_widgets()
        if not w:
            return
        sniz_from_var = w["sniz_from_var"]
        sniz_to_var = w["sniz_to_var"]
        pre_window_var = w["pre_window_var"]
        sniz_summary_var = w["sniz_summary_var"]
        sniz_txt = w["sniz_txt"]

        daily = deps.load_sku_daily_dataframe()
        promos = deps.load_promo_dataframe()
        name_map = deps.sku_name_map()
        if daily.empty or promos.empty:
            deps.render_textbox(sniz_txt, ["Nema podataka (metrike ili promo periodi)."])
            return

        start = deps.parse_user_date(sniz_from_var.get())
        end = deps.parse_user_date(sniz_to_var.get())
        if start is None or end is None or end < start:
            max_date = daily["date_dt"].max()
            if pd.isna(max_date):
                deps.render_textbox(sniz_txt, ["Nema datuma u podacima."])
                return
            end = max_date
            start = end - timedelta(days=180)
            sniz_from_var.set(deps.format_user_date(start))
            sniz_to_var.set(deps.format_user_date(end))

        pre_days = int(pre_window_var.get() or "2") * 30
        sniz_summary_var.set(f"Promo {start.isoformat()} → {end.isoformat()} | Pre-period: {pre_days} dana")

        promos_f = promos.loc[
            promos["promo_start_dt"].notna()
            & promos["promo_end_dt"].notna()
            & (promos["promo_start_dt"] <= end)
            & (promos["promo_end_dt"] >= start)
        ].copy()
        if promos_f.empty:
            deps.render_textbox(sniz_txt, ["Nema promo perioda u tom rasponu."])
            return

        daily = daily.copy()
        daily["net_value_est"] = daily["net_sales_qty"] * daily["sp_unit_net_price"]

        rows = []
        for _, pr in promos_f.iterrows():
            sku = str(pr.get("sku") or "")
            ps = pr.get("promo_start_dt")
            pe = pr.get("promo_end_dt")
            if not sku or ps is None or pe is None:
                continue
            promo_days = (pe - ps).days + 1
            pre_start = ps - timedelta(days=pre_days)
            pre_end = ps - timedelta(days=1)
            promo_df = daily.loc[(daily["sku"] == sku) & (daily["date_dt"] >= ps) & (daily["date_dt"] <= pe)]
            pre_df = daily.loc[(daily["sku"] == sku) & (daily["date_dt"] >= pre_start) & (daily["date_dt"] <= pre_end)]
            if promo_df.empty or pre_df.empty:
                continue
            promo_qty = float(promo_df["net_sales_qty"].sum())
            pre_qty = float(pre_df["net_sales_qty"].sum())
            promo_val = float(promo_df["net_value_est"].sum())
            pre_val = float(pre_df["net_value_est"].sum())
            lift_qty = promo_qty / promo_days - pre_qty / pre_days
            lift_val = promo_val / promo_days - pre_val / pre_days
            rows.append(
                (sku, name_map.get(sku, ""), ps, pe, float(pr.get("avg_discount_share") or 0), lift_qty, lift_val)
            )
        rows.sort(key=lambda x: x[6], reverse=True)
        lines = []
        for i, (sku, art, ps, pe, disc, lift_qty, lift_val) in enumerate(rows[:20], 1):
            lines.append(
                f"{i}. {sku} - {art}\n"
                f"   Promo: {ps.isoformat()} → {pe.isoformat()} | Udio popusta: {disc*100:.0f}%\n"
                f"   Lift/dan: {lift_qty:.2f} kom | {deps.format_amount(lift_val)}"
            )
        deps.render_textbox(sniz_txt, lines if lines else ["Nema dovoljno podataka za poređenje."])

    def refresh_prodaja_views() -> None:
        refresh_prodaja_pregled()
        refresh_trending()
        refresh_snizenja()

    return {
        "refresh_prodaja_pregled": refresh_prodaja_pregled,
        "export_oos_gubitci_excel": export_oos_gubitci_excel,
        "refresh_trending": refresh_trending,
        "refresh_trending_chart": refresh_trending_chart,
        "refresh_snizenja": refresh_snizenja,
        "refresh_prodaja_views": refresh_prodaja_views,
    }

