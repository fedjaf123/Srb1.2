from __future__ import annotations

from typing import Any, Callable

from .ui_context import UIContext


def build_poslovanje_tab(
    ctx: UIContext,
    *,
    ctk: Any,
    tab_poslovanje: Any,
    show_last_imports: Callable[[], None],
    run_import_folder: Callable[[Callable[..., Any], str, str], None],
    import_sp_orders: Callable[..., Any],
    import_minimax: Callable[..., Any],
    import_sp_payments: Callable[..., Any],
    import_bank_xml: Callable[..., Any],
    import_sp_returns: Callable[..., Any],
    run_action_async_process: Callable[..., Any],
    run_match_minimax_process: Callable[..., Any],
    run_match_bank_process: Callable[..., Any],
    close_invoices_from_confirmed_matches: Callable[..., Any],
    run_action: Callable[[Callable[..., Any]], None],
    run_reset_minimax_matches: Callable[[], None],
    run_export_basic: Callable[[], None],
    run_export_single: Callable[[Callable[..., Any], str], None],
    report_unmatched_reasons: Callable[..., Any],
    run_export_bank_refunds: Callable[[], None],
    open_exports: Callable[[], None],
    refresh_poslovanje_lists: Callable[[], None],
    start_test_session: Callable[[str], None],
    _record_decision: Callable[[str], None],
    stop_test_session: Callable[[], None],
    update_baseline_ui: Callable[[], None],
    executor_factory: Callable[[], Any],
):
    poslovanje_body = ctk.CTkFrame(tab_poslovanje)
    poslovanje_body.pack(fill="both", expand=True, padx=10, pady=10)
    poslovanje_body.grid_columnconfigure(0, weight=0)
    poslovanje_body.grid_columnconfigure(1, weight=1)
    poslovanje_body.grid_rowconfigure(0, weight=1)

    ops_frame = ctk.CTkFrame(poslovanje_body)
    ops_frame.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=0)

    akcije_frame = ctk.CTkFrame(ops_frame)
    akcije_frame.pack(anchor="nw", fill="x")
    ctk.CTkLabel(akcije_frame, text="Akcije").pack(anchor="w", pady=(0, 6))
    btn_match_minimax = ctk.CTkButton(
        akcije_frame,
        text="Match Minimax",
        command=lambda: run_action_async_process(
            run_match_minimax_process,
            [str(ctx.state["db_path"])],
            "Match Minimax",
            progress_task="match_minimax",
        ),
    )
    btn_match_minimax.pack(anchor="w", pady=2)
    btn_match_banka = ctk.CTkButton(
        akcije_frame,
        text="Match Banka",
        command=lambda: run_action_async_process(
            run_match_bank_process,
            [str(ctx.state["db_path"]), 2],
            "Match Banka",
            progress_task="match_bank",
        ),
    )
    btn_match_banka.pack(anchor="w", pady=2)
    btn_close = ctk.CTkButton(
        akcije_frame,
        text="Zatvori racune",
        command=lambda: run_action(close_invoices_from_confirmed_matches),
    )
    btn_close.pack(anchor="w", pady=2)
    btn_reset_matches = ctk.CTkButton(
        akcije_frame, text="Reset match (Minimax)", command=run_reset_minimax_matches
    )
    btn_reset_matches.pack(anchor="w", pady=2)
    btn_export = ctk.CTkButton(
        akcije_frame, text="Export (osnovno)", command=run_export_basic
    )
    btn_export.pack(anchor="w", pady=6)
    btn_export_unmatched = ctk.CTkButton(
        akcije_frame,
        text="Export neuparene (razlozi)",
        command=lambda: run_export_single(report_unmatched_reasons, "unmatched-reasons"),
    )
    btn_export_unmatched.pack(anchor="w", pady=2)
    btn_export_refunds = ctk.CTkButton(
        akcije_frame,
        text="Export povrati (banka)",
        command=run_export_bank_refunds,
    )
    btn_export_refunds.pack(anchor="w", pady=2)
    btn_open_exports = ctk.CTkButton(
        akcije_frame, text="Otvori exports", command=open_exports
    )
    btn_open_exports.pack(anchor="w", pady=2)
    btn_refresh_lists = ctk.CTkButton(
        akcije_frame, text="Osvjezi liste", command=refresh_poslovanje_lists
    )
    btn_refresh_lists.pack(anchor="w", pady=2)

    ctk.CTkLabel(akcije_frame, text="Test uparivanja").pack(anchor="w", pady=(10, 4))
    test_frame = ctk.CTkFrame(akcije_frame)
    test_frame.pack(fill="x", padx=0, pady=(0, 6))
    test_top = ctk.CTkFrame(test_frame)
    test_top.pack(fill="x", padx=6, pady=(6, 6))
    ctk.CTkLabel(test_top, text="Session size:").pack(side="left", padx=(6, 4))
    test_size_var = ctk.StringVar(value="30")
    ctk.CTkEntry(test_top, width=60, textvariable=test_size_var).pack(
        side="left", padx=4
    )
    lbl_test_status = ctk.CTkLabel(test_top, text="Spremno.")
    lbl_test_status.pack(side="left", padx=12)

    test_body = ctk.CTkFrame(test_frame)
    test_body.pack(fill="x", padx=6, pady=(0, 6))
    test_left = ctk.CTkFrame(test_body)
    test_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    test_right = ctk.CTkFrame(test_body)
    test_right.pack(side="left", fill="both", expand=True, padx=6, pady=6)

    ctk.CTkLabel(test_left, text="SP narudzba").pack(anchor="w", padx=6, pady=(6, 2))
    txt_test_order = ctk.CTkTextbox(test_left, height=140)
    txt_test_order.pack(fill="both", expand=True, padx=6, pady=(0, 6))
    ctk.CTkLabel(test_right, text="Minimax racun").pack(anchor="w", padx=6, pady=(6, 2))
    txt_test_invoice = ctk.CTkTextbox(test_right, height=140)
    txt_test_invoice.pack(fill="both", expand=True, padx=6, pady=(0, 6))

    test_actions = ctk.CTkFrame(test_frame)
    test_actions.pack(fill="x", padx=6, pady=(0, 6))
    btn_test_suspicious = ctk.CTkButton(test_actions, text="Provjeri sumnjive")
    btn_test_suspicious.pack(side="left", padx=4)
    btn_test_all = ctk.CTkButton(test_actions, text="Provjeri sve")
    btn_test_all.pack(side="left", padx=4)
    btn_test_confirm = ctk.CTkButton(test_actions, text="Potvrdi")
    btn_test_confirm.pack(side="left", padx=4)
    btn_test_reject = ctk.CTkButton(test_actions, text="Odbij")
    btn_test_reject.pack(side="left", padx=4)
    btn_test_skip = ctk.CTkButton(test_actions, text="Preskoci")
    btn_test_skip.pack(side="left", padx=4)
    btn_test_stop = ctk.CTkButton(test_actions, text="Prekini sesiju")
    btn_test_stop.pack(side="left", padx=4)

    btn_test_suspicious.configure(command=lambda: start_test_session("review"))
    btn_test_all.configure(command=lambda: start_test_session("all"))
    btn_test_confirm.configure(command=lambda: _record_decision("potvrdi"))
    btn_test_reject.configure(command=lambda: _record_decision("odbij"))
    btn_test_skip.configure(command=lambda: _record_decision("preskoci"))
    btn_test_stop.configure(command=stop_test_session)

    status_var = ctk.StringVar(value="Spremno.")
    ctk.CTkLabel(ops_frame, textvariable=status_var).pack(anchor="w", pady=(8, 0))
    progress = ctk.CTkProgressBar(ops_frame, mode="indeterminate")
    progress.pack(anchor="w", fill="x", pady=(4, 0))
    progress_pct_var = ctk.StringVar(value="")
    ctk.CTkLabel(ops_frame, textvariable=progress_pct_var).pack(anchor="w", pady=(2, 0))
    progress_eta_var = ctk.StringVar(value="")
    ctk.CTkLabel(ops_frame, textvariable=progress_eta_var).pack(anchor="w", pady=(2, 0))

    ctx.status_var = status_var
    ctx.progress = progress
    ctx.progress_pct_var = progress_pct_var
    ctx.progress_eta_var = progress_eta_var

    ctx.action_buttons = [
        btn_match_minimax,
        btn_match_banka,
        btn_close,
        btn_reset_matches,
        btn_export,
        btn_export_refunds,
        btn_open_exports,
        btn_refresh_lists,
    ]

    update_baseline_ui()

    ctx.executor = executor_factory()

    lists_frame = ctk.CTkFrame(poslovanje_body)
    lists_frame.grid(row=0, column=1, sticky="nsew")

    left_list = ctk.CTkFrame(lists_frame)
    left_list.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    ctk.CTkLabel(left_list, text="Fali Minimax racun").pack(anchor="w", padx=6, pady=(6, 2))
    txt_needs = ctk.CTkTextbox(left_list, height=200)
    txt_needs.pack(fill="both", expand=True, padx=6, pady=6)
    txt_needs.configure(state="disabled")

    right_list = ctk.CTkFrame(lists_frame)
    right_list.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    ctk.CTkLabel(right_list, text="Neuparene narudzbe").pack(anchor="w", padx=6, pady=(6, 2))
    txt_unmatched = ctk.CTkTextbox(right_list, height=200)
    txt_unmatched.pack(fill="both", expand=True, padx=6, pady=6)
    txt_unmatched.configure(state="disabled")

    return {
        "txt_needs": txt_needs,
        "txt_unmatched": txt_unmatched,
        "txt_test_order": txt_test_order,
        "txt_test_invoice": txt_test_invoice,
        "lbl_test_status": lbl_test_status,
        "test_size_var": test_size_var,
    }
