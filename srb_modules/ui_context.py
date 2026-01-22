from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class UIContext:
    """
    Minimal shared context for UI modularization.

    Idea: umjesto da tabovi zavise od closure varijabli iz `run_ui()`,
    svaka sekcija dobije `ctx` (state + callback-e + ref-ove na widgete).
    """

    state: dict[str, Any]

    # Optional callbacks that tabs can call when finished
    refresh_dashboard: Callable[[], None] | None = None
    refresh_poslovanje_lists: Callable[[], None] | None = None

    # Shared widgets/state for "Poslovanje" actions
    status_var: Any | None = None
    progress: Any | None = None
    progress_pct_var: Any | None = None
    progress_eta_var: Any | None = None
    action_buttons: list[Any] | None = None
    executor: Any | None = None
