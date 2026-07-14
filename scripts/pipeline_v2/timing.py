"""Semantic timing classification, independent of horizon eligibility."""

from __future__ import annotations


TIMING_STRUCTURES = (
    "fixed_clock",
    "scheduled_event_start",
    "scheduled_window",
    "deadline_window",
    "endogenous_subevent",
    "unclear",
)

FIXED_PREFIXES = (
    "KXBTC", "KXETH", "KXBNB", "KXDOGE", "KXHYPE", "KXSOL", "KXXRP",
    "KXINXU", "KXNASDAQ100U", "KXWTI", "KXTEMPNYCH", "KXHIGH", "KXLOWT",
)
SUBEVENT_PREFIXES = (
    "KXPGAHOLESCORE", "KXATPSETWINNER", "KXWTASETWINNER", "KXCS2MAP",
    "KXVALORANTMAP", "KXDOTA2MAP",
)
WINDOW_PREFIXES = ("KXATP-", "KXWTA-", "KXWNBACCUP-")
DEADLINE_PREFIXES = ("KXKOSPI-",)
EVENT_TERMS = ("match", "game", "press conference", "total games", "total maps", "set score")


def classify_timing(
    ticker: str = "",
    title: str = "",
    *,
    explicit_timing_structure: str | None = None,
) -> tuple[str, str]:
    if explicit_timing_structure is not None:
        if explicit_timing_structure not in TIMING_STRUCTURES:
            raise ValueError(f"Unsupported timing structure: {explicit_timing_structure}")
        return explicit_timing_structure, "Explicit reviewed timing classification."

    ticker = str(ticker or "").upper()
    text = str(title or "").lower()

    if ticker.startswith(DEADLINE_PREFIXES):
        return "deadline_window", "Outcome may occur during a known deadline window."
    if ticker.startswith(SUBEVENT_PREFIXES):
        return "endogenous_subevent", "Subevent timing is not known ex ante."
    if ticker.startswith(WINDOW_PREFIXES):
        return "scheduled_window", "Multi-event or multi-day scheduled window."
    if ticker.startswith(FIXED_PREFIXES):
        return "fixed_clock", "Outcome is tied to a fixed clock or calendar boundary."
    if any(term in text for term in EVENT_TERMS):
        return "scheduled_event_start", "Parent event has a scheduled start."
    return "unclear", "Available metadata does not establish a timing structure."
