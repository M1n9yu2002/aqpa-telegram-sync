from __future__ import annotations

import re


def normalize_ticker(raw_ticker: str) -> str:
    """
    Normalize ticker into internal canonical form.

    Current Phase 1 rule:
    - keep Taiwan stock ticker as plain numeric code, e.g. "2330"
    - strip spaces
    - uppercase letters if any
    - remove optional ".TW" suffix if user typed it
    """
    value = raw_ticker.strip().upper()

    if value.endswith(".TW"):
        value = value[:-3]

    return value


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def is_valid_taiwan_ticker(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4,6}", value))
