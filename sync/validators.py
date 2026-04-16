from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sync.normalizer import is_valid_taiwan_ticker, normalize_text, normalize_ticker


class ValidationError(Exception):
    pass


@dataclass
class ValidatedPositionRow:
    ticker: str
    shares: float
    avg_cost: float
    source: str | None
    priority: int | None
    sheet_row_index: int


@dataclass
class ValidatedWatchlistRow:
    ticker: str
    notes: str | None
    tracking_status: str | None
    priority: int | None
    sheet_row_index: int


def _parse_float(value: Any, field_name: str, row_index: int) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"Row {row_index}: field '{field_name}' must be numeric."
        ) from exc


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def validate_positions(rows: list[dict[str, Any]]) -> list[ValidatedPositionRow]:
    validated: list[ValidatedPositionRow] = []
    seen_tickers: set[str] = set()

    for idx, row in enumerate(rows, start=2):
        raw_ticker = str(row.get("ticker", "")).strip()
        if not raw_ticker:
            raise ValidationError(f"Row {idx}: field 'ticker' is required.")

        ticker = normalize_ticker(raw_ticker)
        if not is_valid_taiwan_ticker(ticker):
            raise ValidationError(f"Row {idx}: ticker '{raw_ticker}' is invalid.")

        if ticker in seen_tickers:
            raise ValidationError(f"Row {idx}: duplicate ticker '{ticker}' found.")
        seen_tickers.add(ticker)

        shares = _parse_float(row.get("shares"), "shares", idx)
        if shares <= 0:
            raise ValidationError(f"Row {idx}: 'shares' must be > 0.")

        avg_cost = _parse_float(row.get("avg_cost"), "avg_cost", idx)
        if avg_cost < 0:
            raise ValidationError(f"Row {idx}: 'avg_cost' must be >= 0.")

        try:
            priority = _parse_optional_int(row.get("priority"))
        except ValueError as exc:
            raise ValidationError(
                f"Row {idx}: field 'priority' must be an integer."
            ) from exc

        validated.append(
            ValidatedPositionRow(
                ticker=ticker,
                shares=shares,
                avg_cost=avg_cost,
                source=normalize_text(row.get("source")),
                priority=priority,
                sheet_row_index=idx,
            )
        )

    return validated


def validate_watchlist(rows: list[dict[str, Any]]) -> list[ValidatedWatchlistRow]:
    validated: list[ValidatedWatchlistRow] = []
    seen_tickers: set[str] = set()

    for idx, row in enumerate(rows, start=2):
        raw_ticker = str(row.get("ticker", "")).strip()
        if not raw_ticker:
            raise ValidationError(f"Row {idx}: field 'ticker' is required.")

        ticker = normalize_ticker(raw_ticker)
        if not is_valid_taiwan_ticker(ticker):
            raise ValidationError(f"Row {idx}: ticker '{raw_ticker}' is invalid.")

        if ticker in seen_tickers:
            raise ValidationError(f"Row {idx}: duplicate ticker '{ticker}' found.")
        seen_tickers.add(ticker)

        try:
            priority = _parse_optional_int(row.get("priority"))
        except ValueError as exc:
            raise ValidationError(
                f"Row {idx}: field 'priority' must be an integer."
            ) from exc

        validated.append(
            ValidatedWatchlistRow(
                ticker=ticker,
                notes=normalize_text(row.get("notes")),
                tracking_status=normalize_text(row.get("tracking_status")),
                priority=priority,
                sheet_row_index=idx,
            )
        )

    return validated
