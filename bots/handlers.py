from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import ContextTypes

from configs.settings import get_settings
from storage.repositories import Database
from sync.normalizer import is_valid_taiwan_ticker, normalize_ticker
from sync.sync_manager import run_sync


def _is_authorized_chat(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        return False
    settings = get_settings()
    return chat.id in settings.telegram_allowed_chat_id_set


async def _deny_if_unauthorized(update: Update) -> bool:
    if _is_authorized_chat(update):
        return False

    message = update.effective_message
    if message is not None:
        await message.reply_text("This chat is not authorized.")
    return True


def _format_sync_result(result: dict[str, object]) -> str:
    if result["status"] == "success":
        return (
            "✅ Sync success\n"
            f"Inserted: {result['rows_inserted']}\n"
            f"Updated: {result['rows_updated']}\n"
            f"Skipped: {result['rows_skipped']}"
        )

    return (
        "❌ Sync failed\n"
        f"Error: {result['error_message']}"
    )


def _format_positions(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "No positions found in the SQLite cache."

    lines = ["Positions"]
    for row in rows:
        source = row["source"] if row["source"] is not None else "-"
        priority = row["priority"] if row["priority"] is not None else "-"
        lines.extend(
            [
                "",
                f"Ticker: {row['ticker']}",
                f"Shares: {row['shares']}",
                f"Avg Cost: {row['avg_cost']}",
                f"Source: {source}",
                f"Priority: {priority}",
                f"Last Synced: {row['last_synced_at']}",
            ]
        )
    return "\n".join(lines)


def _format_watchlist(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "Watchlist is empty."

    lines = ["Watchlist"]
    for row in rows:
        notes = row["notes"] if row["notes"] is not None else "-"
        tracking_status = (
            row["tracking_status"] if row["tracking_status"] is not None else "-"
        )
        priority = row["priority"] if row["priority"] is not None else "-"
        lines.extend(
            [
                "",
                f"Ticker: {row['ticker']}",
                f"Notes: {notes}",
                f"Tracking Status: {tracking_status}",
                f"Priority: {priority}",
                f"Last Synced: {row['last_synced_at']}",
            ]
        )
    return "\n".join(lines)


def _format_ticker_detail(
    ticker: str,
    position: dict[str, object] | None,
    watchlist_item: dict[str, object] | None,
) -> str:
    if position is None and watchlist_item is None:
        return f"Ticker not found: {ticker}"

    lines = [f"Ticker Detail: {ticker}"]

    if position is not None:
        source = position["source"] if position["source"] is not None else "-"
        priority = position["priority"] if position["priority"] is not None else "-"
        lines.extend(
            [
                "",
                "[Position]",
                f"Shares: {position['shares']}",
                f"Avg Cost: {position['avg_cost']}",
                f"Source: {source}",
                f"Priority: {priority}",
                f"Last Synced: {position['last_synced_at']}",
            ]
        )

    if watchlist_item is not None:
        notes = watchlist_item["notes"] if watchlist_item["notes"] is not None else "-"
        tracking_status = (
            watchlist_item["tracking_status"]
            if watchlist_item["tracking_status"] is not None
            else "-"
        )
        priority = (
            watchlist_item["priority"] if watchlist_item["priority"] is not None else "-"
        )
        lines.extend(
            [
                "",
                "[Watchlist]",
                f"Notes: {notes}",
                f"Tracking Status: {tracking_status}",
                f"Priority: {priority}",
                f"Last Synced: {watchlist_item['last_synced_at']}",
            ]
        )

    return "\n".join(lines)


def _format_help_message() -> str:
    return (
        "AQPA Commands\n\n"
        "/sync - sync Google Sheets to SQLite\n"
        "/positions - show current cached positions\n"
        "/watchlist - show current cached watchlist\n"
        "/ticker <ticker> - show merged detail for one ticker"
    )


async def sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if await _deny_if_unauthorized(update):
        return

    message = update.effective_message
    if message is None:
        return

    result = await asyncio.to_thread(run_sync)
    await message.reply_text(_format_sync_result(result))


async def positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if await _deny_if_unauthorized(update):
        return

    message = update.effective_message
    if message is None:
        return

    database = Database()
    rows = await asyncio.to_thread(database.fetch_all_positions)
    await message.reply_text(_format_positions(rows))


async def watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if await _deny_if_unauthorized(update):
        return

    message = update.effective_message
    if message is None:
        return

    database = Database()
    rows = await asyncio.to_thread(database.fetch_all_watchlist)
    await message.reply_text(_format_watchlist(rows))


async def ticker_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_unauthorized(update):
        return

    message = update.effective_message
    if message is None:
        return

    if not context.args:
        await message.reply_text("Usage: /ticker <ticker>")
        return

    ticker = normalize_ticker(context.args[0])
    if not is_valid_taiwan_ticker(ticker):
        await message.reply_text("Usage: /ticker <ticker>")
        return

    database = Database()
    position = await asyncio.to_thread(database.fetch_position_by_ticker, ticker)
    watchlist_item = await asyncio.to_thread(
        database.fetch_watchlist_item_by_ticker, ticker
    )
    await message.reply_text(_format_ticker_detail(ticker, position, watchlist_item))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if await _deny_if_unauthorized(update):
        return

    message = update.effective_message
    if message is None:
        return

    await message.reply_text(_format_help_message())
