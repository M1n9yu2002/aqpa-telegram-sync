from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
import sqlite3
from typing import Any

from adapters.sheets_client import SheetsClient
from storage.repositories import Database
from sync.validators import (
    ValidatedPositionRow,
    ValidatedWatchlistRow,
    validate_positions,
    validate_watchlist,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _position_checksum(row: ValidatedPositionRow) -> str:
    payload = json.dumps(
        {
            "ticker": row.ticker,
            "shares": row.shares,
            "avg_cost": row.avg_cost,
            "source": row.source,
            "priority": row.priority,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _watchlist_checksum(row: ValidatedWatchlistRow) -> str:
    payload = json.dumps(
        {
            "ticker": row.ticker,
            "notes": row.notes,
            "tracking_status": row.tracking_status,
            "priority": row.priority,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sync_positions(
    database: Database,
    validated_rows: list[ValidatedPositionRow],
    synced_at: str,
    counts: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    existing_rows = database.fetch_positions_map(conn=conn)

    for row in validated_rows:
        checksum = _position_checksum(row)
        existing = existing_rows.get(row.ticker)

        if existing is None:
            database.insert_position(
                ticker=row.ticker,
                shares=row.shares,
                avg_cost=row.avg_cost,
                source=row.source,
                priority=row.priority,
                last_synced_at=synced_at,
                sheet_row_index=row.sheet_row_index,
                sync_checksum=checksum,
                conn=conn,
            )
            counts["rows_inserted"] += 1
            continue

        if existing["sync_checksum"] == checksum:
            counts["rows_skipped"] += 1
            continue

        database.update_position(
            ticker=row.ticker,
            shares=row.shares,
            avg_cost=row.avg_cost,
            source=row.source,
            priority=row.priority,
            last_synced_at=synced_at,
            sheet_row_index=row.sheet_row_index,
            sync_checksum=checksum,
            conn=conn,
        )
        counts["rows_updated"] += 1


def _sync_watchlist(
    database: Database,
    validated_rows: list[ValidatedWatchlistRow],
    synced_at: str,
    counts: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    existing_rows = database.fetch_watchlist_map(conn=conn)

    for row in validated_rows:
        checksum = _watchlist_checksum(row)
        existing = existing_rows.get(row.ticker)

        if existing is None:
            database.insert_watchlist(
                ticker=row.ticker,
                notes=row.notes,
                tracking_status=row.tracking_status,
                priority=row.priority,
                last_synced_at=synced_at,
                sheet_row_index=row.sheet_row_index,
                sync_checksum=checksum,
                conn=conn,
            )
            counts["rows_inserted"] += 1
            continue

        if existing["sync_checksum"] == checksum:
            counts["rows_skipped"] += 1
            continue

        database.update_watchlist(
            ticker=row.ticker,
            notes=row.notes,
            tracking_status=row.tracking_status,
            priority=row.priority,
            last_synced_at=synced_at,
            sheet_row_index=row.sheet_row_index,
            sync_checksum=checksum,
            conn=conn,
        )
        counts["rows_updated"] += 1


def _record_failure(
    database: Database,
    sync_id: int,
    ended_at: str,
    counts: dict[str, int],
    error_message: str,
) -> None:
    with database.connect() as conn:
        database.insert_event_log(
            level="ERROR",
            source="sync_manager",
            message=error_message,
            context_json=json.dumps(
                {
                    "sync_id": sync_id,
                    "rows_inserted": counts["rows_inserted"],
                    "rows_updated": counts["rows_updated"],
                    "rows_skipped": counts["rows_skipped"],
                },
                ensure_ascii=True,
            ),
            created_at=ended_at,
            conn=conn,
        )
        database.finish_sync_run(
            sync_id=sync_id,
            ended_at=ended_at,
            status="failed",
            rows_inserted=counts["rows_inserted"],
            rows_updated=counts["rows_updated"],
            rows_skipped=counts["rows_skipped"],
            error_message=error_message,
            conn=conn,
        )
        conn.commit()


def run_sync() -> dict[str, Any]:
    database = Database()
    sheets_client = SheetsClient()
    started_at = _utc_now_iso()
    counts = {
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
    }

    sync_id = database.insert_sync_run(started_at=started_at, status="running")

    try:
        position_rows = sheets_client.read_positions()
        watchlist_rows = sheets_client.read_watchlist()

        validated_positions = validate_positions(position_rows)
        validated_watchlist = validate_watchlist(watchlist_rows)

        synced_at = _utc_now_iso()
        with database.connect() as conn:
            conn.execute("BEGIN")
            _sync_positions(database, validated_positions, synced_at, counts, conn)
            _sync_watchlist(database, validated_watchlist, synced_at, counts, conn)
            ended_at = _utc_now_iso()
            database.finish_sync_run(
                sync_id=sync_id,
                ended_at=ended_at,
                status="success",
                rows_inserted=counts["rows_inserted"],
                rows_updated=counts["rows_updated"],
                rows_skipped=counts["rows_skipped"],
                error_message=None,
                conn=conn,
            )
            conn.commit()

        return {
            "status": "success",
            "rows_inserted": counts["rows_inserted"],
            "rows_updated": counts["rows_updated"],
            "rows_skipped": counts["rows_skipped"],
            "error_message": None,
        }
    except Exception as exc:
        error_message = str(exc)
        ended_at = _utc_now_iso()
        _record_failure(
            database=database,
            sync_id=sync_id,
            ended_at=ended_at,
            counts=counts,
            error_message=error_message,
        )
        return {
            "status": "failed",
            "rows_inserted": counts["rows_inserted"],
            "rows_updated": counts["rows_updated"],
            "rows_skipped": counts["rows_skipped"],
            "error_message": error_message,
        }
