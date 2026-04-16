from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from configs.settings import get_settings


class Database:
    def __init__(self, db_path: str | None = None) -> None:
        settings = get_settings()
        self.db_path = Path(db_path or settings.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _get_connection(conn: sqlite3.Connection | None, db_path: Path) -> sqlite3.Connection:
        if conn is not None:
            return conn
        new_conn = sqlite3.connect(db_path)
        new_conn.row_factory = sqlite3.Row
        return new_conn

    def initialize(self, schema_path: str = "storage/schema.sql") -> None:
        schema_file = Path(schema_path)
        sql = schema_file.read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.executescript(sql)
            conn.commit()

    def fetch_all_positions(self) -> list[dict[str, Any]]:
        query = """
        SELECT ticker, shares, avg_cost, source, priority, last_synced_at
        FROM positions
        ORDER BY ticker ASC
        """
        with self.connect() as conn:
            rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]

    def fetch_positions_by_ticker(
        self,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, dict[str, Any]]:
        query = """
        SELECT ticker, shares, avg_cost, source, priority, last_synced_at, sheet_row_index, sync_checksum
        FROM positions
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            rows = active_conn.execute(query).fetchall()
            return {row["ticker"]: dict(row) for row in rows}
        finally:
            if owns_connection:
                active_conn.close()

    def fetch_watchlist_by_ticker(
        self,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, dict[str, Any]]:
        query = """
        SELECT ticker, notes, tracking_status, priority, last_synced_at, sheet_row_index, sync_checksum
        FROM watchlist
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            rows = active_conn.execute(query).fetchall()
            return {row["ticker"]: dict(row) for row in rows}
        finally:
            if owns_connection:
                active_conn.close()

    def insert_position(
        self,
        ticker: str,
        shares: float,
        avg_cost: float,
        source: str | None,
        priority: int | None,
        last_synced_at: str,
        sheet_row_index: int,
        sync_checksum: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        INSERT INTO positions (
            ticker, shares, avg_cost, source, priority, last_synced_at, sheet_row_index, sync_checksum
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(
                query,
                (
                    ticker,
                    shares,
                    avg_cost,
                    source,
                    priority,
                    last_synced_at,
                    sheet_row_index,
                    sync_checksum,
                ),
            )
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()

    def update_position(
        self,
        ticker: str,
        shares: float,
        avg_cost: float,
        source: str | None,
        priority: int | None,
        last_synced_at: str,
        sheet_row_index: int,
        sync_checksum: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        UPDATE positions
        SET shares = ?,
            avg_cost = ?,
            source = ?,
            priority = ?,
            last_synced_at = ?,
            sheet_row_index = ?,
            sync_checksum = ?
        WHERE ticker = ?
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(
                query,
                (
                    shares,
                    avg_cost,
                    source,
                    priority,
                    last_synced_at,
                    sheet_row_index,
                    sync_checksum,
                    ticker,
                ),
            )
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()

    def insert_watchlist(
        self,
        ticker: str,
        notes: str | None,
        tracking_status: str | None,
        priority: int | None,
        last_synced_at: str,
        sheet_row_index: int,
        sync_checksum: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        INSERT INTO watchlist (
            ticker, notes, tracking_status, priority, last_synced_at, sheet_row_index, sync_checksum
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(
                query,
                (
                    ticker,
                    notes,
                    tracking_status,
                    priority,
                    last_synced_at,
                    sheet_row_index,
                    sync_checksum,
                ),
            )
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()

    def update_watchlist(
        self,
        ticker: str,
        notes: str | None,
        tracking_status: str | None,
        priority: int | None,
        last_synced_at: str,
        sheet_row_index: int,
        sync_checksum: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        UPDATE watchlist
        SET notes = ?,
            tracking_status = ?,
            priority = ?,
            last_synced_at = ?,
            sheet_row_index = ?,
            sync_checksum = ?
        WHERE ticker = ?
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(
                query,
                (
                    notes,
                    tracking_status,
                    priority,
                    last_synced_at,
                    sheet_row_index,
                    sync_checksum,
                    ticker,
                ),
            )
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()

    def insert_sync_run(
        self,
        started_at: str,
        status: str,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        query = """
        INSERT INTO sync_runs (started_at, status)
        VALUES (?, ?)
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            cursor = active_conn.execute(query, (started_at, status))
            if owns_connection:
                active_conn.commit()
            return int(cursor.lastrowid)
        finally:
            if owns_connection:
                active_conn.close()

    def finish_sync_run(
        self,
        sync_id: int,
        ended_at: str,
        status: str,
        rows_inserted: int,
        rows_updated: int,
        rows_skipped: int,
        error_message: str | None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        UPDATE sync_runs
        SET ended_at = ?,
            status = ?,
            rows_inserted = ?,
            rows_updated = ?,
            rows_skipped = ?,
            error_message = ?
        WHERE sync_id = ?
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(
                query,
                (
                    ended_at,
                    status,
                    rows_inserted,
                    rows_updated,
                    rows_skipped,
                    error_message,
                    sync_id,
                ),
            )
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()

    def insert_event_log(
        self,
        level: str,
        source: str,
        message: str,
        context_json: str | None,
        created_at: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        query = """
        INSERT INTO event_log (level, source, message, context_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """
        owns_connection = conn is None
        active_conn = self._get_connection(conn, self.db_path)
        try:
            active_conn.execute(query, (level, source, message, context_json, created_at))
            if owns_connection:
                active_conn.commit()
        finally:
            if owns_connection:
                active_conn.close()
