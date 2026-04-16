from __future__ import annotations

from storage.repositories import Database
from bots.handlers import _format_ticker_detail


def test_fetch_single_ticker_lookups(tmp_path):
    db_path = tmp_path / "test.db"
    database = Database(str(db_path))
    database.initialize()

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO positions (
                ticker, shares, avg_cost, source, priority, last_synced_at, sheet_row_index, sync_checksum
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("2317", 10.0, 1000.0, "manual", 1, "2026-04-16T11:53:04+00:00", 2, "p1"),
        )
        conn.execute(
            """
            INSERT INTO watchlist (
                ticker, notes, tracking_status, priority, last_synced_at, sheet_row_index, sync_checksum
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2317",
                "iphone / ai server",
                "active",
                1,
                "2026-04-16T10:54:28+00:00",
                2,
                "w1",
            ),
        )
        conn.commit()

    position = database.fetch_position_by_ticker("2317")
    watchlist_item = database.fetch_watchlist_item_by_ticker("2317")

    assert position is not None
    assert position["ticker"] == "2317"
    assert position["shares"] == 10.0
    assert position["avg_cost"] == 1000.0
    assert position["source"] == "manual"
    assert position["priority"] == 1

    assert watchlist_item is not None
    assert watchlist_item["ticker"] == "2317"
    assert watchlist_item["notes"] == "iphone / ai server"
    assert watchlist_item["tracking_status"] == "active"
    assert watchlist_item["priority"] == 1


def test_fetch_single_ticker_lookups_not_found(tmp_path):
    db_path = tmp_path / "test.db"
    database = Database(str(db_path))
    database.initialize()

    assert database.fetch_position_by_ticker("9999") is None
    assert database.fetch_watchlist_item_by_ticker("9999") is None


def test_format_ticker_detail_includes_both_blocks():
    message = _format_ticker_detail(
        "2317",
        {
            "ticker": "2317",
            "shares": 10.0,
            "avg_cost": 1000.0,
            "source": "manual",
            "priority": 1,
            "last_synced_at": "2026-04-16T11:53:04+00:00",
        },
        {
            "ticker": "2317",
            "notes": "iphone / ai server",
            "tracking_status": "active",
            "priority": 1,
            "last_synced_at": "2026-04-16T10:54:28+00:00",
        },
    )

    assert "Ticker Detail: 2317" in message
    assert "[Position]" in message
    assert "Shares: 10.0" in message
    assert "[Watchlist]" in message
    assert "Notes: iphone / ai server" in message


def test_format_ticker_detail_not_found():
    message = _format_ticker_detail("9999", None, None)

    assert message == "Ticker not found: 9999"
