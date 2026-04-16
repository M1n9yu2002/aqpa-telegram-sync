import pytest

from sync.validators import (
    ValidationError,
    validate_positions,
    validate_watchlist,
)


def test_validate_positions_success():
    rows = [
        {
            "ticker": "2330",
            "shares": "1000",
            "avg_cost": "650.5",
            "source": "broker",
            "priority": "1",
        },
        {
            "ticker": "2317",
            "shares": 200,
            "avg_cost": 120,
            "source": " manual ",
            "priority": "",
        },
    ]

    result = validate_positions(rows)

    assert len(result) == 2
    assert result[0].ticker == "2330"
    assert result[0].shares == 1000.0
    assert result[0].avg_cost == 650.5
    assert result[0].source == "broker"
    assert result[0].priority == 1
    assert result[0].sheet_row_index == 2

    assert result[1].ticker == "2317"
    assert result[1].shares == 200.0
    assert result[1].avg_cost == 120.0
    assert result[1].source == "manual"
    assert result[1].priority is None
    assert result[1].sheet_row_index == 3


def test_validate_positions_missing_ticker():
    rows = [
        {
            "ticker": "",
            "shares": "100",
            "avg_cost": "50",
            "source": "broker",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="field 'ticker' is required"):
        validate_positions(rows)


def test_validate_positions_invalid_ticker():
    rows = [
        {
            "ticker": "abc",
            "shares": "100",
            "avg_cost": "50",
            "source": "broker",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="ticker 'abc' is invalid"):
        validate_positions(rows)


def test_validate_positions_duplicate_ticker():
    rows = [
        {
            "ticker": "2330",
            "shares": "100",
            "avg_cost": "50",
            "source": "broker",
            "priority": "1",
        },
        {
            "ticker": "2330",
            "shares": "200",
            "avg_cost": "60",
            "source": "broker",
            "priority": "2",
        },
    ]

    with pytest.raises(ValidationError, match="duplicate ticker '2330' found"):
        validate_positions(rows)


def test_validate_positions_non_numeric_shares():
    rows = [
        {
            "ticker": "2330",
            "shares": "abc",
            "avg_cost": "50",
            "source": "broker",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="field 'shares' must be numeric"):
        validate_positions(rows)


def test_validate_positions_shares_must_be_positive():
    rows = [
        {
            "ticker": "2330",
            "shares": "0",
            "avg_cost": "50",
            "source": "broker",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="'shares' must be > 0"):
        validate_positions(rows)


def test_validate_positions_avg_cost_must_be_non_negative():
    rows = [
        {
            "ticker": "2330",
            "shares": "100",
            "avg_cost": "-1",
            "source": "broker",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="'avg_cost' must be >= 0"):
        validate_positions(rows)


def test_validate_positions_priority_must_be_integer():
    rows = [
        {
            "ticker": "2330",
            "shares": "100",
            "avg_cost": "50",
            "source": "broker",
            "priority": "high",
        }
    ]

    with pytest.raises(ValidationError, match="field 'priority' must be an integer"):
        validate_positions(rows)


def test_validate_watchlist_success():
    rows = [
        {
            "ticker": "2454",
            "notes": "watch earnings",
            "tracking_status": "active",
            "priority": "2",
        },
        {
            "ticker": "2317",
            "notes": " ai server ",
            "tracking_status": "",
            "priority": "",
        },
    ]

    result = validate_watchlist(rows)

    assert len(result) == 2
    assert result[0].ticker == "2454"
    assert result[0].notes == "watch earnings"
    assert result[0].tracking_status == "active"
    assert result[0].priority == 2
    assert result[0].sheet_row_index == 2

    assert result[1].ticker == "2317"
    assert result[1].notes == "ai server"
    assert result[1].tracking_status is None
    assert result[1].priority is None
    assert result[1].sheet_row_index == 3


def test_validate_watchlist_duplicate_ticker():
    rows = [
        {
            "ticker": "2454",
            "notes": "a",
            "tracking_status": "active",
            "priority": "1",
        },
        {
            "ticker": "2454",
            "notes": "b",
            "tracking_status": "active",
            "priority": "2",
        },
    ]

    with pytest.raises(ValidationError, match="duplicate ticker '2454' found"):
        validate_watchlist(rows)


def test_validate_watchlist_invalid_ticker():
    rows = [
        {
            "ticker": "xyz",
            "notes": "bad",
            "tracking_status": "active",
            "priority": "1",
        }
    ]

    with pytest.raises(ValidationError, match="ticker 'xyz' is invalid"):
        validate_watchlist(rows)


def test_validate_watchlist_priority_must_be_integer():
    rows = [
        {
            "ticker": "2454",
            "notes": "watch",
            "tracking_status": "active",
            "priority": "urgent",
        }
    ]

    with pytest.raises(ValidationError, match="field 'priority' must be an integer"):
        validate_watchlist(rows)
