CREATE TABLE IF NOT EXISTS positions (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL UNIQUE,
    shares REAL NOT NULL,
    avg_cost REAL NOT NULL,
    source TEXT,
    priority INTEGER,
    last_synced_at TEXT NOT NULL,
    sheet_row_index INTEGER,
    sync_checksum TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_positions_ticker ON positions (ticker);

CREATE TABLE IF NOT EXISTS watchlist (
    watch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL UNIQUE,
    notes TEXT,
    tracking_status TEXT,
    priority INTEGER,
    last_synced_at TEXT NOT NULL,
    sheet_row_index INTEGER,
    sync_checksum TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watchlist_ticker ON watchlist (ticker);

CREATE TABLE IF NOT EXISTS event_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    context_json TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_log_created_at ON event_log (created_at);

CREATE TABLE IF NOT EXISTS sync_runs (
    sync_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_skipped INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs (started_at);
