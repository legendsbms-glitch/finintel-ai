-- FinIntel AI Database Schema
-- =============================

-- Raw data source table (everything before AI processing)
CREATE TABLE IF NOT EXISTS raw_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,           -- e.g., "openinsider", "fred", "barchart"
    url TEXT,
    raw_content TEXT NOT NULL,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    ticker TEXT,                   -- Nullable for macro data
    data_type TEXT,                 -- "form_4", "options_flow", "wei", etc.
    UNIQUE(source, url, fetched_at)
);

-- AI-scored items (processed data)
CREATE TABLE IF NOT EXISTS scored_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    raw_data_id INTEGER REFERENCES raw_data(id),
    ticker TEXT,
    category TEXT NOT NULL,         -- "Macro", "Equity", "Commodity", "Crypto", "Geopolitical", "IPO", "ICO"
    title TEXT NOT NULL,           -- AI summary (1 line)
    importance_score INTEGER NOT NULL CHECK(importance_score BETWEEN 1 AND 10),
    importance_reason TEXT,       -- Why this score was given
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    display_until DATETIME,     -- When to stop showing on dashboard
    is_archived BOOLEAN DEFAULT 0
);

-- Personal watchlist
CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL UNIQUE,
    asset_class TEXT NOT NULL,     -- "equity", "etf", "crypto", "commodity", "macro"
    added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- IPO/ICO calendar
CREATE TABLE IF NOT EXISTS ipo_ico_calendar (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    launch_date DATE NOT NULL,
    asset_type TEXT NOT NULL,     -- "IPO" or "ICO"
    source TEXT,
    ai_score INTEGER CHECK(ai_score BETWEEN 1 AND 10),
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Cross-signal connections (AI detects links between categories)
CREATE TABLE IF NOT EXISTS cross_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_a_id INTEGER REFERENCES scored_items(id),
    signal_b_id INTEGER REFERENCES scored_items(id),
    connection_type TEXT,         -- "correlated", "causal", "contrarian"
    ai_explanation TEXT,
    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(signal_a_id, signal_b_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_scored_category ON scored_items(category);
CREATE INDEX IF NOT EXISTS idx_scored_score ON scored_items(importance_score DESC);
CREATE INDEX IF NOT EXISTS idx_scored_ticker ON scored_items(ticker);
CREATE INDEX IF NOT EXISTS idx_watchlist_ticker ON watchlist(ticker);

-- Trigger to auto-archive old items after 7 days
CREATE TRIGGER IF NOT EXISTS auto_archive_scored
AFTER INSERT ON scored_items
BEGIN
    UPDATE scored_items 
    SET is_archived = 1 
    WHERE display_until IS NULL 
    AND datetime(fetched_at, '+7 days') < datetime('now');
END;