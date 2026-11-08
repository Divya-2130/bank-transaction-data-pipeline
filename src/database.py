"""SQLite schema and connection helpers."""
from pathlib import Path
import sqlite3

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK (account_type IN ('savings','current')),
    status TEXT NOT NULL CHECK (status IN ('active','frozen','closed')),
    opened_date TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS clean_transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    amount TEXT NOT NULL,
    txn_type TEXT NOT NULL CHECK (txn_type IN ('credit','debit')),
    currency TEXT NOT NULL CHECK (currency = 'INR'),
    transaction_time TEXT NOT NULL,
    txn_date TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_transactions_account_date
    ON clean_transactions(account_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_clean_transactions_run
    ON clean_transactions(run_id);
CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    run_id TEXT NOT NULL,
    raw_row TEXT NOT NULL,
    UNIQUE(run_id, source_file, row_number)
);
CREATE INDEX IF NOT EXISTS idx_rejected_records_run
    ON rejected_records(run_id);
CREATE TABLE IF NOT EXISTS daily_account_summary (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    txn_date TEXT NOT NULL,
    transaction_count INTEGER NOT NULL,
    credit_total TEXT NOT NULL,
    debit_total TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    PRIMARY KEY(account_id, txn_date)
);
CREATE TABLE IF NOT EXISTS account_status_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_account_status_history_account
    ON account_status_history(account_id, changed_at);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    total_rows INTEGER NOT NULL DEFAULT 0,
    clean_rows INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT,
    config_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
    task_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time TEXT,
    end_time TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    UNIQUE(run_id, task_name)
);
CREATE TABLE IF NOT EXISTS processed_files (
    file_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);
"""


def connect(db_path: Path | str) -> sqlite3.Connection:
    """Create a connection with foreign-key checks enabled."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def initialize_database(db_path: Path | str) -> None:
    """Create all required tables and indexes if they do not exist."""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
