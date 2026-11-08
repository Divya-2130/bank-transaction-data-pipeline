import csv
from datetime import datetime, timezone
from pathlib import Path
import pytest
from src.config import PipelineConfig
from src.orchestrator import run_pipeline

ACCOUNT_HEADER = ["account_id", "customer_name", "account_type", "status", "opened_date"]
TX_HEADER = ["transaction_id", "account_id", "amount", "txn_type", "currency", "transaction_time"]
DEFAULT_ACCOUNTS = [
    ["ACC00001", "Active Customer", "savings", "active", "2020-01-01"],
    ["ACC00002", "Closed Customer", "current", "closed", "2020-01-01"],
    ["ACC00003", "Frozen Customer", "savings", "frozen", "2020-01-01"],
    ["ACC00004", "Second Active", "current", "active", "2020-01-01"],
]
DEFAULT_TX = ["TXN0000001", "ACC00001", "10.00", "credit", "INR", "2025-01-01T12:00:00Z"]
FIXED_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if header is not None:
            writer.writerow(header)
        writer.writerows(rows)
    return path


@pytest.fixture
def env(tmp_path):
    input_dir = tmp_path / "input"
    account_path = write_csv(input_dir / "accounts.csv", ACCOUNT_HEADER, DEFAULT_ACCOUNTS)
    txn_path = write_csv(input_dir / "transactions.csv", TX_HEADER, [DEFAULT_TX])
    db_path = tmp_path / "db" / "bank.db"
    config = PipelineConfig(
        accounts_path=account_path,
        transactions_path=txn_path,
        db_path=db_path,
        reports_dir=tmp_path / "reports",
        alerts_path=tmp_path / "logs" / "alerts.log",
        max_reject_rate=0.10,
        task_timeout_seconds=2.0,
        max_attempts=3,
        retry_base_delay_seconds=0.0,
        now=lambda: FIXED_NOW,
    )
    return {
        "tmp": tmp_path,
        "accounts": account_path,
        "transactions": txn_path,
        "db": db_path,
        "config": config,
        "run": run_pipeline,
    }
