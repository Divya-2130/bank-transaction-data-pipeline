"""Atomic SQLite publisher. Reconciliation owns the final COMMIT decision."""
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import sqlite3
from .database import connect
from .errors import PublishError


def _insert_transactions(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk insert using executemany; UNIQUE(transaction_id) protects idempotency."""
    sql = """INSERT OR IGNORE INTO clean_transactions
        (transaction_id, account_id, amount, txn_type, currency, transaction_time,
         txn_date, run_id, source_file, source_row_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    values = [
        (r["transaction_id"], r["account_id"], r["amount"], r["txn_type"],
         r["currency"], r["transaction_time"], r["txn_date"], state_run_id,
         r["source_file"], r["source_row_number"])
        for r, state_run_id in rows
    ]
    inserted = 0
    for value in values:
        cursor = conn.execute(sql, value)
        inserted += cursor.rowcount
    return inserted


def publish(state: dict) -> dict:
    cfg = state["config"]
    conn = connect(cfg.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = cfg.now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now_text = now.astimezone(timezone.utc).isoformat(timespec="seconds")

        # Accounts are upserted first so the transaction foreign key is satisfied.
        for account in state["valid_accounts"]:
            previous = conn.execute(
                "SELECT status, customer_name, account_type, opened_date FROM accounts WHERE account_id = ?",
                (account["account_id"],),
            ).fetchone()
            old_status = previous["status"] if previous else None
            if previous is None or old_status != account["status"]:
                conn.execute(
                    """INSERT INTO account_status_history
                       (account_id, old_status, new_status, changed_at, run_id, source_file)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (account["account_id"], old_status, account["status"], now_text,
                     cfg.run_id, account["source_file"]),
                )
            conn.execute(
                """INSERT INTO accounts
                   (account_id, customer_name, account_type, status, opened_date, run_id, source_file)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(account_id) DO UPDATE SET
                     customer_name=excluded.customer_name,
                     account_type=excluded.account_type,
                     status=excluded.status,
                     opened_date=excluded.opened_date,
                     run_id=excluded.run_id,
                     source_file=excluded.source_file""",
                (account["account_id"], account["customer_name"], account["account_type"],
                 account["status"], account["opened_date"], cfg.run_id, account["source_file"]),
            )

        transaction_rows = state["transformed_transactions"]
        inserted = _insert_transactions(conn, [(row, cfg.run_id) for row in transaction_rows]) if transaction_rows else 0
        if inserted != len(transaction_rows):
            raise PublishError(
                f"Expected to insert {len(transaction_rows)} transactions, inserted {inserted}; refusing partial publish"
            )

        affected = {(r["account_id"], r["txn_date"]) for r in transaction_rows}
        for account_id, txn_date in affected:
            rows = conn.execute(
                "SELECT amount, txn_type FROM clean_transactions WHERE account_id=? AND txn_date=?",
                (account_id, txn_date),
            ).fetchall()
            credit = sum((Decimal(r["amount"]) for r in rows if r["txn_type"] == "credit"), Decimal("0.00"))
            debit = sum((Decimal(r["amount"]) for r in rows if r["txn_type"] == "debit"), Decimal("0.00"))
            conn.execute(
                """INSERT INTO daily_account_summary
                   (account_id, txn_date, transaction_count, credit_total, debit_total, run_id, source_file)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(account_id, txn_date) DO UPDATE SET
                     transaction_count=excluded.transaction_count,
                     credit_total=excluded.credit_total,
                     debit_total=excluded.debit_total,
                     run_id=excluded.run_id,
                     source_file=excluded.source_file""",
                (account_id, txn_date, len(rows), format(credit, ".2f"), format(debit, ".2f"),
                 cfg.run_id, state["files"]["transactions"]["source_file"]),
            )

        # File fingerprints are committed together with the data, never before it.
        for item in state["files"].values():
            conn.execute(
                "INSERT OR IGNORE INTO processed_files(file_hash, source_file, processed_at, run_id) VALUES (?, ?, ?, ?)",
                (item["file_hash"], item["source_file"], now_text, cfg.run_id),
            )
        state["publish_connection"] = conn
        state["publish_inserted_count"] = inserted
        state["publish_transaction_open"] = True
        return {"row_count": inserted}
    except Exception:
        try:
            conn.rollback()
        finally:
            conn.close()
        raise
