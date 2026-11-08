"""Reconcile provisional publish results and commit or roll back atomically."""
from decimal import Decimal
import json
from pathlib import Path
from .errors import ReconciliationError


def _summary_totals(conn) -> tuple[Decimal, Decimal]:
    credit = Decimal("0.00")
    debit = Decimal("0.00")
    for row in conn.execute("SELECT credit_total, debit_total FROM daily_account_summary"):
        credit += Decimal(row["credit_total"])
        debit += Decimal(row["debit_total"])
    return credit, debit


def reconcile(state: dict) -> dict:
    cfg = state["config"]
    conn = state.get("publish_connection")
    if conn is None:
        # A quality-gate-blocked run skips publish and therefore does not reconcile.
        raise ReconciliationError("No open publish transaction is available")
    checks = {}
    try:
        expected = state["transformed_transactions"]
        run_count = conn.execute(
            "SELECT COUNT(*) FROM clean_transactions WHERE run_id = ?", (cfg.run_id,)
        ).fetchone()[0]
        checks["published_count_matches"] = run_count == len(expected)

        ids_match = True
        for item in expected:
            actual = conn.execute(
                """SELECT account_id, amount, txn_type, currency, transaction_time, txn_date
                   FROM clean_transactions WHERE transaction_id=?""",
                (item["transaction_id"],),
            ).fetchone()
            if actual is None or any(actual[key] != item[key] for key in
                ("account_id", "amount", "txn_type", "currency", "transaction_time", "txn_date")):
                ids_match = False
                break
        checks["published_values_match"] = ids_match
        checks["no_orphan_accounts"] = conn.execute(
            """SELECT COUNT(*) FROM clean_transactions t
               LEFT JOIN accounts a ON a.account_id=t.account_id WHERE a.account_id IS NULL"""
        ).fetchone()[0] == 0
        # A previously valid transaction remains valid if an account is closed
        # in a later run. Check the rows published by this run, not all history.
        checks["no_transactions_on_closed_accounts"] = conn.execute(
            """SELECT COUNT(*) FROM clean_transactions t JOIN accounts a USING(account_id)
               WHERE a.status='closed' AND t.run_id=?""", (cfg.run_id,)
        ).fetchone()[0] == 0
        checks["input_counts_add_up"] = (
            state["counts"]["total_rows"] == state["counts"]["clean_rows"] + state["counts"]["rejected_count"]
        )

        clean_credit = Decimal("0.00")
        clean_debit = Decimal("0.00")
        for row in conn.execute("SELECT amount, txn_type FROM clean_transactions"):
            if row["txn_type"] == "credit":
                clean_credit += Decimal(row["amount"])
            else:
                clean_debit += Decimal(row["amount"])
        summary_credit, summary_debit = _summary_totals(conn)
        checks["summary_totals_match"] = clean_credit == summary_credit and clean_debit == summary_debit
        checks["summary_transaction_counts_match"] = sum(
            row[0] for row in conn.execute("SELECT transaction_count FROM daily_account_summary")
        ) == conn.execute("SELECT COUNT(*) FROM clean_transactions").fetchone()[0]
        state["reconciliation_report"] = {
            "run_id": cfg.run_id,
            "checks": {name: ("PASS" if passed else "FAIL") for name, passed in checks.items()},
            "clean_credit_total": format(clean_credit, ".2f"),
            "clean_debit_total": format(clean_debit, ".2f"),
            "summary_credit_total": format(summary_credit, ".2f"),
            "summary_debit_total": format(summary_debit, ".2f"),
        }
        failed = [name for name, passed in checks.items() if not passed]
        report_path = Path(cfg.reports_dir) / f"reconciliation_{cfg.run_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(state["reconciliation_report"], indent=2), encoding="utf-8")
        if failed:
            raise ReconciliationError("Reconciliation failed: " + ", ".join(failed))
        conn.commit()
        state["publish_transaction_open"] = False
        return {"row_count": len(expected)}
    except Exception:
        if state.get("publish_transaction_open"):
            conn.rollback()
            state["publish_transaction_open"] = False
        raise
    finally:
        conn.close()
        state["publish_connection"] = None
