"""Run the sample normally, as an idempotent rerun, and with a blocked quality gate."""
import csv
import json
from pathlib import Path
import shutil
from decimal import Decimal
from .config import PipelineConfig, ROOT_DIR
from .database import connect
from .generate_data import generate, TRANSACTION_HEADER
from .orchestrator import run_pipeline
from .verify_labels import verify


def _export_query(conn, query: str, path: Path, headers: list[str] | None = None) -> None:
    rows = conn.execute(query).fetchall()
    names = headers or (list(rows[0].keys()) if rows else [])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(names)
        for row in rows:
            writer.writerow([row[name] for name in names])


def main() -> int:
    output = ROOT_DIR / "sample_output"
    data_dir = ROOT_DIR / "data/input"
    answer_key = ROOT_DIR / "data/answer_key/expected_rejects.csv"
    db_path = output / "sample.db"
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True, exist_ok=True)
    generate(60, 20261108, 0.50, data_dir, answer_key)

    common = dict(accounts_path=data_dir / "accounts.csv", transactions_path=data_dir / "transactions.csv",
                  db_path=db_path, reports_dir=output / "reports", alerts_path=output / "alerts.log")
    normal = run_pipeline(PipelineConfig(**common, max_reject_rate=0.60))
    rerun = run_pipeline(PipelineConfig(**common, max_reject_rate=0.60))

    # A changed file fingerprint forces a new batch; already-published IDs are
    # correctly treated as previously published and the strict gate blocks it.
    blocked_path = output / "blocked_transactions.csv"
    with (data_dir / "transactions.csv").open("r", newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    rows.append(["TXN9999999", "ACC00001", "bad-amount", "credit", "INR", "2025-12-01T12:00:00Z"])
    with blocked_path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)
    blocked = run_pipeline(PipelineConfig(**{**common, "transactions_path": blocked_path}, max_reject_rate=0.10))

    conn = connect(db_path)
    try:
        _export_query(conn, "SELECT * FROM clean_transactions ORDER BY transaction_id", output / "clean_transactions.csv")
        _export_query(conn, "SELECT source_file,row_number,reason,run_id,raw_row FROM rejected_records ORDER BY reject_id", output / "rejected_records.csv")
        _export_query(conn, "SELECT * FROM daily_account_summary ORDER BY account_id,txn_date", output / "daily_account_summary.csv")
        _export_query(conn, "SELECT run_id,status,start_time,end_time,total_rows,clean_rows,rejected_count,failure_reason FROM pipeline_runs ORDER BY start_time", output / "run_status.csv")

        total_in = normal["counts"]["total_rows"]
        normal_clean = conn.execute("SELECT COUNT(*) FROM clean_transactions WHERE run_id=?", (normal["run_id"],)).fetchone()[0]
        normal_rejects = conn.execute("SELECT COUNT(*) FROM rejected_records WHERE run_id=? AND source_file='transactions.csv'", (normal["run_id"],)).fetchone()[0]
        duplicate_ids = conn.execute("SELECT COUNT(*) FROM (SELECT transaction_id FROM clean_transactions GROUP BY transaction_id HAVING COUNT(*)>1)").fetchone()[0]
        orphan_count = conn.execute("""SELECT COUNT(*) FROM clean_transactions t LEFT JOIN accounts a USING(account_id) WHERE a.account_id IS NULL""").fetchone()[0]
        closed_count = conn.execute("""SELECT COUNT(*) FROM clean_transactions t JOIN accounts a USING(account_id) WHERE a.status='closed'""").fetchone()[0]
        summary_credit = sum((Decimal(r[0]) for r in conn.execute("SELECT credit_total FROM daily_account_summary")), Decimal("0.00"))
        summary_debit = sum((Decimal(r[0]) for r in conn.execute("SELECT debit_total FROM daily_account_summary")), Decimal("0.00"))
        clean_credit = sum((Decimal(r[0]) for r in conn.execute("SELECT amount FROM clean_transactions WHERE txn_type='credit'")), Decimal("0.00"))
        clean_debit = sum((Decimal(r[0]) for r in conn.execute("SELECT amount FROM clean_transactions WHERE txn_type='debit'")), Decimal("0.00"))
        total_after_rerun = conn.execute("SELECT COUNT(*) FROM clean_transactions").fetchone()[0]
    finally:
        conn.close()

    labels_ok, labels_details = verify(db_path, answer_key, normal["run_id"])
    with (output / "alerts.log").open("a", encoding="utf-8") as handle:
        pass
    # Blocked run must have a skipped publish task and a CRITICAL quality alert.
    conn = connect(db_path)
    try:
        publish_status = conn.execute("SELECT status FROM task_runs WHERE run_id=? AND task_name='publish'", (blocked["run_id"],)).fetchone()
        publish_skipped = publish_status is not None and publish_status[0] == "skipped"
    finally:
        conn.close()
    alerts_text = (output / "alerts.log").read_text(encoding="utf-8") if (output / "alerts.log").exists() else ""
    checks = {
        "rows_in_equal_clean_plus_rejected": total_in == normal_clean + normal_rejects,
        "no_duplicate_transaction_ids": duplicate_ids == 0,
        "no_orphan_accounts": orphan_count == 0,
        "no_transactions_on_closed_accounts": closed_count == 0,
        "daily_summary_totals_match": summary_credit == clean_credit and summary_debit == clean_debit,
        "rerun_adds_nothing": rerun["status"] == "succeeded" and total_after_rerun == normal_clean,
        "rejects_match_answer_key": labels_ok,
        "blocked_run_publish_skipped": blocked["status"] == "blocked" and publish_skipped,
        "blocked_run_has_critical_alert": '"severity": "CRITICAL"' in alerts_text or '"severity":"CRITICAL"' in alerts_text,
    }
    report = {
        "normal_run": normal,
        "rerun": rerun,
        "blocked_run": blocked,
        "label_verification": labels_details,
        "checks": {key: ("PASS" if value else "FAIL") for key, value in checks.items()},
        "overall": "PASS" if all(checks.values()) else "FAIL",
    }
    (output / "validation_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    for name, status in report["checks"].items():
        print(f"{status}: {name}")
    print(f"Overall: {report['overall']}")
    print(f"Sample output: {output}")
    return 0 if report["overall"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
