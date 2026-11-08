import csv
import json
import time
from decimal import Decimal
from pathlib import Path

import pytest

from src.config import PipelineConfig
from src.database import connect
from src.errors import ReconciliationError, TaskTimeout, TransientError
from src.generate_data import generate, TRANSACTION_HEADER, ACCOUNT_HEADER
from src.orchestrator import Orchestrator, Task, run_pipeline
from src.reconciliation import reconcile
from src.verify_labels import verify
from conftest import DEFAULT_TX, FIXED_NOW, TX_HEADER, ACCOUNT_HEADER as ACCOUNT_COLS, write_csv


def _write_tx(env, rows, header=None, filename="transactions.csv"):
    path = env["tmp"] / "input" / filename
    write_csv(path, TX_HEADER if header is None else header, rows)
    env["config"].transactions_path = path
    return path


def _query(db, sql, params=()):
    conn = connect(db)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def test_happy_path(env):
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert result["counts"]["total_rows"] == 1
    assert result["counts"]["clean_rows"] == 1
    assert result["counts"]["rejected_count"] == 0
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 1


def test_empty_batch_is_valid(env):
    _write_tx(env, [])
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert result["counts"]["total_rows"] == 0
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 0


def test_zero_byte_file_is_schema_failure(env):
    path = env["tmp"] / "input" / "zero.csv"
    path.write_bytes(b"")
    env["config"].transactions_path = path
    result = env["run"](env["config"])
    assert result["status"] == "failed"
    assert result["tasks"]["validate_schema"] == "failed"
    assert result["tasks"]["publish"] == "skipped"


def test_malformed_row_field_count_is_quarantined(env):
    _write_tx(env, [DEFAULT_TX, ["TXN0000002", "ACC00001", "5.00"]])
    env["config"].max_reject_rate = 0.5
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    reject = _query(env["db"], "SELECT row_number,reason,raw_row FROM rejected_records")[0]
    assert reject[0] == 2
    assert reject[1] == "malformed_row_field_count"
    assert json.loads(reject[2]) == ["TXN0000002", "ACC00001", "5.00"]


@pytest.mark.parametrize("field", TX_HEADER)
def test_every_transaction_required_field_empty(env, field):
    row = list(DEFAULT_TX)
    row[TX_HEADER.index(field)] = ""
    _write_tx(env, [row])
    env["config"].max_reject_rate = 1.0
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    reason = _query(env["db"], "SELECT reason FROM rejected_records")[0][0]
    assert reason == f"missing_required_field:{field}"


@pytest.mark.parametrize(
    "change,expected_reason",
    [
        (lambda r: r.__setitem__(2, "abc"), "invalid_amount"),
        (lambda r: r.__setitem__(3, "transfer"), "invalid_txn_type"),
        (lambda r: r.__setitem__(4, "USD"), "unsupported_currency"),
        (lambda r: r.__setitem__(5, "not-a-time"), "invalid_timestamp"),
        (lambda r: r.__setitem__(5, "2027-01-01T00:00:00Z"), "future_transaction_time"),
        (lambda r: r.__setitem__(1, "ACC99999"), "unknown_account"),
        (lambda r: r.__setitem__(1, "ACC00002"), "closed_account"),
    ],
)
def test_invalid_transaction_values(env, change, expected_reason):
    row = list(DEFAULT_TX)
    change(row)
    _write_tx(env, [row])
    env["config"].max_reject_rate = 1.0
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert _query(env["db"], "SELECT reason FROM rejected_records")[0][0] == expected_reason


def test_duplicate_and_conflicting_duplicate_first_wins(env):
    duplicate = list(DEFAULT_TX)
    conflict = list(DEFAULT_TX)
    conflict[2] = "15.00"
    _write_tx(env, [DEFAULT_TX, duplicate, conflict])
    env["config"].max_reject_rate = 0.67
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    reasons = [r[0] for r in _query(env["db"], "SELECT reason FROM rejected_records ORDER BY row_number")]
    assert reasons == ["duplicate_transaction_id", "conflicting_transaction_id"]
    assert _query(env["db"], "SELECT amount FROM clean_transactions")[0][0] == "10.00"


@pytest.mark.parametrize(
    "header",
    [
        ["transaction_id", "account_id", "amount", "txn_type", "currency", "wrong_name"],
        TX_HEADER + ["unexpected"],
    ],
)
def test_renamed_or_extra_transaction_column_stops_run(env, header):
    _write_tx(env, [DEFAULT_TX], header=header)
    result = env["run"](env["config"])
    assert result["status"] == "failed"
    assert result["tasks"]["validate_schema"] == "failed"
    assert result["tasks"]["publish"] == "skipped"
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 0


def test_extra_account_column_is_schema_failure(env):
    path = env["tmp"] / "input" / "accounts_extra.csv"
    write_csv(path, ACCOUNT_COLS + ["extra"], [row + ["x"] for row in [
        ["ACC00001", "A", "savings", "active", "2020-01-01"]
    ]])
    env["config"].accounts_path = path
    result = env["run"](env["config"])
    assert result["status"] == "failed"
    assert result["tasks"]["validate_schema"] == "failed"


@pytest.mark.parametrize("field", ACCOUNT_COLS)
def test_every_account_required_field_empty_is_rejected(env, field):
    path = env["tmp"] / "input" / "accounts_empty.csv"
    account = ["ACC00001", "Active", "savings", "active", "2020-01-01"]
    account[ACCOUNT_COLS.index(field)] = ""
    write_csv(path, ACCOUNT_COLS, [account])
    env["config"].accounts_path = path
    env["config"].max_reject_rate = 1.0
    # The transaction also correctly becomes unknown because its account is invalid.
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    reasons = {r[0] for r in _query(env["db"], "SELECT reason FROM rejected_records")}
    assert f"missing_required_field:{field}" in reasons


def test_frozen_account_can_receive_credit(env):
    row = list(DEFAULT_TX)
    row[1] = "ACC00003"
    _write_tx(env, [row])
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 1


def test_frozen_account_debit_is_rejected(env):
    row = list(DEFAULT_TX)
    row[1] = "ACC00003"
    row[3] = "debit"
    _write_tx(env, [row])
    env["config"].max_reject_rate = 1.0
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert _query(env["db"], "SELECT reason FROM rejected_records")[0][0] == "frozen_account_debit"


def test_utc_offset_can_cross_date_boundary(env):
    row = list(DEFAULT_TX)
    row[5] = "2025-01-02T00:30:00+02:00"
    _write_tx(env, [row])
    env["run"](env["config"])
    result = _query(env["db"], "SELECT transaction_time,txn_date FROM clean_transactions")[0]
    assert result[0] == "2025-01-01T22:30:00Z"
    assert result[1] == "2025-01-01"


def test_sub_paisa_amount_rounds_to_zero_and_is_rejected(env):
    row = list(DEFAULT_TX)
    row[2] = "0.004"
    _write_tx(env, [row])
    env["config"].max_reject_rate = 1.0
    env["run"](env["config"])
    assert _query(env["db"], "SELECT reason FROM rejected_records")[0][0] == "amount_not_positive_after_rounding"


def test_late_and_out_of_order_rows_keep_true_dates(env):
    first = list(DEFAULT_TX)
    first[5] = "2025-01-10T12:00:00Z"
    second = list(DEFAULT_TX)
    second[0] = "TXN0000002"
    second[5] = "2025-01-02T12:00:00Z"
    _write_tx(env, [first, second])
    env["config"].max_reject_rate = 0.0
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    rows = _query(env["db"], "SELECT transaction_id,txn_date FROM clean_transactions ORDER BY source_row_number")
    assert [(r[0], r[1]) for r in rows] == [("TXN0000001", "2025-01-10"), ("TXN0000002", "2025-01-02")]


def test_retry_until_success_and_growing_delay(env):
    calls, delays = {"count": 0}, []
    def flaky(_state):
        calls["count"] += 1
        if calls["count"] < 3:
            raise TransientError("temporary lock")
        return {"row_count": 7}
    task = Task("flaky", flaky)
    env["config"].retry_base_delay_seconds = 0.1
    orchestrator = Orchestrator(env["config"], tasks=[task], sleep=delays.append)
    outcome = orchestrator._execute(task, {})
    assert calls["count"] == 3
    assert outcome["attempts"] == 3
    assert delays == [0.1, 0.2]


def test_retries_exhausted_report_attempt_count(env):
    def always_fails(_state):
        raise TransientError("still locked")
    task = Task("flaky", always_fails)
    orchestrator = Orchestrator(env["config"], tasks=[task], sleep=lambda _seconds: None)
    with pytest.raises(TransientError) as exc:
        orchestrator._execute(task, {})
    assert exc.value.pipeline_attempts == 3


def test_quality_gate_and_schema_errors_are_not_retried(env):
    calls = {"count": 0}
    def always_transient(_state):
        calls["count"] += 1
        raise TransientError("do not retry critical gate")
    task = Task("quality_gate", always_transient)
    orchestrator = Orchestrator(env["config"], tasks=[task], sleep=lambda _seconds: None)
    with pytest.raises(TransientError):
        orchestrator._execute(task, {})
    assert calls["count"] == 1


def test_task_timeout_is_recorded(env):
    def slow(_state):
        time.sleep(0.08)
        return {}
    task = Task("slow", slow)
    env["config"].task_timeout_seconds = 0.01
    orchestrator = Orchestrator(env["config"], tasks=[task])
    with pytest.raises(TaskTimeout):
        orchestrator._execute(task, {})
    time.sleep(0.09)  # Let the daemon worker finish before the temporary test ends.


def test_dependency_failure_skips_dependents(env):
    def fail(_state):
        raise ValueError("boom")
    tasks = [Task("first", fail), Task("second", lambda _state: {}, ("first",))]
    result = Orchestrator(env["config"], tasks=tasks).run()
    assert result["tasks"] == {"first": "failed", "second": "skipped"}


def test_quality_gate_blocks_publish_but_keeps_rejects(env):
    bad = list(DEFAULT_TX)
    bad[2] = "bad"
    _write_tx(env, [DEFAULT_TX, bad])
    env["config"].max_reject_rate = 0.10
    result = env["run"](env["config"])
    assert result["status"] == "blocked"
    assert result["tasks"]["quality_gate"] == "failed"
    assert result["tasks"]["publish"] == "skipped"
    assert _query(env["db"], "SELECT COUNT(*) FROM rejected_records")[0][0] == 1
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 0
    alert = env["config"].alerts_path.read_text(encoding="utf-8")
    assert '"severity": "CRITICAL"' in alert


def test_reject_rate_exactly_at_limit_passes(env):
    bad = list(DEFAULT_TX)
    bad[2] = "oops"
    _write_tx(env, [DEFAULT_TX, bad])
    env["config"].max_reject_rate = 0.5
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert result["counts"]["reject_rate"] == 0.5


def test_same_file_rerun_is_a_safe_noop(env):
    first = env["run"](env["config"])
    before = _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0]
    env["config"].run_id = "safe-rerun"
    second = env["run"](env["config"])
    after = _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0]
    assert first["status"] == second["status"] == "succeeded"
    assert second["counts"]["total_rows"] == 0
    assert before == after == 1
    assert _query(env["db"], "SELECT COUNT(*) FROM rejected_records WHERE run_id=?", (second["run_id"],))[0][0] == 0


def test_restart_after_quality_failure_can_process_same_files(env):
    bad = list(DEFAULT_TX)
    bad[2] = "wrong"
    _write_tx(env, [DEFAULT_TX, bad])
    env["config"].max_reject_rate = 0.0
    blocked = env["run"](env["config"])
    assert blocked["status"] == "blocked"
    env["config"].run_id = "second-attempt-after-fix"
    env["config"].max_reject_rate = 0.5
    succeeded = env["run"](env["config"])
    assert succeeded["status"] == "succeeded"
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 1


def test_account_status_changes_are_kept_as_history(env):
    env["run"](env["config"])
    path = env["tmp"] / "input" / "accounts_changed.csv"
    changed_accounts = [
        ["ACC00001", "Active Customer", "savings", "frozen", "2020-01-01"],
        ["ACC00002", "Closed Customer", "current", "closed", "2020-01-01"],
        ["ACC00003", "Frozen Customer", "savings", "frozen", "2020-01-01"],
        ["ACC00004", "Second Active", "current", "active", "2020-01-01"],
    ]
    write_csv(path, ACCOUNT_COLS, changed_accounts)
    env["config"].accounts_path = path
    _write_tx(env, [])
    env["config"].run_id = "status-change-run"
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    history = _query(env["db"], "SELECT old_status,new_status FROM account_status_history WHERE account_id='ACC00001' ORDER BY history_id")
    assert [(r[0], r[1]) for r in history] == [(None, "active"), ("active", "frozen")]


def test_reconciliation_failure_rolls_back_provisional_publish(env):
    from src.orchestrator import ingest, validate_schema, validate_rules, quarantine, transform, quality_gate, publish
    def fail_reconciliation(_state):
        raise ReconciliationError("forced mismatch for atomicity test")
    tasks = [
        Task("ingest", ingest), Task("validate_schema", validate_schema, ("ingest",)),
        Task("validate_rules", validate_rules, ("validate_schema",)),
        Task("quarantine", quarantine, ("validate_rules",)), Task("transform", transform, ("quarantine",)),
        Task("quality_gate", quality_gate, ("transform",)), Task("publish", publish, ("quality_gate",)),
        Task("reconcile", fail_reconciliation, ("publish",)),
    ]
    result = Orchestrator(env["config"], tasks=tasks).run()
    assert result["status"] == "failed"
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 0
    assert _query(env["db"], "SELECT COUNT(*) FROM accounts")[0][0] == 0
    assert _query(env["db"], "SELECT COUNT(*) FROM processed_files")[0][0] == 0


def test_reconciliation_detects_missing_rows_and_rolls_back(env):
    from src.orchestrator import ingest, validate_schema, validate_rules, quarantine, transform, quality_gate, publish
    def corrupt_then_reconcile(state):
        state["publish_connection"].execute("DELETE FROM clean_transactions WHERE transaction_id='TXN0000001'")
        return reconcile(state)
    tasks = [
        Task("ingest", ingest), Task("validate_schema", validate_schema, ("ingest",)),
        Task("validate_rules", validate_rules, ("validate_schema",)), Task("quarantine", quarantine, ("validate_rules",)),
        Task("transform", transform, ("quarantine",)), Task("quality_gate", quality_gate, ("transform",)),
        Task("publish", publish, ("quality_gate",)), Task("reconcile", corrupt_then_reconcile, ("publish",)),
    ]
    result = Orchestrator(env["config"], tasks=tasks).run()
    assert result["status"] == "failed"
    assert "published_count_matches" in result["failure_reason"]
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 0


def test_end_to_end_sample_matches_answer_key(tmp_path):
    input_dir = tmp_path / "input"
    answer_key = tmp_path / "answer_key.csv"
    generated = generate(60, 12345, 0.50, input_dir, answer_key)
    cfg = PipelineConfig(
        accounts_path=input_dir / "accounts.csv", transactions_path=input_dir / "transactions.csv",
        db_path=tmp_path / "bank.db", reports_dir=tmp_path / "reports", alerts_path=tmp_path / "alerts.log",
        max_reject_rate=0.60, now=lambda: FIXED_NOW,
    )
    result = run_pipeline(cfg)
    assert result["status"] == "succeeded"
    passed, details = verify(cfg.db_path, answer_key, result["run_id"])
    assert passed, details
    assert generated["expected_rejects"] == 30


def test_generator_reproducibility(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    key1, key2 = tmp_path / "key1.csv", tmp_path / "key2.csv"
    generate(60, 999, 0.50, first, key1)
    generate(60, 999, 0.50, second, key2)
    assert (first / "accounts.csv").read_bytes() == (second / "accounts.csv").read_bytes()
    assert (first / "transactions.csv").read_bytes() == (second / "transactions.csv").read_bytes()
    assert key1.read_bytes() == key2.read_bytes()


def test_batched_transaction_insert_matches_rowwise_reference(tmp_path):
    from src.publisher import _insert_transactions
    from src.database import connect
    rows = [
        {"transaction_id": f"TXN{i:07d}", "account_id": "ACC00001", "amount": f"{i + 1}.00",
         "txn_type": "credit" if i % 2 else "debit", "currency": "INR",
         "transaction_time": "2025-01-01T12:00:00Z", "txn_date": "2025-01-01",
         "source_file": "transactions.csv", "source_row_number": i + 1}
        for i in range(1, 2050)
    ]
    db_batch = tmp_path / "batch.db"
    db_reference = tmp_path / "reference.db"
    for db in (db_batch, db_reference):
        conn = connect(db)
        conn.execute("CREATE TABLE clean_transactions (transaction_id TEXT PRIMARY KEY, account_id TEXT, amount TEXT, txn_type TEXT, currency TEXT, transaction_time TEXT, txn_date TEXT, run_id TEXT, source_file TEXT, source_row_number INTEGER)")
        conn.close()

    conn = connect(db_batch)
    conn.execute("BEGIN")
    inserted = _insert_transactions(conn, [(row, "test-run") for row in rows])
    conn.commit()
    batch_result = conn.execute("SELECT * FROM clean_transactions ORDER BY transaction_id").fetchall()
    conn.close()

    conn = connect(db_reference)
    conn.execute("BEGIN")
    sql = "INSERT OR IGNORE INTO clean_transactions VALUES (?,?,?,?,?,?,?,?,?,?)"
    for row in rows:
        conn.execute(sql, (row["transaction_id"], row["account_id"], row["amount"], row["txn_type"],
                           row["currency"], row["transaction_time"], row["txn_date"], "test-run",
                           row["source_file"], row["source_row_number"]))
    conn.commit()
    reference_result = conn.execute("SELECT * FROM clean_transactions ORDER BY transaction_id").fetchall()
    conn.close()

    assert inserted == len(rows)
    assert [tuple(r) for r in batch_result] == [tuple(r) for r in reference_result]

@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda a: a.__setitem__(0, "BADID"), "invalid_account_id"),
        (lambda a: a.__setitem__(2, "premium"), "invalid_account_type"),
        (lambda a: a.__setitem__(3, "suspended"), "invalid_account_status"),
        (lambda a: a.__setitem__(4, "not-a-date"), "invalid_opened_date"),
    ],
)
def test_invalid_account_values_are_quarantined(env, mutate, expected):
    account = ["ACC00001", "Active Customer", "savings", "active", "2020-01-01"]
    mutate(account)
    path = env["tmp"] / "input" / "bad_account.csv"
    write_csv(path, ACCOUNT_COLS, [account])
    env["config"].accounts_path = path
    env["config"].max_reject_rate = 1.0
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    reasons = {row[0] for row in _query(env["db"], "SELECT reason FROM rejected_records")}
    assert expected in reasons


def test_invalid_transaction_id_format_is_rejected(env):
    row = list(DEFAULT_TX)
    row[0] = "BAD-TXN"
    _write_tx(env, [row])
    env["config"].max_reject_rate = 1.0
    env["run"](env["config"])
    assert _query(env["db"], "SELECT reason FROM rejected_records")[0][0] == "invalid_transaction_id"


def test_closing_account_does_not_invalidate_historical_transactions(env):
    first = env["run"](env["config"])
    assert first["status"] == "succeeded"
    changed = [
        ["ACC00001", "Active Customer", "savings", "closed", "2020-01-01"],
        ["ACC00002", "Closed Customer", "current", "closed", "2020-01-01"],
        ["ACC00003", "Frozen Customer", "savings", "frozen", "2020-01-01"],
        ["ACC00004", "Second Active", "current", "active", "2020-01-01"],
    ]
    account_path = env["tmp"] / "input" / "accounts_closed.csv"
    write_csv(account_path, ACCOUNT_COLS, changed)
    env["config"].accounts_path = account_path
    _write_tx(env, [])
    env["config"].run_id = "close-account-later"
    result = env["run"](env["config"])
    assert result["status"] == "succeeded"
    assert _query(env["db"], "SELECT status FROM accounts WHERE account_id='ACC00001'")[0][0] == "closed"
    assert _query(env["db"], "SELECT COUNT(*) FROM clean_transactions")[0][0] == 1


def test_retry_attempt_count_is_saved_in_task_runs(env):
    calls = {"count": 0}
    def retry_then_success(_state):
        calls["count"] += 1
        if calls["count"] < 3:
            raise TransientError("temporary lock")
        return {"row_count": 1}
    task = Task("retry_task", retry_then_success)
    result = Orchestrator(env["config"], tasks=[task], sleep=lambda _seconds: None).run()
    assert result["status"] == "succeeded"
    saved = _query(env["db"], "SELECT attempts,status FROM task_runs WHERE task_name='retry_task'")[0]
    assert (saved[0], saved[1]) == (3, "succeeded")


def test_quality_gate_fails_when_counts_do_not_add_up(env):
    from src.quality_gate import quality_gate
    from src.errors import QualityGateError
    state = {
        "config": env["config"],
        "counts": {"total_rows": 5, "clean_rows": 3, "rejected_count": 1},
    }
    with pytest.raises(QualityGateError, match="do not add up"):
        quality_gate(state)
