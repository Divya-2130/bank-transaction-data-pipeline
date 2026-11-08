"""Generate deterministic synthetic accounts, transactions, and a separate answer key."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
from decimal import Decimal
from .config import ROOT_DIR

ACCOUNT_HEADER = ["account_id", "customer_name", "account_type", "status", "opened_date"]
TRANSACTION_HEADER = ["transaction_id", "account_id", "amount", "txn_type", "currency", "transaction_time"]
LABEL_REASON = {
    "missing_field": "missing_required_field:amount",
    "bad_amount": "invalid_amount",
    "bad_txn_type": "invalid_txn_type",
    "bad_currency": "unsupported_currency",
    "bad_timestamp": "invalid_timestamp",
    "future_date": "future_transaction_time",
    "unknown_account": "unknown_account",
    "closed_account": "closed_account",
    "exact_duplicate": "duplicate_transaction_id",
    "conflicting_duplicate": "conflicting_transaction_id",
}


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def generate(rows: int, seed: int, bad_rate: float, out_dir: Path, answer_key: Path) -> dict:
    if rows < 20:
        raise ValueError("rows must be at least 20 so all planted issue types can be represented")
    if not 0 <= bad_rate < 0.9:
        raise ValueError("bad_rate must be in [0, 0.9)")
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    accounts = []
    for i in range(1, 21):
        status = "closed" if i == 2 else ("frozen" if i == 3 else "active")
        accounts.append([f"ACC{i:05d}", f"Synthetic Customer {i:02d}", "savings" if i % 2 else "current", status, "2020-01-01"])
    _write_csv(out_dir / "accounts.csv", ACCOUNT_HEADER, accounts)

    bad_count = 0 if bad_rate == 0 else max(10, round(rows * bad_rate))
    bad_count = min(bad_count, rows - 10)
    anomaly_count = bad_count * 4 // 5
    duplicate_count = bad_count - anomaly_count
    base_count = rows - bad_count
    # Keep each planted type represented at small sizes, and spread large batches evenly.
    anomaly_each = [anomaly_count // 8] * 8
    for i in range(anomaly_count % 8):
        anomaly_each[i] += 1
    duplicate_each = duplicate_count // 2
    conflict_each = duplicate_count - duplicate_each

    base_rows: list[tuple[list[str], str | None]] = []
    for i in range(1, base_count + 1):
        account_number = rng.choice([1, 3, *range(4, 21)])
        txn_type = "credit" if account_number == 3 else rng.choice(["credit", "debit"])
        day = rng.randint(1, 365)
        moment = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=day - 1, seconds=rng.randint(0, 86399))
        row = [
            f"TXN{i:07d}", f"ACC{account_number:05d}",
            format(Decimal(rng.randint(100, 250000)) / Decimal("100"), ".2f"), txn_type, "INR",
            moment.isoformat(timespec="seconds").replace("+00:00", "Z"),
        ]
        base_rows.append((row, None))
    rng.shuffle(base_rows)  # Transaction times are also random, so arrival order is not chronological.

    anomalies: list[tuple[list[str], str | None]] = []
    anomaly_specs = [
        ("missing_field", lambda r: r.__setitem__(2, "")),
        ("bad_amount", lambda r: r.__setitem__(2, "not-a-number")),
        ("bad_txn_type", lambda r: r.__setitem__(3, "transfer")),
        ("bad_currency", lambda r: r.__setitem__(4, "USD")),
        ("bad_timestamp", lambda r: r.__setitem__(5, "not-a-date")),
        ("future_date", lambda r: r.__setitem__(5, "2099-12-31T23:59:59Z")),
        ("unknown_account", lambda r: r.__setitem__(1, "ACC99999")),
        ("closed_account", lambda r: r.__setitem__(1, "ACC00002")),
    ]
    next_id = base_count + 1
    for (label, mutator), count in zip(anomaly_specs, anomaly_each):
        for _ in range(count):
            account_number = rng.choice([1, 4, 5, 6])
            moment = datetime(2025, 6, 1, tzinfo=timezone.utc) + timedelta(minutes=rng.randint(0, 100000))
            row = [f"TXN{next_id:07d}", f"ACC{account_number:05d}", "123.45", "credit", "INR",
                   moment.isoformat(timespec="seconds").replace("+00:00", "Z")]
            mutator(row)
            anomalies.append((row, label))
            next_id += 1
    rng.shuffle(anomalies)

    # Duplicate rows are appended after their first occurrence, so the answer key
    # always identifies the later duplicate/conflict regardless of the shuffle.
    duplicates: list[tuple[list[str], str | None]] = []
    anchors = [row for row, _ in base_rows]
    for i in range(duplicate_each):
        anchor = list(anchors[i % len(anchors)])
        duplicates.append((anchor, "exact_duplicate"))
    for i in range(conflict_each):
        anchor = list(anchors[(i + duplicate_each) % len(anchors)])
        anchor[2] = format(Decimal(anchor[2]) + Decimal("17.31"), ".2f")
        duplicates.append((anchor, "conflicting_duplicate"))
    rng.shuffle(duplicates)

    final = base_rows + anomalies + duplicates
    # Preserve the base/anomaly shuffle but interleave the appended duplicate rows
    # at different positions after all original IDs have appeared.
    final_rows = [row for row, _ in final]
    final_labels = [label for _, label in final]
    txn_path = out_dir / "transactions.csv"
    _write_csv(txn_path, TRANSACTION_HEADER, final_rows)
    key_rows = []
    for row_number, (row, label) in enumerate(zip(final_rows, final_labels), start=1):
        if label:
            key_rows.append([txn_path.name, row_number, label, LABEL_REASON[label]])
    _write_csv(answer_key, ["source_file", "row_number", "label", "expected_reason"], key_rows)
    return {"accounts": len(accounts), "transactions": len(final_rows), "expected_rejects": len(key_rows),
            "good_rows": base_count, "accounts_path": str(out_dir / "accounts.csv"),
            "transactions_path": str(txn_path), "answer_key_path": str(answer_key)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20261108)
    parser.add_argument("--bad-rate", type=float, default=None)
    parser.add_argument("--out-dir", type=Path, default=ROOT_DIR / "data/input")
    parser.add_argument("--answer-key", type=Path, default=ROOT_DIR / "data/answer_key/expected_rejects.csv")
    args = parser.parse_args(argv)
    rate = args.bad_rate if args.bad_rate is not None else (0.50 if args.rows < 10000 else 0.05)
    result = generate(args.rows, args.seed, rate, args.out_dir, args.answer_key)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
