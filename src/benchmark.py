"""Benchmark each pipeline task and compare row-wise vs batched SQLite inserts."""
import argparse
import csv
from pathlib import Path
import tempfile
import time
from .config import PipelineConfig, ROOT_DIR
from .database import connect
from .generate_data import generate
from .orchestrator import run_pipeline


def _insert_comparison(rows: list[tuple], mode: str) -> float:
    with tempfile.TemporaryDirectory() as temp:
        db = Path(temp) / "bench.db"
        conn = connect(db)
        conn.execute("CREATE TABLE benchmark_rows (transaction_id TEXT PRIMARY KEY, amount TEXT, txn_date TEXT)")
        conn.execute("BEGIN")
        started = time.perf_counter()
        if mode == "rowwise":
            for row in rows:
                conn.execute("INSERT INTO benchmark_rows VALUES (?,?,?)", row)
        else:
            for start in range(0, len(rows), 1000):
                conn.executemany("INSERT INTO benchmark_rows VALUES (?,?,?)", rows[start:start + 1000])
        elapsed = time.perf_counter() - started
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM benchmark_rows").fetchone()[0]
        if count != len(rows):
            raise AssertionError(f"{mode} inserted {count}; expected {len(rows)}")
        conn.close()
        return elapsed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[10000, 50000, 100000, 200000])
    parser.add_argument("--output", type=Path, default=ROOT_DIR / "reports/benchmark.csv")
    parser.add_argument("--seed", type=int, default=20261108)
    args = parser.parse_args(argv)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    task_results = []
    comparison_results = []
    for size in args.sizes:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_dir = root / "input"
            answer_key = root / "answer_key.csv"
            generate(size, args.seed, 0.05, input_dir, answer_key)
            cfg = PipelineConfig(
                accounts_path=input_dir / "accounts.csv", transactions_path=input_dir / "transactions.csv",
                db_path=root / "pipeline.db", reports_dir=root / "reports", alerts_path=root / "alerts.log",
                max_reject_rate=0.06, task_timeout_seconds=120,
            )
            result = run_pipeline(cfg)
            if result["status"] != "succeeded":
                raise RuntimeError(f"Benchmark pipeline failed for {size}: {result['failure_reason']}")
            for task, seconds in result["task_timings"].items():
                task_results.append({"rows": size, "task": task, "seconds": f"{seconds:.6f}"})

            # Use the actual generated clean input values for a fair insert-only comparison.
            with (input_dir / "transactions.csv").open("r", newline="", encoding="utf-8") as handle:
                source = list(csv.DictReader(handle))
            clean_rows = []
            for item in source:
                try:
                    amount = f"{float(item['amount']):.2f}"
                    # Invalid or planted rows are excluded from this isolated insert benchmark.
                    if item["txn_type"] not in {"credit", "debit"} or item["currency"] != "INR":
                        continue
                    if item["account_id"] == "ACC99999" or item["account_id"] == "ACC00002":
                        continue
                    if not item["transaction_id"].startswith("TXN") or not item["amount"].strip():
                        continue
                    clean_rows.append((item["transaction_id"], amount, item["transaction_time"][:10]))
                except (ValueError, KeyError):
                    continue
            # Remove repeated transaction IDs so both insertion strategies receive a valid set.
            unique = {}
            for row in clean_rows:
                unique.setdefault(row[0], row)
            clean_rows = list(unique.values())
            before = _insert_comparison(clean_rows, "rowwise")
            after = _insert_comparison(clean_rows, "batched")
            comparison_results.append({"rows": size, "insert_rows": len(clean_rows),
                                       "rowwise_seconds": f"{before:.6f}",
                                       "batched_seconds": f"{after:.6f}",
                                       "speedup": f"{(before / after if after else 0):.2f}x"})
            print(f"{size:>7} rows: pipeline={result['status']}, rowwise={before:.3f}s, batched={after:.3f}s", flush=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["rows", "task", "seconds"])
        writer.writeheader()
        writer.writerows(task_results)
    comparison_path = args.output.with_name("benchmark_insert_comparison.csv")
    with comparison_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["rows", "insert_rows", "rowwise_seconds", "batched_seconds", "speedup"])
        writer.writeheader()
        writer.writerows(comparison_results)
    print(f"Task timings: {args.output}")
    print(f"Insert comparison: {comparison_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
