"""Compare a selected pipeline run's transaction rejects with the separate answer key."""
import argparse
import csv
from pathlib import Path
from .config import ROOT_DIR
from .database import connect


def verify(db_path: Path, answer_key: Path, run_id: str) -> tuple[bool, list[str]]:
    with answer_key.open("r", newline="", encoding="utf-8-sig") as handle:
        expected = {
            (row["source_file"], int(row["row_number"]), row["expected_reason"])
            for row in csv.DictReader(handle)
        }
    conn = connect(db_path)
    try:
        actual = {
            (row["source_file"], row["row_number"], row["reason"])
            for row in conn.execute(
                "SELECT source_file,row_number,reason FROM rejected_records WHERE run_id=? AND source_file LIKE '%transactions.csv'",
                (run_id,),
            )
        }
    finally:
        conn.close()
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    details = []
    if missing:
        details.append(f"Missing rejects: {missing[:10]}")
    if unexpected:
        details.append(f"Unexpected rejects: {unexpected[:10]}")
    if expected == actual:
        details.append(f"PASS: {len(expected)} answer-key rejects match exactly for run {run_id}")
        return True, details
    details.insert(0, "FAIL: answer key and pipeline rejects differ")
    return False, details


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT_DIR / "data/bank_pipeline.db")
    parser.add_argument("--answer-key", type=Path, default=ROOT_DIR / "data/answer_key/expected_rejects.csv")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    passed, details = verify(args.db, args.answer_key, args.run_id)
    for line in details:
        print(line)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
