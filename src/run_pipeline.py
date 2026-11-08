"""CLI entry point: python -m src.run_pipeline"""
import argparse
import json
from pathlib import Path
from .config import PipelineConfig, ROOT_DIR
from .orchestrator import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the synthetic bank transaction pipeline.")
    parser.add_argument("--accounts", type=Path, default=ROOT_DIR / "data/input/accounts.csv")
    parser.add_argument("--transactions", type=Path, default=ROOT_DIR / "data/input/transactions.csv")
    parser.add_argument("--db", type=Path, default=ROOT_DIR / "data/bank_pipeline.db")
    parser.add_argument("--reports-dir", type=Path, default=ROOT_DIR / "reports")
    parser.add_argument("--alerts-log", type=Path, default=ROOT_DIR / "alerts.log")
    parser.add_argument("--max-reject-rate", type=float, default=0.10)
    parser.add_argument("--task-timeout", type=float, default=30.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--run-id", type=str, default=None)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    cfg = PipelineConfig(
        accounts_path=args.accounts,
        transactions_path=args.transactions,
        db_path=args.db,
        reports_dir=args.reports_dir,
        alerts_path=args.alerts_log,
        max_reject_rate=args.max_reject_rate,
        task_timeout_seconds=args.task_timeout,
        max_attempts=args.max_attempts,
        run_id=args.run_id or PipelineConfig().run_id,
    )
    result = run_pipeline(cfg)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
