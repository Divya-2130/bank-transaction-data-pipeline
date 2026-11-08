"""Display recent pipeline runs and task details."""
import argparse
from pathlib import Path
from .config import ROOT_DIR
from .database import connect


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Show pipeline run status.")
    parser.add_argument("--db", type=Path, default=ROOT_DIR / "data/bank_pipeline.db")
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    if not args.db.exists():
        print(f"Database not found: {args.db}")
        return 1
    conn = connect(args.db)
    try:
        if args.run_id:
            runs = conn.execute("SELECT * FROM pipeline_runs WHERE run_id=?", (args.run_id,)).fetchall()
        else:
            runs = conn.execute("SELECT * FROM pipeline_runs ORDER BY start_time DESC LIMIT 20").fetchall()
        if not runs:
            print("No pipeline runs found.")
            return 0
        for run in runs:
            print(f"\nRun: {run['run_id']} | status={run['status']} | started={run['start_time']}")
            print(f"Rows: input={run['total_rows']} clean={run['clean_rows']} rejected={run['rejected_count']}")
            if run["failure_reason"]:
                print(f"Failure: {run['failure_reason']}")
            tasks = conn.execute(
                "SELECT task_name,status,attempts,row_count,rejected_count,error FROM task_runs WHERE run_id=? ORDER BY task_run_id",
                (run["run_id"],),
            ).fetchall()
            for task in tasks:
                print(f"  {task['task_name']:<18} {task['status']:<10} attempts={task['attempts']} rows={task['row_count']} rejected={task['rejected_count']}")
                if task["error"]:
                    print(f"      {task['error']}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
