"""Small dependency-aware task runner with transient retries and honest timeouts."""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
import threading
import time
from typing import Callable
from .database import connect, initialize_database
from .errors import TaskTimeout, TransientError
from .ingest import ingest
from .validation import validate_schema, validate_rules
from .quarantine import quarantine
from .transform import transform
from .quality_gate import quality_gate
from .publisher import publish
from .reconciliation import reconcile

CRITICAL_TASKS = {"validate_schema", "quality_gate"}

@dataclass
class Task:
    name: str
    func: Callable[[dict], dict]
    dependencies: tuple[str, ...] = ()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Orchestrator:
    def __init__(self, config, tasks: list[Task] | None = None, sleep: Callable[[float], None] = time.sleep):
        self.config = config
        self.tasks = tasks or [
            Task("ingest", ingest),
            Task("validate_schema", validate_schema, ("ingest",)),
            Task("validate_rules", validate_rules, ("validate_schema",)),
            Task("quarantine", quarantine, ("validate_rules",)),
            Task("transform", transform, ("quarantine",)),
            Task("quality_gate", quality_gate, ("transform",)),
            Task("publish", publish, ("quality_gate",)),
            Task("reconcile", reconcile, ("publish",)),
        ]
        self.sleep = sleep

    def _run_with_timeout(self, task: Task, state: dict) -> dict:
        result: dict = {}
        error: list[BaseException] = []
        finished = threading.Event()

        def target() -> None:
            try:
                result.update(task.func(state) or {})
            except BaseException as exc:  # captured and re-raised in the orchestration thread
                error.append(exc)
            finally:
                finished.set()

        worker = threading.Thread(target=target, name=f"pipeline-{task.name}", daemon=True)
        worker.start()
        worker.join(self.config.task_timeout_seconds)
        if not finished.is_set():
            # Python cannot safely force-kill a running thread. The daemon worker may
            # finish later; tasks should therefore be short and side-effect-safe.
            raise TaskTimeout(
                f"Task '{task.name}' exceeded {self.config.task_timeout_seconds:.2f}s; "
                "Python cannot force-kill the worker thread"
            )
        if error:
            raise error[0]
        return result

    def _execute(self, task: Task, state: dict) -> dict:
        attempts = 0
        last_error = None
        while attempts < self.config.max_attempts:
            attempts += 1
            try:
                result = self._run_with_timeout(task, state)
                return {"result": result, "attempts": attempts}
            except BaseException as exc:
                last_error = exc
                transient = isinstance(exc, TransientError) or (
                    isinstance(exc, sqlite3.OperationalError) and
                    ("locked" in str(exc).lower() or "busy" in str(exc).lower())
                )
                if not transient or task.name in {"validate_schema", "quality_gate", "reconcile"}:
                    break
                if attempts < self.config.max_attempts:
                    delay = self.config.retry_base_delay_seconds * (2 ** (attempts - 1))
                    self.sleep(delay)
        try:
            setattr(last_error, "pipeline_attempts", attempts)
        except Exception:
            pass
        raise last_error

    def _alert(self, task_name: str, error: BaseException, critical: bool) -> None:
        severity = "CRITICAL" if critical else "ERROR"
        payload = {
            "timestamp": _utc_now(), "severity": severity, "run_id": self.config.run_id,
            "task": task_name, "message": str(error),
        }
        line = json.dumps(payload, ensure_ascii=False)
        self.config.alerts_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.alerts_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print(f"{severity}: {task_name}: {error}")

    def run(self) -> dict:
        cfg = self.config
        initialize_database(cfg.db_path)
        started = _utc_now()
        cfg.reports_dir.mkdir(parents=True, exist_ok=True)
        conn = connect(cfg.db_path)
        conn.execute(
            "INSERT INTO pipeline_runs(run_id,status,start_time,config_json) VALUES(?,?,?,?)",
            (cfg.run_id, "running", started, json.dumps({
                "max_reject_rate": cfg.max_reject_rate,
                "task_timeout_seconds": cfg.task_timeout_seconds,
                "max_attempts": cfg.max_attempts,
            })),
        )
        conn.close()

        state = {
            "config": cfg,
            "counts": {"total_rows": 0, "clean_rows": 0, "rejected_count": 0},
            "task_status": {},
        }
        task_records = []
        failure_reason = None
        for task in self.tasks:
            unmet = [dep for dep in task.dependencies if state["task_status"].get(dep) != "succeeded"]
            if unmet:
                state["task_status"][task.name] = "skipped"
                task_records.append({"task_name": task.name, "status": "skipped", "start_time": None,
                                     "end_time": _utc_now(), "row_count": 0, "rejected_count": 0,
                                     "attempts": 0, "error": f"Dependencies not successful: {', '.join(unmet)}"})
                continue
            task_start = _utc_now()
            started_clock = time.perf_counter()
            try:
                outcome = self._execute(task, state)
                metrics = outcome["result"]
                attempts = outcome["attempts"]
                status = "succeeded"
                error_text = None
                state["task_status"][task.name] = status
            except BaseException as exc:
                attempts = getattr(exc, "pipeline_attempts", 1)
                metrics = {}
                status = "failed"
                error_text = f"{type(exc).__name__}: {exc}"
                state["task_status"][task.name] = status
                if failure_reason is None:
                    failure_reason = f"{task.name}: {error_text}"
                self._alert(task.name, exc, task.name in CRITICAL_TASKS or task.name == "reconcile")
            elapsed = time.perf_counter() - started_clock
            state.setdefault("task_timings", {})[task.name] = elapsed
            task_records.append({
                "task_name": task.name, "status": status, "start_time": task_start,
                "end_time": _utc_now(), "row_count": int(metrics.get("row_count", 0)),
                "rejected_count": int(metrics.get("rejected_count", 0)),
                "attempts": attempts, "error": error_text,
            })

        # Any uncommitted publication must be rolled back if the run did not finish.
        if state.get("publish_transaction_open") and state.get("publish_connection") is not None:
            try:
                state["publish_connection"].rollback()
                state["publish_connection"].close()
            except sqlite3.Error:
                pass

        status = "succeeded" if all(state["task_status"].get(t.name) == "succeeded" for t in self.tasks) else "failed"
        if state["task_status"].get("quality_gate") == "failed":
            status = "blocked"
        conn = connect(cfg.db_path)
        try:
            conn.execute("BEGIN")
            for record in task_records:
                conn.execute(
                    """INSERT INTO task_runs
                       (run_id,task_name,status,start_time,end_time,row_count,rejected_count,attempts,error)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (cfg.run_id, record["task_name"], record["status"], record["start_time"],
                     record["end_time"], record["row_count"], record["rejected_count"],
                     record["attempts"], record["error"]),
                )
            conn.execute(
                """UPDATE pipeline_runs SET status=?, end_time=?, total_rows=?, clean_rows=?,
                   rejected_count=?, failure_reason=? WHERE run_id=?""",
                (status, _utc_now(), state["counts"].get("total_rows", 0),
                 state["counts"].get("clean_rows", 0), state["counts"].get("rejected_count", 0),
                 failure_reason, cfg.run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {
            "run_id": cfg.run_id, "status": status, "counts": dict(state["counts"]),
            "tasks": state["task_status"], "task_timings": state.get("task_timings", {}),
            "failure_reason": failure_reason,
        }


def run_pipeline(config) -> dict:
    return Orchestrator(config).run()
