"""Configuration shared by the command line and tests."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
import uuid

ROOT_DIR = Path(__file__).resolve().parents[1]

@dataclass
class PipelineConfig:
    accounts_path: Path = ROOT_DIR / "data" / "input" / "accounts.csv"
    transactions_path: Path = ROOT_DIR / "data" / "input" / "transactions.csv"
    db_path: Path = ROOT_DIR / "data" / "bank_pipeline.db"
    reports_dir: Path = ROOT_DIR / "reports"
    alerts_path: Path = ROOT_DIR / "alerts.log"
    max_reject_rate: float = 0.10
    task_timeout_seconds: float = 30.0
    max_attempts: int = 3
    retry_base_delay_seconds: float = 0.25
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    now: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        self.accounts_path = Path(self.accounts_path)
        self.transactions_path = Path(self.transactions_path)
        self.db_path = Path(self.db_path)
        self.reports_dir = Path(self.reports_dir)
        self.alerts_path = Path(self.alerts_path)
        if not 0.0 <= self.max_reject_rate <= 1.0:
            raise ValueError("max_reject_rate must be between 0 and 1")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.task_timeout_seconds <= 0:
            raise ValueError("task_timeout_seconds must be greater than 0")
        if self.retry_base_delay_seconds < 0:
            raise ValueError("retry_base_delay_seconds cannot be negative")
