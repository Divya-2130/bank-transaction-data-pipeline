"""CSV ingestion. Keeps row numbers and raw field values for quarantine."""
import csv
import hashlib
import io
from pathlib import Path
from typing import Any
from .database import connect, initialize_database


def read_csv(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    raw_bytes = path.read_bytes()
    file_hash = hashlib.sha256(raw_bytes).hexdigest()
    text = raw_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration:
        header = []
    rows = []
    for row_number, raw in enumerate(reader, start=1):
        rows.append({
            "row_number": row_number,
            "raw": raw,
            "values": dict(zip(header, raw)) if len(raw) == len(header) else None,
            "malformed": len(raw) != len(header),
        })
    return {
        "path": path,
        "source_file": path.name,
        "file_hash": file_hash,
        "header": header,
        "rows": rows,
        "row_count": len(rows),
    }


def ingest(state: dict) -> dict:
    cfg = state["config"]
    initialize_database(cfg.db_path)
    files = {
        "accounts": read_csv(cfg.accounts_path),
        "transactions": read_csv(cfg.transactions_path),
    }
    conn = connect(cfg.db_path)
    try:
        for item in files.values():
            item["already_processed"] = conn.execute(
                "SELECT 1 FROM processed_files WHERE file_hash = ?",
                (item["file_hash"],),
            ).fetchone() is not None
            # A known file is a safe no-op: it must not create new rejects.
            if item["already_processed"]:
                item["rows_to_process"] = []
            else:
                item["rows_to_process"] = item["rows"]
    finally:
        conn.close()
    state["files"] = files
    state["counts"]["total_rows"] = len(files["transactions"]["rows_to_process"])
    return {"row_count": state["counts"]["total_rows"]}

