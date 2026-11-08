"""Persist invalid source rows separately from the publish transaction."""
import json
from .database import connect


def quarantine(state: dict) -> dict:
    cfg = state["config"]
    conn = connect(cfg.db_path)
    try:
        conn.execute("BEGIN")
        conn.executemany(
            """INSERT OR IGNORE INTO rejected_records
               (source_file, row_number, reason, run_id, raw_row)
               VALUES (?, ?, ?, ?, ?)""",
            [
                (r["source_file"], r["row_number"], r["reason"], cfg.run_id,
                 json.dumps(r["raw_row"], ensure_ascii=False))
                for r in state["rejects"]
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"row_count": len(state["rejects"]), "rejected_count": len(state["rejects"])}
