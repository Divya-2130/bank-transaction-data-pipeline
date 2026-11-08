"""Strict schema and business-rule validation for the two source files."""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from .database import connect
from .errors import SchemaValidationError

ACCOUNT_COLUMNS = ["account_id", "customer_name", "account_type", "status", "opened_date"]
TRANSACTION_COLUMNS = ["transaction_id", "account_id", "amount", "txn_type", "currency", "transaction_time"]
ACCOUNT_ID_RE = re.compile(r"^ACC\d{5}$")
TRANSACTION_ID_RE = re.compile(r"^TXN\d{7}$")
CENT = Decimal("0.01")


def validate_schema(state: dict) -> dict:
    errors = []
    for key, required in (("accounts", ACCOUNT_COLUMNS), ("transactions", TRANSACTION_COLUMNS)):
        item = state["files"][key]
        header = item["header"]
        if not header:
            errors.append(f"{item['source_file']}: missing header (empty or zero-byte file)")
            continue
        missing = sorted(set(required) - set(header))
        extra = sorted(set(header) - set(required))
        duplicates = sorted({name for name in header if header.count(name) > 1})
        if missing or extra or duplicates or len(header) != len(required):
            errors.append(
                f"{item['source_file']}: schema mismatch; missing={missing}, extra={extra}, duplicate={duplicates}"
            )
        item["column_set_valid"] = not (missing or extra or duplicates or len(header) != len(required))
    if errors:
        raise SchemaValidationError("; ".join(errors))
    return {"row_count": sum(x["row_count"] for x in state["files"].values())}


def _as_utc(value: str) -> datetime:
    # Require a date-time separator; a date by itself is not a transaction timestamp.
    cleaned = value.strip()
    if "T" not in cleaned.upper():
        raise ValueError("timestamp must contain an ISO-8601 date-time separator")
    # fromisoformat accepts offsets and the trailing Z form in Python 3.12.
    parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00").replace("z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _reject(state: dict, item: dict, row: dict, reason: str) -> None:
    state["rejects"].append({
        "source_file": item["source_file"],
        "row_number": row["row_number"],
        "reason": reason,
        "raw_row": row["raw"],
    })


def _account_reason(values: dict) -> str | None:
    for field in ACCOUNT_COLUMNS:
        if not str(values.get(field, "")).strip():
            return f"missing_required_field:{field}"
    if not ACCOUNT_ID_RE.fullmatch(values["account_id"].strip()):
        return "invalid_account_id"
    if values["account_type"].strip() not in {"savings", "current"}:
        return "invalid_account_type"
    if values["status"].strip() not in {"active", "frozen", "closed"}:
        return "invalid_account_status"
    try:
        date.fromisoformat(values["opened_date"].strip())
    except ValueError:
        return "invalid_opened_date"
    return None


def validate_rules(state: dict) -> dict:
    cfg = state["config"]
    state["rejects"] = []
    state["valid_accounts"] = []
    state["clean_transactions"] = []

    conn = connect(cfg.db_path)
    try:
        account_lookup = {
            row["account_id"]: dict(row)
            for row in conn.execute("SELECT * FROM accounts")
        }
        existing_transaction_ids = {
            row[0] for row in conn.execute("SELECT transaction_id FROM clean_transactions")
        }
    finally:
        conn.close()

    account_file = state["files"]["accounts"]
    seen_account_ids = set()
    if not account_file["already_processed"]:
        for row in account_file["rows_to_process"]:
            if row["malformed"]:
                _reject(state, account_file, row, "malformed_row_field_count")
                continue
            values = row["values"]
            reason = _account_reason(values)
            if reason:
                _reject(state, account_file, row, reason)
                continue
            account_id = values["account_id"].strip()
            if account_id in seen_account_ids:
                _reject(state, account_file, row, "duplicate_account_id")
                continue
            seen_account_ids.add(account_id)
            account = {
                "account_id": account_id,
                "customer_name": values["customer_name"].strip(),
                "account_type": values["account_type"].strip(),
                "status": values["status"].strip(),
                "opened_date": values["opened_date"].strip(),
                "source_file": account_file["source_file"],
                "row_number": row["row_number"],
            }
            state["valid_accounts"].append(account)
            account_lookup[account_id] = account

    txn_file = state["files"]["transactions"]
    seen_transactions: dict[str, tuple[str, ...]] = {}
    now = cfg.now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)

    for row in txn_file["rows_to_process"]:
        if row["malformed"]:
            _reject(state, txn_file, row, "malformed_row_field_count")
            continue
        values = row["values"]
        missing = next((field for field in TRANSACTION_COLUMNS if not str(values.get(field, "")).strip()), None)
        if missing:
            _reject(state, txn_file, row, f"missing_required_field:{missing}")
            continue
        transaction_id = values["transaction_id"].strip()
        account_id = values["account_id"].strip()
        if not TRANSACTION_ID_RE.fullmatch(transaction_id):
            _reject(state, txn_file, row, "invalid_transaction_id")
            continue
        if not ACCOUNT_ID_RE.fullmatch(account_id):
            _reject(state, txn_file, row, "invalid_account_id")
            continue
        try:
            amount = Decimal(values["amount"].strip())
            if not amount.is_finite():
                raise InvalidOperation
            amount = amount.quantize(CENT, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError):
            _reject(state, txn_file, row, "invalid_amount")
            continue
        if amount <= 0:
            _reject(state, txn_file, row, "amount_not_positive_after_rounding")
            continue
        txn_type = values["txn_type"].strip()
        if txn_type not in {"credit", "debit"}:
            _reject(state, txn_file, row, "invalid_txn_type")
            continue
        if values["currency"].strip() != "INR":
            _reject(state, txn_file, row, "unsupported_currency")
            continue
        try:
            transaction_time = _as_utc(values["transaction_time"])
        except (ValueError, TypeError):
            _reject(state, txn_file, row, "invalid_timestamp")
            continue
        if transaction_time > now:
            _reject(state, txn_file, row, "future_transaction_time")
            continue
        account = account_lookup.get(account_id)
        if account is None:
            _reject(state, txn_file, row, "unknown_account")
            continue
        if account["status"] == "closed":
            _reject(state, txn_file, row, "closed_account")
            continue
        if account["status"] == "frozen" and txn_type == "debit":
            _reject(state, txn_file, row, "frozen_account_debit")
            continue
        if transaction_id in existing_transaction_ids:
            _reject(state, txn_file, row, "transaction_id_already_published")
            continue
        raw_signature = tuple(row["raw"])
        if transaction_id in seen_transactions:
            reason = "duplicate_transaction_id" if seen_transactions[transaction_id] == raw_signature else "conflicting_transaction_id"
            _reject(state, txn_file, row, reason)
            continue
        seen_transactions[transaction_id] = raw_signature
        state["clean_transactions"].append({
            "transaction_id": transaction_id,
            "account_id": account_id,
            "amount": format(amount, ".2f"),
            "txn_type": txn_type,
            "currency": "INR",
            "transaction_time": transaction_time.isoformat().replace("+00:00", "Z"),
            "txn_date": transaction_time.date().isoformat(),
            "source_file": txn_file["source_file"],
            "source_row_number": row["row_number"],
        })

    txn_rejects = [r for r in state["rejects"] if r["source_file"] == txn_file["source_file"]]
    state["counts"].update({
        "total_rows": len(txn_file["rows_to_process"]),
        "clean_rows": len(state["clean_transactions"]),
        "rejected_count": len(txn_rejects),
    })
    return {"row_count": state["counts"]["total_rows"], "rejected_count": len(state["rejects"])}
