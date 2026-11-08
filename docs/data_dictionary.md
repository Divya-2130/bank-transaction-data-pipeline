# Data Dictionary

SQLite column types below describe the declared schema. SQLite is dynamically typed, so application validation also enforces the intended domain.

## Source files

| File | Column | Logical type | Required | Rule |
|---|---|---|---|---|
| `accounts.csv` | `account_id` | TEXT | Yes | `ACC` plus exactly five digits; unique in the file |
| | `customer_name` | TEXT | Yes | Non-empty after trimming |
| | `account_type` | TEXT | Yes | `savings` or `current` |
| | `status` | TEXT | Yes | `active`, `frozen`, or `closed` |
| | `opened_date` | DATE text | Yes | ISO `YYYY-MM-DD` |
| `transactions.csv` | `transaction_id` | TEXT | Yes | `TXN` plus exactly seven digits; unique within accepted data |
| | `account_id` | TEXT | Yes | Must identify a known account |
| | `amount` | Decimal text | Yes | Parse with `Decimal`; round to 2 decimal places; must remain greater than zero |
| | `txn_type` | TEXT | Yes | `credit` or `debit` |
| | `currency` | TEXT | Yes | `INR` only |
| | `transaction_time` | ISO-8601 text | Yes | Cannot be in the future; offsets converted to UTC |

Input headers must have exactly the required set of names. A reordered set is acceptable. Missing, extra, or duplicate column names fail the schema task. A data row with the wrong number of values is quarantined.

## Database tables

### `accounts`

**Grain:** one current row per account. **Primary key:** `account_id`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `account_id` | TEXT | NOT NULL | Account business key |
| `customer_name` | TEXT | NOT NULL | Synthetic customer label |
| `account_type` | TEXT | NOT NULL | `savings` or `current` |
| `status` | TEXT | NOT NULL | Current account status |
| `opened_date` | TEXT | NOT NULL | ISO date |
| `run_id` | TEXT | NOT NULL | Run that last inserted/updated this row |
| `source_file` | TEXT | NOT NULL | Source file that last inserted/updated this row |

### `clean_transactions`

**Grain:** one accepted transaction. **Primary key:** `transaction_id`. `account_id` references `accounts.account_id`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `transaction_id` | TEXT | NOT NULL | Transaction business key |
| `account_id` | TEXT | NOT NULL | Account foreign key |
| `amount` | TEXT | NOT NULL | Two-decimal string, e.g. `125.40` |
| `txn_type` | TEXT | NOT NULL | `credit` or `debit` |
| `currency` | TEXT | NOT NULL | Always `INR` |
| `transaction_time` | TEXT | NOT NULL | UTC ISO timestamp ending in `Z` |
| `txn_date` | TEXT | NOT NULL | Derived UTC date (`YYYY-MM-DD`) |
| `run_id` | TEXT | NOT NULL | Publishing run |
| `source_file` | TEXT | NOT NULL | Original transactions file name |
| `source_row_number` | INTEGER | NOT NULL | One-based data row number, excluding header |

### `rejected_records`

**Grain:** one rejected source row. **Unique key:** `(run_id, source_file, row_number)`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `reject_id` | INTEGER PK | NOT NULL | Generated reject key |
| `source_file` | TEXT | NOT NULL | Input file name |
| `row_number` | INTEGER | NOT NULL | One-based data row number, excluding header |
| `reason` | TEXT | NOT NULL | Stable validation reason code |
| `run_id` | TEXT | NOT NULL | Run that evaluated the row |
| `raw_row` | TEXT | NOT NULL | JSON array of original field values |

Quarantine is committed independently from the publish transaction so bad rows remain available even when the quality gate blocks publication.

### `daily_account_summary`

**Grain:** one account per UTC transaction date. **Primary key:** `(account_id, txn_date)`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `account_id` | TEXT | NOT NULL | Account foreign key |
| `txn_date` | TEXT | NOT NULL | UTC date derived from transaction time |
| `transaction_count` | INTEGER | NOT NULL | Number of clean transactions in this account/date group |
| `credit_total` | TEXT | NOT NULL | Exact decimal sum of credits |
| `debit_total` | TEXT | NOT NULL | Exact decimal sum of debits |
| `run_id` | TEXT | NOT NULL | Run that last recomputed the group |
| `source_file` | TEXT | NOT NULL | Transactions file that last affected the group |

### `account_status_history`

**Grain:** one initial status or status transition. **Primary key:** `history_id`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `history_id` | INTEGER PK | NOT NULL | Generated history key |
| `account_id` | TEXT | NOT NULL | Account business key |
| `old_status` | TEXT | NULL | Prior status; NULL for initial account load |
| `new_status` | TEXT | NOT NULL | Newly observed status |
| `changed_at` | TEXT | NOT NULL | UTC timestamp when the pipeline applied the change |
| `run_id` | TEXT | NOT NULL | Run that applied the transition |
| `source_file` | TEXT | NOT NULL | Account source file |

### `pipeline_runs`

**Grain:** one orchestration run. **Primary key:** `run_id`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `run_id` | TEXT PK | NOT NULL | Unique run identifier |
| `status` | TEXT | NOT NULL | `running`, `succeeded`, `failed`, or `blocked` |
| `start_time` | TEXT | NOT NULL | Run start timestamp (UTC) |
| `end_time` | TEXT | NULL | Run end timestamp (UTC) |
| `total_rows` | INTEGER | NOT NULL | Transaction rows evaluated in this run |
| `clean_rows` | INTEGER | NOT NULL | Transactions accepted for this run |
| `rejected_count` | INTEGER | NOT NULL | Transaction rows rejected in this run |
| `failure_reason` | TEXT | NULL | First task failure, if any |
| `config_json` | TEXT | NOT NULL | Small JSON snapshot of relevant settings |

### `task_runs`

**Grain:** one task attempt summary per task name and run. **Unique key:** `(run_id, task_name)`.

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `task_run_id` | INTEGER PK | NOT NULL | Generated task record key |
| `run_id` | TEXT | NOT NULL | Parent run |
| `task_name` | TEXT | NOT NULL | Task identifier |
| `status` | TEXT | NOT NULL | `succeeded`, `failed`, or `skipped` |
| `start_time` | TEXT | NULL | Task start timestamp |
| `end_time` | TEXT | NULL | Task finish/skip timestamp |
| `row_count` | INTEGER | NOT NULL | Task row count metric |
| `rejected_count` | INTEGER | NOT NULL | Task reject count metric |
| `attempts` | INTEGER | NOT NULL | Attempts used, including retry attempts |
| `error` | TEXT | NULL | Error type and message or skip reason |

### `processed_files` (supporting table)

**Grain:** one successfully processed file content fingerprint. **Primary key:** `file_hash` (SHA-256).

| Column | SQLite type | Nullability | Meaning |
|---|---|---|---|
| `file_hash` | TEXT PK | NOT NULL | SHA-256 of exact input file bytes |
| `source_file` | TEXT | NOT NULL | Original file name |
| `processed_at` | TEXT | NOT NULL | UTC successful publish time |
| `run_id` | TEXT | NOT NULL | Run that committed the fingerprint |

The table makes exact-content reruns no-ops. Fingerprints are inserted in the same transaction as the publish, so failed runs do not mark the input as successfully processed.

## Derived fields and rules

- `txn_date` is the calendar date after converting `transaction_time` to UTC.
- Amount is rounded with `Decimal.quantize(Decimal('0.01'), ROUND_HALF_UP)`. A value such as `0.004` becomes `0.00` and is rejected.
- A timestamp with no offset is assumed to be UTC. An offset timestamp is converted to UTC.
- The quality-gate reject rate is `transaction rejects / transaction input rows`. It is zero for an empty transaction batch. Equality with the configured limit passes; only a greater rate blocks.
- Account-master rejects are quarantined but excluded from the transaction reject-rate denominator.
- A previously published transaction ID in a changed file is rejected as `transaction_id_already_published`.
- Existing historical transactions are not invalidated when an account is closed later. The reconciliation closed-account check applies to transactions published by the current run.
