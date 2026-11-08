# AI Usage – Project Prompts

## Project: Bank Transaction Data Pipeline

### AI Tool Used
ChatGPT

### Purpose
To get assistance designing, implementing, testing, documenting, and debugging a local data engineering pipeline using Python and SQLite.

## 1. Initial Project Prompt

The following is the original project specification provided to ChatGPT.

---

You are a senior data engineer building a complete, tested, local data pipeline project. I am a graduate learning, so write clear, commented, simple code and explain decisions briefly.

## Goal

Build an orchestrated data pipeline for SYNTHETIC bank transactions. It reads two CSV files, validates them, quarantines bad rows with a reason, publishes clean data to SQLite, and reconciles source against target. If a critical task fails, nothing is published.

## Environment

Windows 11, PowerShell, Python 3.12. Use only the standard library plus pytest. No cloud, no paid services, no secrets. Everything must run from a clean clone using requirements.txt.

## Working method

1. First, show me a short plan: file list, tables, task order, and the phases below. Wait for my approval before writing code.
2. Build one phase at a time. After each phase: run the relevant tests or commands, show me the real output, fix failures, then make a git commit with a clear message.
3. Never claim something works unless you ran it. If something fails or is incomplete, say so plainly.
4. Keep the scope small. A complete, tested vertical slice beats a large unfinished system.

## Data model

accounts.csv: account_id (ACC + 5 digits, unique), customer_name, account_type (savings|current), status (active|frozen|closed), opened_date (YYYY-MM-DD).

transactions.csv: transaction_id (TXN + 7 digits, unique), account_id (must exist), amount (decimal > 0 after rounding to 2 places), txn_type (credit|debit), currency (INR only), transaction_time (ISO-8601, not in the future).

Output tables (snake_case): clean_transactions, rejected_records, daily_account_summary, account_status_history, pipeline_runs, task_runs. Every output row carries run_id and source_file where relevant.

## Business rules

- Required fields must not be empty.
- Malformed rows (wrong field count) are rejected.
- Unknown account is rejected.
- Closed account is rejected (frozen may still receive credits).
- Same transaction_id with identical content is a duplicate (keep first, reject later).
- Same ID with different content is a conflict (first wins, reject later).
- A repeat of the same file in a rerun must NOT be rejected, it must be a safe no-op.
- Late and out-of-order rows are accepted and land on their true date.
- Timestamps with offsets are converted to UTC.
- A missing or extra column is a schema failure that stops the run.
- Rejected rows keep source_file, row_number (from 1, header excluded), reason, run_id and the raw row as JSON.

## Pipeline tasks (in order)

ingest, validate_schema (critical), validate_rules, quarantine, transform (adds txn_date, run_id, source_file), quality_gate (critical: fail if reject rate is above a configurable limit, default 10%, or counts do not add up), publish, reconcile (writes a JSON report; fail if checks differ).

publish must be idempotent (conflict-ignore for transactions, upsert for accounts) and all-or-nothing in one database transaction. Keep account status changes as history, never overwrite silently.

## Orchestrator

Write a small custom orchestrator (no Airflow). Support:

- Task dependencies (a task runs only if its dependencies succeeded, otherwise it is skipped).
- Retries with growing delay for transient errors only (do not retry schema, quality-gate or reconciliation failures).
- A per-task timeout (document honestly that Python threads cannot be force-killed).
- Run status saved in pipeline_runs and task_runs (run_id, start/end time, row counts, rejected count, failure reason, attempts, error).
- Failure alerts to the console and a JSON-lines alerts.log.
- A publish gate: publish runs only if EVERY critical task succeeded.

Add a CLI (src/run_pipeline.py) and a status viewer (src/show_runs.py).

## Synthetic data

Write src/generate_data.py with a fixed --seed.

Produce a small committed sample (about 60 rows, half bad on purpose) and a configurable large run (100,000+ rows, about 5% bad).

Plant these problems and label each one in a SEPARATE answer-key CSV that the pipeline never reads:

- missing field
- bad amount
- bad txn_type
- bad currency
- bad timestamp
- future date
- unknown account
- closed account
- exact duplicate
- conflicting duplicate

Shuffle rows so some arrive out of order.

Write src/verify_labels.py that compares pipeline rejects with the answer key exactly.

## Tests (pytest, fresh temp database per test, fixed now)

- Happy path
- Empty batch
- Zero-byte file
- Malformed rows
- Every required field empty
- Each invalid value
- Duplicates and conflicts
- Schema change (renamed and extra column)
- Invalid account reference
- Closed and frozen accounts
- UTC date boundary
- Sub-paisa amount
- Late and out-of-order data
- Retry until success
- Retries exhausted
- Timeout
- Quality gate blocks publish but keeps rejects
- Reject rate exactly at the limit
- Atomic publish
- Reruns add nothing
- Restart after a failed run
- Status history
- Reconciliation detects missing rows
- End-to-end runs compared with the answer key
- Generator reproducibility

## Evidence and performance

src/make_sample_output.py runs the sample three times (normal, rerun, blocked by the quality gate) and writes to sample_output/: clean and rejected CSVs, daily summary, run status, and a validation report with PASS/FAIL checks:

- rows in = clean + rejected
- no duplicate IDs
- no orphan accounts
- no transactions on closed accounts
- totals match
- rerun adds nothing
- rejects match the answer key

src/benchmark.py times each task at 10k, 50k, 100k and 200k rows.

Find ONE real bottleneck by measuring, apply ONE optimization, prove the results are identical with a test, and report before and after numbers. Do not claim improvements you did not measure.

## Documentation

README.md: purpose, flow diagram, setup and run commands, folder layout, orchestration behaviour, idempotency/restart/backfill, testing, assumptions, known limitations, next steps.

docs/data_dictionary.md: tables, keys, types, nullability, grain, derived fields, rules.

docs/design_notes.md: decision and trade-off table.

Create ai-usage/ with empty prompts.md and reflection.md templates and tell me I must fill them in myself truthfully. Never invent my prompts or my actions.

## Definition of done

- All tests pass.
- A fresh clone runs the README commands successfully.
- verify_labels reports PASS on both the sample and the 100,000-row run.
- The blocked run shows publish skipped and a CRITICAL alert.
- The repo has meaningful commits.

Finish by listing what works, what does not, and known limitations.

---

## 2. Additional follow-up prompts

During the project, I also used ChatGPT to assist with:

- Creating and packaging the complete project.
- Setting up the Python virtual environment on Windows.
- Running pytest and resolving command-line issues.
- Running and verifying the 60-row sample.
- Generating and verifying 100,000 synthetic transactions.
- Troubleshooting answer-key verification and run IDs.
- Running performance benchmarks at 10K, 50K, 100K and 200K rows.
- Measuring SQLite insertion performance.
- Managing Git commits and publishing the project to GitHub.
- Inspecting SQLite tables and database records.

Note: This section summarizes follow-up assistance during the conversation; it is not a verbatim transcript of every follow-up message.