# Synthetic Bank Transaction Data Pipeline

A small, local Python data-engineering project. It reads synthetic bank-account and transaction CSV files, validates their schemas and business rules, quarantines invalid source rows, transforms accepted transactions, publishes them into SQLite, and reconciles the published data before committing.

This repository uses only the Python standard library at runtime. `pytest` is the only development/test dependency. It does not connect to real banks, cloud services, paid services, or external APIs. All records are synthetic.

## What the pipeline does

- Reads `accounts.csv` and `transactions.csv`.
- Stops when either file has missing, extra, or duplicate column names. Column order may differ; names must match exactly.
- Quarantines malformed rows and business-rule failures with a reason, source file, one-based data row number, run ID, and raw row JSON.
- Normalizes transaction timestamps to UTC and derives `txn_date` from the UTC date.
- Keeps the first valid transaction ID; later identical rows are duplicates and later changed rows are conflicts.
- Allows credits to frozen accounts, rejects debits to frozen accounts, and rejects transactions for closed or unknown accounts.
- Uses a configurable reject-rate gate (default 10%). Rejected rows are committed to quarantine before this gate, so they remain available when publishing is blocked.
- Publishes accounts, transactions, summaries, status history, and processed-file fingerprints in one SQLite transaction. The transaction is committed only after reconciliation succeeds.
- Treats a successfully processed file fingerprint as a safe no-op on a rerun.

## Flow

```text
accounts.csv + transactions.csv
              |
            ingest
              |
       validate_schema  [critical]
              |
        validate_rules
              |
          quarantine  ----> rejected_records (kept even if publish is blocked)
              |
           transform
              |
        quality_gate   [critical]
              |
           publish     ----> SQLite transaction remains open
              |
          reconcile    ----> commit on PASS; rollback on FAIL
```

The publish gate is satisfied only when all critical tasks that precede publish (`validate_schema` and `quality_gate`) have succeeded. Reconciliation follows publish to preserve the required task order, but publish is provisional: the SQLite transaction is not committed until reconciliation passes. A reconciliation failure rolls back the provisional publish.

## Requirements

- Windows 11 and Python 3.12 are the intended user environment.
- The code uses standard-library features available in Python 3.12.
- `pytest` is required only to run the test suite.

The project was executed in the authoring container with Python 3.13.5. The test suite and sample commands passed there. A Windows 11 / Python 3.12 run has not been performed in this environment, so run the commands below on your machine as the final platform check.

## Setup in Windows PowerShell

From the extracted project folder:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks virtual-environment activation, you can use the environment's Python executable directly:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Run the sample

The committed sample has 60 transaction rows, with 30 intentionally rejected examples. Because its reject rate is 50%, run it with a higher explicit limit to demonstrate a successful publish:

```powershell
python -m src.run_pipeline --max-reject-rate 0.60
```

The normal CLI default remains 10%:

```powershell
python -m src.run_pipeline
```

With the intentionally dirty 60-row sample, the default 10% gate should block publication. This is expected, not a code failure.

Show run and task status:

```powershell
python -m src.show_runs
```

The CLI prints the new `run_id`. Compare the transaction rejects for that run with the committed answer key:

```powershell
python -m src.verify_labels --run-id YOUR_RUN_ID
```

## Generate synthetic data

Regenerate the 60-row example with a fixed seed:

```powershell
python -m src.generate_data --rows 60 --seed 20261108 --bad-rate 0.50
```

Generate a 100,000-row dataset with about 5% planted rejects in a separate folder:

```powershell
python -m src.generate_data `
  --rows 100000 `
  --seed 20261108 `
  --bad-rate 0.05 `
  --out-dir data/large/input `
  --answer-key data/large/expected_rejects.csv
```

Run that dataset using its own local database:

```powershell
python -m src.run_pipeline `
  --accounts data/large/input/accounts.csv `
  --transactions data/large/input/transactions.csv `
  --db data/large/bank_pipeline.db `
  --reports-dir data/large/reports `
  --alerts-log data/large/alerts.log `
  --max-reject-rate 0.10
```

Use the run ID printed by the command:

```powershell
python -m src.verify_labels `
  --db data/large/bank_pipeline.db `
  --answer-key data/large/expected_rejects.csv `
  --run-id YOUR_RUN_ID
```

The answer key is used only by `verify_labels.py` and the evidence script. The pipeline itself never reads it.

## Reproduce the evidence files

Run the normal sample, the idempotent rerun, and a deliberately blocked run:

```powershell
python -m src.make_sample_output
```

This writes to `sample_output/`:

- `clean_transactions.csv`
- `rejected_records.csv`
- `daily_account_summary.csv`
- `run_status.csv`
- `validation_report.json`
- `alerts.log`
- `reports/` with reconciliation JSON reports

The normal sample run uses a 60% reject limit because half of its rows are deliberately bad. The rerun must add no records. The third run changes the transaction file fingerprint, encounters already-published transaction IDs, and is blocked at the 10% quality gate. Its publish task is skipped and it writes a CRITICAL alert.

## Tests

Run all tests from the project root:

```powershell
python -m pytest -q
```

Tests use fresh temporary SQLite databases and a fixed clock. The suite covers happy and empty batches, zero-byte files, malformed records, required fields, invalid values, duplicates/conflicts, schema changes, account states, UTC boundaries, rounding, out-of-order records, retry/backoff behaviour, retry exhaustion, timeouts, dependency skipping, quality-gate behaviour, idempotency, restart after failure, account-status history, atomic publish rollback, reconciliation failures, answer-key verification, generator reproducibility, and batch-insert equivalence.

## Benchmark

Run the four required sizes (10k, 50k, 100k, 200k):

```powershell
python -m src.benchmark
```

The script writes task timings to `reports/benchmark.csv` and an isolated row-wise-vs-batched insert comparison to `reports/benchmark_insert_comparison.csv`. Existing measured baseline and after-optimization results are retained in `reports/benchmark_before.csv`, `reports/benchmark_after.csv`, and the comparison CSVs.

Benchmark timings depend on machine, disk, Python build, and SQLite build. The recorded measurements in this repository came from the authoring container (Python 3.13.5, Linux), not a Windows 11 / Python 3.12 workstation. See `reports/benchmark_report.md` for the measured figures and their limits.

## Folder layout

```text
bank-data-pipeline/
├── data/
│   ├── input/                 # committed 60-row synthetic sample
│   └── answer_key/            # separate generator labels; never read by pipeline
├── src/                       # pipeline tasks, orchestrator, CLIs and utilities
├── tests/                     # pytest suite; every test uses a temp database
├── docs/
│   ├── data_dictionary.md
│   └── design_notes.md
├── ai-usage/
│   ├── prompts.md              # intentionally empty; fill in your real prompts
│   └── reflection.md           # intentionally empty; fill in your own reflection
├── sample_output/              # sample run evidence
├── reports/                    # measured benchmark evidence
├── requirements.txt
└── README.md
```

## Orchestration behaviour

- Tasks run in this fixed order: `ingest`, `validate_schema`, `validate_rules`, `quarantine`, `transform`, `quality_gate`, `publish`, `reconcile`.
- A task runs only when its dependencies succeeded. Otherwise it is saved as `skipped`.
- `validate_schema` and `quality_gate` are critical pre-publish gates. Reconciliation is a post-publish commit gate.
- Only `TransientError` and SQLite lock/busy operational errors are retried. Retry delay grows exponentially (`base_delay * 2^(attempt - 1)`). Schema, quality-gate, and reconciliation failures are never retried.
- Each task has a timeout. Python threads cannot be safely force-killed. A timed-out worker may continue until its function returns; this limitation is made explicit rather than claiming hard cancellation.
- Pipeline and task status, timestamps, counts, attempts, errors, and failure reasons are stored in `pipeline_runs` and `task_runs`.
- Failures are printed and appended as JSON Lines to `alerts.log`. Schema, quality-gate, and reconciliation failures are marked `CRITICAL`.

## Idempotency, restart, and backfill

`processed_files` stores SHA-256 fingerprints only inside the same transaction as a successful publish. A later run with the same file content skips those rows and does not create new rejects. If validation, the quality gate, publishing, or reconciliation fails before commit, fingerprints are not stored, so a corrected or retried batch can be processed.

A changed file with a transaction ID already in the target is rejected as `transaction_id_already_published`; this prevents a changed source file from silently changing an immutable transaction. For a backfill, provide a new file containing genuinely new transaction IDs and timestamps. Late and out-of-order transactions are accepted; `txn_date` comes from the transaction timestamp converted to UTC, not from arrival order.

Account rows are upserted. Status transitions append to `account_status_history`; the current account row is updated while prior status changes remain auditable.

## Assumptions and known limitations

- All data is synthetic. Do not use real bank/customer information.
- `accounts.csv` and `transactions.csv` must each have exactly the required column names (no extras or duplicates). Reordered columns are accepted.
- A timestamp without an explicit UTC offset is interpreted as UTC. An explicit offset is converted to UTC.
- Money is parsed with `Decimal`, rounded to two places using `ROUND_HALF_UP`, and stored as text in SQLite to avoid binary floating-point rounding.
- Reject rate is calculated over transaction rows. Invalid account master rows are quarantined but are not included in the transaction reject-rate denominator.
- A zero-byte file is a schema failure; a header-only file is a valid empty batch.
- The task timeout is best-effort because standard Python threads cannot be force-killed.
- There is no distributed locking or multi-process scheduler. Use one local pipeline process per SQLite database at a time.
- This is a learning project, not a regulated banking system. It does not implement encryption, access control, PII handling, financial posting, reversals, or production monitoring.

## AI-use notes

`ai-usage/prompts.md` and `ai-usage/reflection.md` are intentionally empty. Fill them in yourself with the prompts you actually used and your honest reflection on what you personally reviewed, changed, tested, and understood. Do not claim prompts or actions that you did not perform.

## Next steps

1. Run the setup and tests on your own Windows 11 / Python 3.12 machine.
2. Generate and verify the 100,000-row dataset locally.
3. Inspect `rejected_records`, `task_runs`, and reconciliation JSON reports in SQLite.
4. Extend the pipeline only after you can explain each task and its failure behaviour.
