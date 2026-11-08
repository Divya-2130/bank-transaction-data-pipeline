# Design Notes and Trade-offs

| Decision | Choice | Why | Trade-off / limitation |
|---|---|---|---|
| Orchestration | Small custom sequential task runner | Keeps dependencies, retries, timeouts, and task history visible without an Airflow service | No distributed scheduling, UI, or multi-worker execution |
| Storage | SQLite file | Standard library, local, easy to inspect and test | Single-writer database; not intended for concurrent production workloads |
| Input parsing | `csv.reader` with explicit header and field-count checks | Detects short/long rows and preserves raw values for quarantine | Large files are read into memory for this learning-sized vertical slice |
| Schema policy | Exact set of required column names; reorder allowed | Missing/extra/duplicate names cannot silently pass | No schema-version migration framework |
| Money | `Decimal` and two-decimal text storage | Avoids binary floating-point errors in currency totals | Totals are aggregated in Python rather than SQLite numeric `SUM` |
| Time | Parse ISO-8601, assume UTC for naive values, convert offsets to UTC | One consistent date basis for summaries and backfills | The source timezone is not inferred when the file omits an offset |
| Validation | First valid transaction ID wins | Deterministic duplicate and conflict behaviour | No correction/update semantics for a previously published transaction |
| Quarantine | Commit rejected rows before the publish gate | Bad rows remain visible even when clean data is blocked | Quarantine is intentionally not part of the clean-data publish transaction |
| Publish atomicity | `BEGIN IMMEDIATE`; accounts, transactions, summaries, file fingerprints in one transaction | Avoids partial clean-data writes | Long runs hold the SQLite writer lock through reconciliation |
| Reconciliation | Check provisional rows and aggregates before commit | A mismatch can roll back the publish | The check runs after provisional publish, so it is a commit gate rather than a pre-publish task |
| Idempotency | SHA-256 file-content fingerprints committed with successful publish | Exact reruns safely become no-ops, independent of path | Any changed file is a new batch; pre-published transaction IDs are rejected |
| Account history | Append a history row on initial load or status transition | Status changes are visible rather than silently overwritten | Only status history is versioned; other account attributes retain current-state semantics |
| Retry policy | Retry explicit `TransientError` and SQLite locked/busy errors with exponential delay | Avoids retrying deterministic data failures | There is no sophisticated error taxonomy beyond these transient classes |
| Timeout | Run each task in a daemon thread and wait up to the limit | Provides a best-effort timeout around arbitrary Python functions | Python cannot safely force-kill a thread; work may continue after timeout |
| Alerts | Console message plus JSON Lines file | Works offline and is easy to inspect | No email, paging, or monitoring integration |
| Answer key | Separate CSV generated beside the data | Enables exact deterministic validation without contaminating pipeline logic | Treat the answer key as test evidence, not production reference data |
| Performance optimization | Batch transaction inserts with `executemany` in chunks of 1,000 | Reduces Python/SQLite call overhead | Aggregate publish work still exists, so overall publish-task improvement is modest |

## Critical-task ordering

`validate_schema` and `quality_gate` are the explicit critical pre-publish gates. The required order places `reconcile` after `publish`; therefore publish starts only after the pre-publish critical tasks pass, holds the SQLite transaction open, and commits only when reconciliation passes. If reconciliation fails, the publish transaction rolls back. This resolves the ordering constraint without claiming that reconciliation ran before publish.

## Idempotency boundary

A source file is considered processed only after its fingerprint is committed together with the clean-data publish. If the quality gate or reconciliation fails, no fingerprint is committed and the exact files can be attempted again after a configuration or data fix. Rejected rows are stored by run, so a failed attempt remains auditable.

## Benchmark interpretation

The retained benchmark captures the baseline publisher (one `execute` call per transaction) and the optimized publisher (chunked `executemany`). The baseline publish task was the slowest task across tested sizes. The isolated insert comparison demonstrates the row-wise vs batch difference; the full publish task improves less because it also upserts accounts, recomputes daily summaries, and writes file fingerprints. See `reports/benchmark_report.md` for actual numbers. Results are specific to the authoring container and should be rerun on the target Windows machine.
