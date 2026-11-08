# Benchmark Report

## Method

The benchmark script runs the complete pipeline at 10,000, 50,000, 100,000, and 200,000 transaction rows. It records the elapsed time of each orchestrated task. The baseline run used a row-by-row publisher (`connection.execute` per transaction). The optimized run uses `executemany` in chunks of 1,000 rows. Both publishers execute inside the same SQLite transaction and run the same validation, quarantine, transform, and reconciliation checks.

A separate insert-only comparison runs both row-wise and batched insertion against equivalent temporary SQLite tables inside an explicit transaction. A regression test, `test_batched_transaction_insert_matches_rowwise_reference`, confirms that both strategies write identical ordered rows.

## Full publish-task timing

| Input rows | Baseline publish (s) | Batched publish (s) | Measured reduction |
|---:|---:|---:|---:|
| 10,000 | 0.110065 | 0.091294 | 17.1% |
| 50,000 | 0.522355 | 0.460670 | 11.8% |
| 100,000 | 1.097586 | 0.969850 | 11.6% |
| 200,000 | 2.241723 | 2.162955 | 3.5% |

At every tested size, publish was the slowest individual task in the baseline pipeline. The total publish task includes account upserts, transaction insertion, daily-summary recomputation, and processed-file fingerprints. Therefore, improving only transaction insertion does not remove the other publish work; the measured total-task gain is smaller at 200,000 rows.

## Isolated insertion measurement

In the baseline benchmark's 192,000-row insert comparison:

| Strategy | Elapsed seconds |
|---|---:|
| Row-wise `execute` | 0.515511 |
| Chunked `executemany` | 0.379953 |

That isolated comparison measured a 1.36x throughput ratio (about 26.3% less elapsed time) for batched insertion. It supports the selected optimization, but it is not the same as the whole publish-task improvement.

## Files

- `benchmark_before.csv`: per-task baseline timings before the batch-insert change.
- `benchmark_after.csv`: per-task timings after the change.
- `benchmark_insert_comparison_before.csv`: isolated insert comparison from the baseline run.
- `benchmark_insert_comparison_after.csv`: isolated insert comparison from the after run.

## Environment and limitations

These measurements were run in the authoring container using Python 3.13.5 on Linux. Machine hardware details were not captured. They are real measurements from this run, not Windows 11 / Python 3.12 measurements. Results are sensitive to filesystem, CPU, SQLite build, and background load; rerun `python -m src.benchmark` on the target Windows environment before using the figures for capacity planning.
