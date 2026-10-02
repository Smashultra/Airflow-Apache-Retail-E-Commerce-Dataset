# Objective

Implement the user's official retail DAG following the reviewed design and updated rubric.

## Scope

Six real tasks, date validation and readiness sensor, immutable daily landing,
run-specific Spark outputs and manifests, RFM labels and order audit, completion
summary, retries/deadline, environment setup, focused tests and scheduler smoke run.

## Out of scope

Publishing Git, sending external notifications, deleting legacy data, production
cluster deployment and Kafka streaming.

## Steps

- [x] Inspect installed APIs and existing tests; implement shared contracts/landing.
- [x] Implement Spark pipeline mode while preserving legacy CLI behavior.
- [x] Implement DAG/sensor/callbacks and environment configuration.
- [x] Verify focused tests, failure/publication contracts and actual scheduler execution.
- [x] Update setup/design/status with results and limitations.

## Acceptance criteria

Six required tasks use real operators; historical dates are explicit; published
results are complete and traceable; legacy output paths remain untouched; relevant
tests pass and Airflow imports and executes the DAG with recorded evidence.

## Verification

Python syntax, Docker pytest for contracts/Spark/DAG, existing regression tests,
Airflow import checks and controlled DagRun on existing historical input.

Final evidence: 87 focused/regression tests passed in Docker; Ruff, formatter,
syntax and host repository-structure checks passed. Landing prepared from 541,909
rows with source SHA unchanged. Actual scheduler run `official_smoke_20260929`
and all six tasks succeeded. ETL completed with 391,057 RFM input rows and 536,641
audit rows; RFM produced 4,334 customers and order audit flagged 133 of 25,900
invoices. Independent read-back matched all eight output counts and 13 monthly
partitions. DAG paused after verification. Evidence and exact test commands:
`docs/IMPLEMENTATION_VI.md` and `docs/evidence/official_run_20260929.json`.

Limitations: no forced Spark crash/retry, deliberately overdue deadline or
multi-day backfill experiment; pool one means no runtime branch overlap. These
are explicitly documented follow-up acceptance experiments. Implementation and
the complete successful scheduler run are verified; no claim that every proposed
T01-T28 experiment or the entire graded assignment is complete.

## Risks and rollback

Existing user documentation and `.env.example` are preserved. New run outputs are
isolated from legacy data. Keep DAG paused except controlled smoke run; no automatic
cleanup. Deadlines and file publication require integration verification.
