# Objective

Implement the approved minimal two-DAG design with frozen model identity.

## Scope

Keep six daily tasks, cumulative ETL, audit and legacy CLI. Add training DAG,
full PipelineModel persistence, activation metadata and ETL-only bootstrap.

## Steps

- [x] Implement model contracts, fit/apply split and training/bootstrap entry.
- [x] Wire three-task training DAG and daily model binding.
- [x] Test lifecycle, temporal/model identity, Spark round-trip and DAG imports.
- [x] Update setup, design status and handoff with actual verification.

## Verification

19 metadata/DAG tests and 75 Spark/regression tests passed in Docker. Ruff and
DAG processor import checks passed. Full-data scheduler demo was not rerun;
SETUP documents bootstrap and activation before enabling daily scoring.

## Acceptance criteria

Daily never fits; scaler/model identity survives retry; training consumes
committed ETL without daily completion; future/incompatible models rejected;
old activations survive failed training. Both DAGs share the Spark pool.

## Risks and rollback

No deletes or replacement of existing model/data generations. Keep legacy CLI.
Runtime verification depends on Docker availability; record exact limitations.
