# Objective

Produce a detailed Vietnamese DAG design grounded in the supplied Airflow second-edition book and assignment, with reasons and requirement traceability.

## Scope

Documentation, current-code gap analysis, architecture, task/data/time contracts, failure recovery, tests, delivery and demonstration plan.

## Out of scope

Implementing or deploying the new DAG, changing source datasets, publishing to Git, sending notifications.

## Steps

- [x] Extract assignment including equations/image and inspect relevant book chapters.
- [x] Map requirements to current code and identify contradictions.
- [x] Write design, rationale, book references, acceptance cases and implementation sequence.
- [x] Verify source references, document links and coverage; update shared state.

## Acceptance criteria

All assignment sections addressed; conflicting streaming rubric disclosed; recommendations distinguished from existing implementation; dates, partitioning, invoice-level anomalies, segmentation/churn and safe retries specified.

## Verification

Review book page references against local PDF text, relevant current code and official versioned Airflow documentation. Documentation checks only; no claim of implemented runtime behavior.

Result: `docs/DAG_DESIGN_VI.md` and REPORT navigation written. Illustrative Python AST parse, six local source links, requirement/test matrix coverage and `git diff --check` passed. Runtime acceptance tests remain future implementation work.

## Risks and rollback

Assignment rubric conflicts with the main assignment. Keep the conflict explicit until clarified. All runtime changes remain proposals; revert only newly written documentation if scope changes.
