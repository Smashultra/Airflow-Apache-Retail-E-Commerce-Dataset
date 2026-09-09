# Project Instructions

These instructions apply to every contributor and coding agent working in this repository.

## Required reading order

1. Read this file.
2. Read `.agent/HANDOFF.md`.
3. Read `.agent/TODO.md`.
4. Read `.agent/DECISIONS.md`.
5. Read the relevant plan under `.agent/plans/active/`, if one exists.
6. Inspect `git status` and `git diff` before editing.

## Sources of truth

- Code, schemas, tests, and Docker configuration define actual behavior.
- Git commits and pull requests record every file change and its reason.
- `.agent/HANDOFF.md` records the latest operational state only.
- `.agent/TODO.md` records outstanding work and ownership.
- `.agent/DECISIONS.md` records durable technical decisions, not routine edits.
- `.agent/plans/active/` contains approved plans for multi-step work.

## Working rules

- Work only on the assigned task and avoid overwriting unrelated changes.
- Assign an owner and branch in `.agent/TODO.md` before substantial work.
- Prefer the smallest implementation that satisfies the assignment.
- Keep orchestration in `dags/` and Spark computation in `scripts/`.
- Do not place heavy data processing inside an Airflow `PythonOperator`.
- Do not commit secrets, `.env`, generated Airflow config, logs, bytecode, full datasets, or generated Parquet output.
- Add or update one focused test for non-trivial logic.
- Do not claim completion without recording the verification command and result.
- Do not commit, push, publish, or delete user data unless explicitly requested.

## Before stopping

1. Run the smallest relevant checks.
2. Update `.agent/TODO.md`.
3. Update `.agent/HANDOFF.md` with result, verification, risks, and next action.
4. Move a completed plan from `plans/active/` to `plans/archive/`.
