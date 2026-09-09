# Current Handoff

## Current state

The collaborative project scaffold and local-mode PySpark Docker configuration are complete on branch `initial-setup` and ready for team review.

## Completed

- Added shared `AGENTS.md`, `.agent` state, contribution rules, and a pull-request template.
- Added Docker, Airflow, PySpark, source, test, data, and documentation structure.
- Expanded `README.md` with the pipeline, complete directory map, Docker services, data paths, team workflow, commands, and implementation status.
- Standardized decision headings to include time and UTC offset.
- Expanded the LocalExecutor decision with the previous Celery architecture, rationale, removed services, resource consequences, and revisit conditions.
- Documented the selected Kaggle UCI Online Retail dataset and its CLI download command to `data/raw/data.csv`.
- Normalized the DAG task IDs to the assignment names while keeping tasks as placeholders.
- Removed generated Airflow config, logs, bytecode, and `.env` from the source tree; Git can recover the deleted tracked files.
- No `CLAUDE.md` was created.

## Verification

- `python -m pytest -q -p no:cacheprovider tests\test_pyspark_jobs.py` -> 1 passed.
- Python source compilation -> 5 files passed.
- `docker-compose.yaml` parsed with PyYAML.
- Spark's `spark_default` connection resolves to local mode using all available Docker CPU cores (`local[*]`).
- `.codex/config.toml` parsed with Python `tomllib`.
- `git diff --check` passed; only line-ending notices were emitted.
- README local-link validation passed (`readme_links=ok`).
- The setup is isolated on `initial-setup`; `main` remains unchanged.

## Unresolved risks

- Docker is not available in the current host shell, so the image has not been built yet.
- The anomaly-threshold method is not selected yet.
- Pytest created an inaccessible ignored directory named `pytest-cache-files-phri2efg`; removal failed with `Access is denied`, so its ACL was not altered.

## Next action

Download `data/raw/data.csv`, verify the Docker environment, then implement raw-data validation.
