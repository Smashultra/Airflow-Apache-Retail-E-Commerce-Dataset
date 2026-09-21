# Current Handoff

## Current state

The collaborative project scaffold and local-mode PySpark Docker configuration are complete. On `main`, the EDA notebook explains the zero-price records, and `scripts/pyspark_clean.py` writes separate RFM-ready and anomaly Parquet datasets with stricter RFM transaction filters.

## Completed

- Added shared `AGENTS.md`, `.agent` state, contribution rules, and a pull-request template.
- Added Docker, Airflow, PySpark, source, test, data, and documentation structure.
- Expanded `README.md` with the pipeline, complete directory map, Docker services, data paths, team workflow, commands, and implementation status.
- Standardized decision headings to include time and UTC offset.
- Expanded the LocalExecutor decision with the previous Celery architecture, rationale, removed services, resource consequences, and revisit conditions.
- Documented the selected Kaggle UCI Online Retail dataset and its CLI download command to `data/raw/data.csv`.
- Clarified that `docker compose build` installs `requirements.txt`; the Kaggle CLI runs from the built image, so contributors do not duplicate the Python environment on the host.
- Fixed the boolean filter in EDA notebook cell 15 and added reason summaries plus full-row inspection for non-cancelled negative quantities.
- Confirmed all 474 rows have zero unit price and missing customer ID; their descriptions indicate stock adjustments/write-offs rather than customer cancellations.
- Added a code cell immediately below EDA cell 15 that analyzes the remaining 582 zero-price rows using customer presence and whether the same invoice contains a positive-price line.
- Added the next EDA code cell to display the seven raw suspicious rows first, compare them with paid product/customer history, and explain the four affected invoices.
- Implemented `scripts/pyspark_clean.py` with configurable input/output paths and Spark-safe cleanup in `finally`.
- Added `data/curated/RFM.parquet`, which removes full-row duplicates, missing values, cancelled invoices, non-positive quantities, and non-positive unit prices.
- Added `data/audit/anomalies.parquet`, which removes full-row duplicates but preserves missing values and negative quantities.
- Added focused PySpark tests for the two cleaning rules, default output paths, and Parquet round-trip writes.
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
- README dependency flow matches the Dockerfile: `docker compose build` installs `requirements.txt`, including the Kaggle CLI; `git diff --check` passed.
- Targeted execution of EDA cell 15 against `data.csv` passed: 474 rows found, all summary counts total 474, and the detailed output contains all 474 rows.
- Notebook JSON parsing and Python syntax checks for cell 15 passed; the stale error output was removed.
- Targeted execution of the new adjacent EDA cell passed: `1056 = 474 + 582`, all 582 remaining rows have positive quantity and are not cancellations, and the four reason groups total 582.
- Targeted execution of the seven-row inspection cell passed: its first table contains exactly 7 raw rows, the evidence table contains all 7 rows, and the findings cover all 4 affected invoices.
- `docker compose build` completed and produced `ecommerce-airflow:3.3.1` with PySpark 4.2.0 and pytest 8.4.2.
- `docker compose run --rm --no-deps airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_pyspark_clean.py` -> 3 passed, with one upstream pandas-support warning from PySpark.
- Full-data `spark-submit /opt/airflow/scripts/pyspark_clean.py` completed with exit code 0.
- RFM cleaning tests in Docker -> 3 passed; coverage includes null/NaN customer IDs, cancelled invoices, non-positive quantities, and non-positive prices.
- Regenerated the full-data Parquet outputs in Docker; direct verification of `data/curated/RFM.parquet` -> `rows=392692`, with 0 invalid customer IDs, 0 cancelled invoices, 0 non-positive quantities, and 0 non-positive unit prices.

## Unresolved risks

- The Docker image builds and PySpark runs, but the complete Airflow/PostgreSQL service stack has not been initialized or smoke-tested.
- The anomaly-threshold method is not selected yet.
- Cell 15's seven reason groups are keyword-based interpretations of free-text `Description` values; 51 rows remain explicitly classified as unclear rather than being over-interpreted.
- The remaining zero-price rows do not contain a definitive reason label; the new cell distinguishes evidence-backed invoice contexts and explicitly treats gifts/promotions versus missing prices as unresolved possibilities.
- The four invoice-level explanations for the seven suspicious rows remain evidence-based hypotheses because the source data has no explicit reason field.
- Pytest created an inaccessible ignored directory named `pytest-cache-files-phri2efg`; removal failed with `Access is denied`, so its ACL was not altered.
- PySpark 4.2.0 emits an upstream warning that some features may not fully support pandas 3.x; this job does not use pandas APIs.
- Host Spark 3.5.9 cannot commit Parquet on Windows because the local Hadoop installation lacks `hadoop.dll`; use the Docker workflow, which completed successfully.
- `.env.example` is currently deleted in the working tree by an unrelated change and was intentionally not restored as part of this task.

## Next action

Wire `scripts/pyspark_clean.py` into the `submit_pyspark_etl` DAG task, then implement the downstream RFM metrics and anomaly-threshold jobs.
