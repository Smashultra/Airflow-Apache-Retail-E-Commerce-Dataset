# Current Handoff

## Current state

The collaborative project scaffold and local-mode PySpark Docker configuration are complete on branch `initial-setup`. On `main`, adjacent EDA code cells now explain all 1,056 zero-unit-price rows and inspect the seven customer-linked rows whose entire invoice has zero prices.

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

## Unresolved risks

- Docker is not available in the current host shell, so the image has not been built yet.
- The anomaly-threshold method is not selected yet.
- Cell 15's seven reason groups are keyword-based interpretations of free-text `Description` values; 51 rows remain explicitly classified as unclear rather than being over-interpreted.
- The remaining zero-price rows do not contain a definitive reason label; the new cell distinguishes evidence-backed invoice contexts and explicitly treats gifts/promotions versus missing prices as unresolved possibilities.
- The four invoice-level explanations for the seven suspicious rows remain evidence-based hypotheses because the source data has no explicit reason field.
- Pytest created an inaccessible ignored directory named `pytest-cache-files-phri2efg`; removal failed with `Access is denied`, so its ACL was not altered.

## Next action

Re-run cells 13-17 in `notebooks/EDA_Online_Retail.ipynb` to render all zero-price analyses, then continue with raw-data validation.
