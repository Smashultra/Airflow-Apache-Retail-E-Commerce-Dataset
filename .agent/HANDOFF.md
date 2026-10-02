# Current Handoff

## Official DAG implemented and exercised - 2026-09-29

- User explicitly requested the official DAG implementation and continuation. Implemented six real tasks, SDK timetable/deadline declaration, FileSensor lifecycle, three Spark submissions, immutable daily landing, frozen run context, versioned Parquet and consistent all-success publication. Legacy CLI branches remain available; original data and user `.env.example` were preserved. No commit/push or external messages.
- Files: `dags/ecommerce_etl_dag.py`, `dags/retail_support/tasks.py`, `scripts/retail_contracts.py`, `scripts/prepare_daily_landing.py`, `scripts/retail_pipeline.py`, legacy script entry points, Compose, dependency pins, focused tests and setup/design/report/evidence. Detailed final behavior and limitations: `docs/IMPLEMENTATION_VI.md`.
- Docker services were recreated to apply PYTHONPATH, local[2] Spark and File connections. Prepared `online-retail-v1` landing from 541,909 source rows; source SHA remains unchanged. Created retail_spark pool with one slot; other user Docker workloads were untouched.
- Verification: Docker `python -m pytest -q tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py` -> 87 passed, one upstream pandas/PySpark warning, 281.51 seconds. Ruff check for dags/scripts/tests and formatter check for new code passed, syntax/host structure checks passed, no DAG import errors, `git diff --check` passed.
- Real scheduler run `official_smoke_20260929`, business_date 2011-12-09, k=2, churn_days=90: DagRun and all six tasks success, UTC 06:17:58.986 to 06:27:28.054. DAG paused afterward. Published `data/manifests/published/2011-12-10.json`; run context key `dd98a9cee141be7fa1da53074440e416640730919bad3540f22c9ee81019ee51`.
- Results: ETL 536,641 dedup/audit rows, 391,057 RFM input rows, zero parse errors; RFM 4,334 customers, 1,734 high-value, 1,463 inactive; audit 25,900 invoices, 133 flagged, 5,993 not assessable, 40,051 flagged source rows. Independent Spark read-back of all eight outputs matched manifest counts and verified 13 monthly partitions. Aggregate evidence is `docs/evidence/official_run_20260929.json`; raw logs and local read-back helper/result are ignored under logs/data.
- Limits: one JVM at a time (independent branches, no parallel-runtime claim), full-history snapshots, local shared compute, global customer rank Window, mean-only cluster profile, retrospective anomaly and inactivity proxy. Deadline declaration/import verified, but no deliberately overdue deadline run, multi-day backfill or interrupted-JVM recovery experiment. These are recorded as limits, not failed checks or completed acceptance cases.
- Next action: user can inspect the successful run in Graph/Grid at localhost:8080 or run another explicit historical date following SETUP. Remaining assignment deliverables are selected screenshots, final report/presentation and optional extended acceptance experiments. Do not rebuild or rerun the completed pipeline just to repeat successful checks.

## Updated assignment rubric incorporated - 2026-09-28

- User supplied replacement rubric: conceptual depth 25, DAG implementation 25, Spark ETL/analytics 25, architecture/setup/reproducibility 15, DQ/anomaly 10. This resolves the earlier streaming-rubric conflict; no further scope confirmation is needed. DOCX sections 1–5 remain functional requirements.
- Updated `docs/DAG_DESIGN_VI.md` and REPORT navigation with a 100-point evidence matrix, PEP8/modularity and scaling evidence. Design now uses a FileSensor-derived LandingValidationSensor at `validate_raw_data` (reschedule, finite timeout, metadata checks after readiness) while preserving six business tasks. Added Airflow 3.3.1 DeadlineAlert/SyncCallback design distinct from hard timeouts and freshness monitoring; acceptance cases extended through T28.
- Verification: illustrative Python AST parse passed; R01–R18 and T01–T28 coverage and rubric total checked; `git diff --check` passed. Read official 3.3.1 Deadline Alerts and standard-provider FileSensor APIs plus book custom-sensor pages 210–212. Runtime code unchanged; sensor/deadline behavior is proposed, not tested implementation.
- Next: implement the documented phases when requested; review configurable business thresholds/resource budgets during implementation. No remaining Kafka clarification blocker, no commit/push, no changes to user `.env.example` or datasets. Older design notes below describe the superseded scope checkpoint.

## Assignment DAG design - 2026-09-28

- Added `docs/DAG_DESIGN_VI.md` (20 sections) and replaced the empty REPORT outline with a report navigation/status page. The Vietnamese design traces assignment sections 1–5 to the six-task DAG, explains book-backed decisions with printed/PDF page references, and specifies time/data/task contracts, partitioned Parquet, order-total anomaly checks, RFM business/churn labels, publication manifests, resource profiles, failure recovery, tests and presentation deliverables.
- Inspected the DOCX paragraphs/tables, embedded equations and architecture image; extracted the supplied 513-page book to temporary storage and read relevant sections. Main assignment requires retail batch; the overview image and section 6 rubric describe a different Kafka/Streaming/recommendation pipeline. Optional clarification was requested; the document explicitly assumes sections 1–5 for the primary design and lists unresolved streaming requirements separately.
- Current-code gaps are proposals, not completed changes: daily landing, year/month partitions, invoice-total statistics, churn proxy, manifest publication and real DAG operators still require implementation. No DAG/scripts/Compose/source data were changed or new runtime jobs run. Existing user `.env.example` remains untouched.
- Verification: Python AST parse of the illustrative DAG block passed; six relative source links resolved; 20 main sections and complete R01–R18/T01–T24 matrices checked; `git diff --check` passed. Official Airflow 3.3.1 and Spark provider 6.3.2 docs were consulted for compatibility. CSV read-only check confirmed 541,909 rows and dates 2010-12-01 08:26 through 2011-12-09 12:50.
- Risks/next action: resolve rubric mismatch, review business thresholds and snapshot-storage tradeoff, then implement phases P1–P8 under a new implementation task. Atomic file replacement on the Windows bind mount and concurrent Spark capacity remain acceptance tests, not verified guarantees. No commit/push.

## PySpark anomaly detector - 2026-09-23

- Implemented `scripts/pyspark_anomalies.py`, focused Spark tests, README usage, and an ignore rule for generated date partitions. Six business checks and three exact upper IQR checks retain every pre-cutoff source row and explain each status. No Airflow wiring, source repair, or fraud labeling.
- Verification: Docker `python -m pytest -q -p no:cacheprovider /opt/airflow/tests/test_pyspark_clean.py /opt/airflow/tests/test_pyspark_rfm.py /opt/airflow/tests/test_pyspark_anomalies.py` -> 50 passed, one upstream pandas warning. Direct `spark-submit --master local[2] --driver-memory 3g` with run-date 2011-12-10 -> exit 0, 536,641 rows in the fresh ignored `data/audit/anomaly_results_verification_2026-09-23/run_date=2011-12-10/`. Written Parquet read-back confirmed the exact multiset of eight original columns and nine checks per row. Input directory SHA-256 before/after: `cb1647b8f07eec5bd2fbb772aaff61b3739ef6efa4a2b5a034facced6dd0c55c`. `python -m py_compile scripts/pyspark_anomalies.py tests/test_pyspark_anomalies.py` and `git diff --check` passed.
- Results: 40,051 unique rows have at least one flag. Business rule counts match the notebook exactly (0 cancelled with nonnegative quantity, 1,336 negative quantity outside C, 2 negative prices, 395 zero prices in priced invoices, 2,115 all-zero invoices, 0 multiple customer IDs). Quantity IQR matches at 24,869; Spark price IQR is 7,607 versus notebook 7,588, and line-value IQR is 26,637 versus 26,639. The 19 and 2 row differences are observations exactly on floating-point fences: Spark and pandas quartile calculations differ by about one unit in the last decimal place for stock codes 85025C, 23272, 16014, and 16012. The eligible row counts match.
- Risks and next action: Review flags are retrospective candidates without validated labels. The default 1 GB Spark heap ran out of memory on full data; the documented two-worker, 3 GB setting passed with Docker reporting about 8 GB available. The unrelated host structure test still fails because `.env.example` is absent. The earlier EDA notebook, guide, and archived EDA plan were committed and pushed separately as `3a0a9a6`; detector publication was requested on 2026-09-23. Next: review detector results and wire the DAG only under its own task.

## Detector plan publication - 2026-09-23

- Saved detailed PySpark detector plan, now archived at `.agent/plans/archive/2026-09-23-pyspark-anomalies.md`; documented the accepted retrospective cutoff and scope in DECISIONS, TODO, and this handoff. At that planning checkpoint, implementation was pending.
- Publish only these four detector-planning state files. The existing EDA notebook, Vietnamese guide, and archived EDA plan remain separate local changes. Plan content and `git diff --check` verified before publication.

## Interpretation guide - 2026-09-23

- Added `notebooks/EDA_Anomalies_Guide_VI.md`: detailed Vietnamese A-G walkthrough, current output figures, examples, denominators, keyword correction, IQR eligibility, case-review limitations and proposed detector output. Source is the executed notebook after StockCode correction; no notebook/data changes or rerun needed.
- Verification: checked section coverage, local notebook link, UTF-8 text, numeric examples against saved output and `git diff --check`. No commit/push.

## Description keyword correction - 2026-09-23

- Fixed anomalies EDA keyword inference: full-word patterns plus same-StockCode product-name references from at least two distinct positive, non-cancelled sales invoices, excluding known services/special codes. All qualifying name variants are retained. Product-name matches are separated from unresolved no-reference matches; remaining keyword flags are hypotheses, not confirmed stock operations.
- Regression: StockCode 85084 / HOLLY TOP CHRISTMAS STOCKING no longer matches stock_words. Embedded fixture covers names with keyword CHECK, damage/stock notes, another code sharing a name, missing description/code, and repeated lines in one invoice.
- Missing-ID counts after correction: stock_words 246 (previous 551), damage_words 163 (previous 1,436), loss_words 20 (previous 129), coding_words 31. Keyword product-name matches 293; unresolved references 28. Historical counts in earlier reports are superseded.
- Verification: `python -m nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=300 notebooks/EDA_Anomalies.ipynb` exited 0; 11 code cells executed without error, focused regression and existing assertions passed, input SHA-256 unchanged. Notebook schema/syntax and `git diff --check` passed.
- Limits: references are inferred from retrospective paid sales, not an authoritative product catalog. No source data, cleaning, RFM or detector changes; no commit/push. Next: review the newly displayed name/reference evidence before selecting detector rules.


## Anomalies EDA completed - 2026-09-22

- Created and executed `notebooks/EDA_Anomalies.ipynb` in Vietnamese. PyArrow reads existing Parquet; pandas/matplotlib provide analysis. No detector, cleaning, RFM, Airflow or input data changes.
- Current Parquet: 536,641 rows; 135,037 missing CustomerID (25.16%), across 3,710 entirely unidentified invoices; no mixed-ID invoices observed. Of missing-ID rows, 126,009 qualify for at least one product IQR check; 10,102 exceed at least one upper 3-IQR fence (review candidates, not verified anomalies).
- Reconciliation: CSV after deduplication and Parquet agree in row count and the multiset of seven non-Description columns. Full-row comparison reports 2,540 differing combinations; paired examples show different quoting in Description. Do not claim identical input content or silently repair it.
- Verification: `python -m nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=300 notebooks/EDA_Anomalies.ipynb` -> exit 0, all 10 code cells executed, no error outputs. Notebook assertions verify IQR boundaries, eligibility, cohort/row preservation, original columns and unchanged SHA-256 for all input files. Both chart outputs visually inspected. `git diff --check` passed.
- Windows sandbox blocked Jupyter secure connection-file creation; authorized execution outside sandbox succeeded without ACL changes. ZMQ emits a harmless Windows event-loop warning.
- Independent source review corrected boundary-test fixtures, a string syntax issue and all-zero-invoice eligibility with missing prices; final fresh-kernel execution passed after fixes. Focused tests are embedded in the notebook as approved.
- Next action: review EDA evidence and choose detector rules/baseline scope. IQR is retrospective; business description flags are hypotheses. No precision/recall claim or production threshold acceptance. Existing sync notes preserved; no commit/push.


## Latest repository sync - 2026-09-22

- Pulled `origin/main` with `git pull --ff-only origin main`: fast-forward from `be75e6d` to `b260d6b` (8 files updated).
- Verification: `git rev-list --left-right --count HEAD...origin/main` returned `0 0`; `git diff --check` passed; working tree was clean immediately after the pull.
- Only this sync record and its TODO entry were added locally afterward. No runtime tests were run for this Git-only task.
- Risks and next action: incoming application behavior is not verified in this session; run focused tests before executing the updated jobs.

## Current state

The collaborative project scaffold and local-mode PySpark Docker configuration are complete. On `main`, the EDA notebook explains the zero-price records, and the notebook plus `scripts/pyspark_clean.py` use stable business types before writing separate RFM-ready and anomaly Parquet datasets. The RFM output now excludes known service and fee lines; the anomaly output retains them.

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
- Standardized `CustomerID` and `InvoiceNo` as identifiers (`string`), `InvoiceDate` as a datetime/timestamp, `Quantity` as an integer, and `UnitPrice` as a floating-point value in both pandas EDA and PySpark.
- Replaced Spark CSV schema inference with an explicit transaction schema and regenerated both Parquet outputs with the new schema.
- Normalized the DAG task IDs to the assignment names while keeping tasks as placeholders.
- Excluded `POST`, `M`, `DOT`, `BANK CHARGES`, `C2`, and `PADS` stock codes, plus exact `PACKING CHARGE` and `NEXT DAY CARRIAGE` descriptions, from RFM only. Matches ignore case and surrounding spaces; product rows in the same invoice remain.
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
- Notebook JSON/syntax and full-data pandas type check passed: 541,909 rows, 0 invalid dates, and the five requested columns have the intended dtypes.
- `docker compose run --rm --no-deps airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_pyspark_clean.py` -> 4 passed; the Parquet round-trip test confirms both output schemas exactly match `TRANSACTION_SCHEMA`.
- Full-data Parquet regeneration via Docker `spark-submit` completed with exit code 0; verification found `RFM=392692` rows and `anomalies=536641` rows, with matching string/timestamp/int/double schemas and 0 null parsed dates.
- `git diff --check` passed after the schema changes; only line-ending notices were emitted.
- `docker compose run --rm --no-deps airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_pyspark_clean.py` -> 5 passed, 1 upstream PySpark pandas warning.
- `docker compose run --rm --no-deps airflow-scheduler bash -c 'spark-submit /opt/airflow/scripts/pyspark_clean.py --input /opt/airflow/data/raw/data.csv --rfm-output /opt/airflow/data/curated/RFM.parquet --anomalies-output /opt/airflow/data/audit/anomalies.parquet'` -> exit 0; regenerated both local Parquet outputs.
- Direct Spark read of regenerated outputs -> `RFM_ROWS=391057`, `RFM_EXCLUDED_ROWS=0`, `ANOMALIES_ROWS=536641` (unchanged anomaly row count). RFM has 1,635 fewer rows than the previous output.
- `git diff --check` passed for the service-line update; only line-ending notices were emitted.

## Unresolved risks

- The Docker image builds and PySpark runs, but the complete Airflow/PostgreSQL service stack has not been initialized or smoke-tested.
- The anomaly IQR multiplier 3 is provisional and has not been validated against labels.
- Cell 15's seven reason groups are keyword-based interpretations of free-text `Description` values; 51 rows remain explicitly classified as unclear rather than being over-interpreted.
- The remaining zero-price rows do not contain a definitive reason label; the new cell distinguishes evidence-backed invoice contexts and explicitly treats gifts/promotions versus missing prices as unresolved possibilities.
- The four invoice-level explanations for the seven suspicious rows remain evidence-based hypotheses because the source data has no explicit reason field.
- Pytest created an inaccessible ignored directory named `pytest-cache-files-phri2efg`; removal failed with `Access is denied`, so its ACL was not altered.
- PySpark 4.2.0 emits an upstream warning that some features may not fully support pandas 3.x; this job does not use pandas APIs.
- Host Spark 3.5.9 cannot commit Parquet on Windows because the local Hadoop installation lacks `hadoop.dll`; use the Docker workflow, which completed successfully.
- `.env.example` is currently deleted in the working tree by an unrelated change and was intentionally not restored as part of this task.
- The complete Docker test command reaches 4 passing Spark tests but its repository-structure test fails because the container mounts only `scripts/`, `tests/`, and `data/`; the host structure check also identifies the already-missing `.env.example`.

## Next action

Review the anomaly audit output, then wire the existing cleaning, RFM, and anomaly jobs into the DAG under a separate task. Raw-data validation and the full Airflow/PostgreSQL smoke test remain open.
