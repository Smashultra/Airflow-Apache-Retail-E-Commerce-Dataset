# Current Handoff

## Two-tier anomaly rule base: review fix pass PAUSED - 2026-10-08

- Owner: Claude; branch `feature/anomaly-rule-base`. All 8 plan tasks are done (see entry below). A fresh whole-branch review returned "with fixes". The fix pass is **in progress, paused at the user's request**.
- Done and committed in this pause commit:
  - Not-applied checks no longer carry evidence (value_at_risk, context_level, reference_*, robust_z, fold_change). Test: `test_not_applied_checks_carry_no_evidence`.
  - The customer-level quantity baseline now needs a stock median from >= min_samples identified lines, so a rare stock no longer compares to itself. Test: `test_customer_level_requires_reliable_stock_median`.
  - Quantity abs/sum is cast to double, so Quantity = -2147483648 no longer overflows under ANSI. Test: `test_extreme_negative_quantity_does_not_overflow`.
  - Robust-z gate coverage with a dispersed baseline. Test: `test_robust_z_gate_with_dispersed_baseline`; a mutation check confirmed it fails when the gate is disabled.
- Verification so far: the 4 new tests went RED then GREEN. `tests/test_pyspark_anomalies.py` ran with 17 passed and 2 fixture-caused failures; both fixtures are fixed, and `-k "hierarchy or overflow"` now gives 2 passed. The full anomaly suite has not been re-run in one go since the fixture fix.
- Remaining steps, about 35-40 min:
  1. Reproducibility (review Important #4): add the E8 follow-up (no-purchase cancels by fee share/month/description) and E9 (lines per invoice, web invoice value/day) to `scripts/anomaly_evidence.py`. Add the MAD=0 %, low %, fold p10/p50/p90 and price-direction tables to `scripts/anomaly_summary.py` R5.
  2. Run the full Docker suite (README command plus `tests/test_anomaly_evidence.py`).
  3. Re-run full-data `pyspark_anomalies.py` and `anomaly_summary.py` (commands in doc section 9). The review fixes change the customer-level counts, so **doc section 9 numbers are stale until re-run**.
  4. Update doc sections 9 and 8 and the ledger `.superpowers/sdd/2026-10-08-anomaly-rule-base/progress.md` (Final: lines).
  5. Commit, then run finishing-a-development-branch. Do not merge or push without the user's request.
- Deferred minors from the review:
  - `assess_orders` derives its channel with `F.trim` instead of `_normalized`, so edge whitespace IDs can disagree with the row channel.
  - The doc says |ΣQ| but the code compares Σ|Q| for unmatched_return.
  - There is no test for the Task 7 `outside_scope` order ruling.

## Two-tier anomaly rule base - 2026-10-08

- Owner: Claude; branch: `feature/anomaly-rule-base` (from `main` 57e6afe). Plan `.agent/plans/archive/2026-10-08-anomaly-rule-base.md`. Not pushed or merged.
- `scripts/pyspark_anomalies.py` now classifies rows (`record_type`) and invoices (`channel`: identified, retail_web via DOT, retail_other, internal), then runs 14 checks: 7 tier-1 data rules (ISO/IEC 25012 / DAMA) and 7 tier-2 business rules (contextual quantity/price deviation with customer -> channel robust baselines, zero-price lines, stock counts, accounting entries, unmatched returns). Each flag has value_at_risk (GBP) and severity. `assess_orders` moved here from `retail_pipeline.py` with customer -> channel robust baselines (replaces global mean + 3 sigma). CLI `--iqr-multiplier` replaced by `--z-threshold` (3.5).
- Full documentation, evidence (E1-E9), rules and results: `docs/ANOMALY_DETECTION_VI.md`. Evidence script `scripts/anomaly_evidence.py` (pandas), result summary `scripts/anomaly_summary.py` (Spark).
- Verification: Docker `python -m pytest -q -p no:cacheprovider tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py tests/test_anomaly_evidence.py` -> 94 passed; host structure test passed (called directly, host has no pytest). Full-data clean + anomaly spark-submit exit 0 (252 s); 536,641 rows preserved; record_type/channel counts match independent pandas evidence exactly; 46,933 rows and 54 invoices flagged.
- Risks: contextual rules over-flag low-value normal behaviour (83-99.7% of their flags have MAD = 0; price flags mostly quantity-tier pricing) — recalibration options in doc section 9.3 await user approval. DAG `detect_anomalies` stage not re-run end-to-end through Airflow (stack not initialized this session). Handbook and `docs/DAG_DESIGN_VI.md` still describe the old nine checks / mean + 3 sigma. Docker image was built with `--add-host` pinning PyPI to a fast CDN edge because two Fastly edges served ~40 KB/s; normal `docker compose build` works when the edges are healthy.
- Next action: user decides on section 9.3 recalibration, then review/merge the branch.

## Online Retail guide without embedded images - 2026-10-03

- Owner: Codex; branch: `main`. Inspected saved outputs: EDA_Anomalies.ipynb has two image outputs; EDA_RFM_Parquet.ipynb has seven. Neither corresponding guide embeds or links image files. Following the user's request, removed all nine image embeds from EDA_Online_Retail_Guide.md and deleted `notebooks/assets` after verifying its resolved workspace path and contents.
- Replaced figure captions/numbers and assets instructions with explanations pointing to the relevant notebook sections. Preserved all 12 sections, numeric tables, cleaning additions and histogram/boxplot interpretation. This supersedes the previous conversion entry's image-delivery requirement.
- Verification: PowerShell here-string `python -` assertions passed: no image/asset references or orphan numbered captions, 12 numbered sections, resolving local links, Python block syntax, UTF-8/whitespace checks and assets absent. SHA-256 matches all three notebooks and both reference guides against `logs/eda_guide_no_images_hashes.json`. `git diff --check` passed with line-ending notices only. No runtime/data changes or notebook reruns; no commit/push.
- Next action: read the guide alongside the notebook for plots; no remaining work for this request. The older image-specific verification helper in logs records the prior delivery and is no longer applicable.

## Online Retail EDA Markdown guide - 2026-10-03

- Owner: Codex; branch: `main`. Converted the user-provided `notebooks/Tong_hop_EDA_Online_Retail.docx` into `notebooks/EDA_Online_Retail_Guide.md`, preserving its 12 sections, tables, code and eight figures under `notebooks/assets/EDA_Online_Retail_Guide/`. Deleted that DOCX only after verification, as explicitly requested. No commit/push.
- Added the omitted histogram from notebook section 4 (guide section 6), log-axis/boxplot/quantile explanations and a section mapping. Expanded guide section 4 with CSV-verified zero-price groups (2,515 = 1,336 + 772 + 367 + 7 + 33), seven customer-linked rows across four entirely zero-price invoices, negative-price context and filter reconciliation. Clarified revenue/cohort denominators, incomplete cleaning and interpretation limits; verified formerly reference-only special codes/invoices against local CSV.
- Verification: `python logs/verify_eda_guide.py` passed before and after DOCX deletion: 12 sections, nine valid PNGs, resolving local links, original eight image bytes preserved, histogram bytes matching saved notebook, two executable Python blocks, cleaning counts, quantiles and customer/country metrics. SHA-256 confirms notebook and raw CSV unchanged. `git diff --check` passed (line-ending notices only). Verification helper and source hashes are local ignored files in logs; histogram visually inspected.
- Limits: notebook was not rerun or edited; it still reads `data.csv` relative to the kernel directory. Guide explains the actual repository path and distinguishes added CSV analysis from existing notebook output. Markdown images require the accompanying assets directory. Unrelated active Word-handbook plan untouched.
- Next action: open the Markdown preview beside `EDA_Online_Retail.ipynb`; no remaining work for this conversion request.

## Complete local snapshot published to main - 2026-10-03

- Owner: Codex; workspace branch: `initial-setup`; destination: `main`, explicitly corrected by user. `git fetch origin` succeeded; `git merge-base --is-ancestor origin/main HEAD` passed before push.
- Verification: `git push origin HEAD:main` succeeded as fast-forward (4fe270f -> 7edc7ef), publishing the complete committed local snapshot and existing branch history. No force push, branch checkout, local file deletion or dataset changes. Prior source/notebook checks and limitations remain as recorded below.
- Next action: publish this final bookkeeping commit to main and verify HEAD matches origin/main with a clean workspace. No remaining implementation work in this publishing request.

## Local snapshot pushed successfully - 2026-10-03

- Owner: Codex; branch: `initial-setup`. User-authorized complete nonignored local snapshot committed as ba2bf05 (40 changed files), including notebook outputs, renamed guide, source/tests, all comparison Parquet/CRC/success markers and demo fixtures. Existing nine unpublished commits were also included in the branch push. Ignored files stayed local; nothing deleted.
- Verification: `git push origin initial-setup` succeeded (8254828 -> ba2bf05); `git status --short --branch` showed no modified/untracked files and HEAD matched origin/initial-setup. Syntax/notebook/credential scan results and runtime limitations are recorded in the preparation entry below.
- Next action: push this final TODO/HANDOFF bookkeeping commit, then verify clean status and matching remote HEAD. Main branch was not updated. No further publishing requested.

## Publish local snapshot preparation - 2026-10-03

- Owner: Codex; branch/target: `initial-setup`. User explicitly authorized all modified and untracked local files, including generated comparison Parquet and fixtures visible to Git; ignored environment/raw/runtime files are excluded. Preserve exact user file contents.
- Verification: Python AST parsing passed for the three modified Python files; notebook JSON/code compile and saved-error checks passed for both notebooks; common credential-pattern scan passed. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` returned 1 skipped because PySpark is unavailable. `git diff --check` reports five existing trailing-whitespace lines in scripts/pyspark_rfm.py, deliberately preserved in this snapshot. `git fetch origin` succeeded.
- Limits: user local RFM entry point lacks upstream --pipeline-context dispatch, so official DAG compatibility is not claimed. Guide currently named EDA_RFM_Guide.md; notebook links still refer to previous _VI name. This publishing task preserves those local contents rather than revising them.
- Next action: stage all nonignored files, commit, push origin initial-setup and verify matching remote HEAD. No user data deletion.

## RFM notebooks organized and interpretation guide - 2026-10-03

- Owner: Codex; branch: `initial-setup`. Organized notebooks/EDA_RFM_Parquet.ipynb and notebooks/comparison_revised.ipynb into A-G reading flows with central paths/date configuration, retained exploration and refreshed outputs. Added notebooks/EDA_RFM_Guide_VI.md covering both notebooks, formulas, denominators, transition, score evidence and interpretation limits.
- Replaced hard-coded EDA cluster names with median-profile labels matching comparison; added EDA_Segment while preserving any pipeline Segment. Fixed heatmap direction (low Recency/high F,M green), deterministic samples and input/cohort/date checks. Removed blank/redundant cells and stale fixed conclusions; added optional saved k-score plot.
- Verification: Python stdin using nbformat.validate, compile and nbclient.NotebookClient(...).execute() from notebooks directory passed: EDA 17 code cells, comparison 9, no error outputs. SHA256 before/after matched all 17 source Parquet/score CSV files. Focused AST-extracted semantic_mapping tests passed for ID permutation invariance and tied moderate Recency rejection in both notebooks. Saved heatmap visually inspected after correcting UTF-8 writing.
- Backup of original notebooks: C:/Users/Lenovo/AppData/Local/Temp/rfm-notebooks-20261003-8bjem3i2. No datasets/source jobs changed, no Spark retraining, commit or push. Existing unrelated local changes preserved.
- Risks: pandas loads datasets into RAM; legacy local paths differ from official versioned outputs; k-score CSV lacks matching seed/fingerprint proof. Labels support k=2,3,4 only and do not establish churn/new customer status or stability. Kernel emitted nonfatal Windows IPython permission/debugger/zmq warnings; execution completed.
- Next action: read the guide and reopen saved notebooks; use Restart Kernel / Run All after changing inputs. The unrelated active Word-handbook plan remains untouched.

## Main pull preserving local work - 2026-10-03

- Owner: Codex; branch: `initial-setup`. Fetched origin and integrated origin/main `4fe270f` via merge `64261d7`. No push.
- Restored all 39 original local files outside project notes byte-for-byte from backup. Local and upstream notes retained below.
- Verification: Python SHA256 manifest check passed for all 39 files; `git merge-base --is-ancestor origin/main HEAD` passed; `git diff --name-only --diff-filter=U` returned no paths.
- Backup: C:\Users\Lenovo\AppData\Local\Temp\retail-pull-20261003-wbqt8a4c; stash retained. Ignored datasets were not touched. Runtime tests not run for this Git synchronization.
- Risk/next action: local RFM source/tests/helper remain the user versions and may need compatibility review with the incoming pipeline before execution.

## Upstream handoff

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

## Preserved local handoff history

# Current Handoff

## Comparison notebook root initialization - 2026-09-28

- Owner: Codex; branch: `initial-setup`. Supplied standalone PROJECT_ROOT initialization for the user's comparison cell. `notebooks/comparison.ipynb` is zero bytes on disk, so the unsaved editor cell could not be patched.
- Verification: PowerShell here-string piped to `python -` checked ancestor-based repository discovery from the repository root and notebooks directory; both passed. pandas read_parquet loaded k=2, k=3 and k=4 comparison directories successfully, each with 4,334 rows.
- Next action: paste the initialization before comparison_path in the open notebook and rerun the cell. No dataset or source changes, commit or push.

## KMeans files published to main - 2026-09-24

- Owner: Codex; target: main. Commit 0185d1e pushed successfully (fbfefbd -> 0185d1e). Contains exactly scripts/pyspark_rfm.py, tests/test_pyspark_rfm.py, scripts/select_kmeans_k.py and data/analytics/k_selection/2011-12-10/k_scores.csv.
- Used isolated worktree C:/Users/Lenovo/AppData/Local/Temp/retail-kmeans-publish-20260924 from latest origin/main, preserving incoming anomaly changes and original workspace. Trailing whitespace normalized in committed Python copies only; AST equivalence with local originals verified.
- Verification: three Python AST checks passed; CSV contains valid k=2..10, positive WSSSE and silhouette in [-1,1]. git diff --check and git diff --cached --check passed; staged paths were exactly the four authorized files. python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py reported 1 skipped (host lacks PySpark).
- Risks/next action: post-fix Spark runtime tests remain unverified here; run in Docker. Published helper reflects current local user version (does not explicitly configure UTC). No further publishing needed. Notebook, full data and local notes excluded.


## RFM test failure diagnosis - 2026-09-24

- Owner: Codex; branch: `initial-setup`. User-provided Docker log reports 41 passed and 17 failed; all 17 failures are NameError for LongType in validate_rfm_output. Current source confirms the missing import.
- Added only LongType to pyspark.sql.types imports. Existing int/bigint and invalid-cluster tests already cover the affected path; no test changes needed.
- Verification: Python AST syntax checks for source/tests and explicit AST assertion of IntegerType/LongType imports passed. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` locally reports 1 skipped because PySpark is unavailable; full post-fix runtime result remains unverified.
- Next action: rerun the same Docker pytest command with --entrypoint python. No data changes, commit or push.


## KMeans run commands - 2026-09-24

- Owner: Codex; branch: `initial-setup`. Provided Docker commands in dependency order: clean, select k, then RFM with explicit k=4. Existing curated data allows skipping clean.
- Verification: inspected CLI flags/defaults and Compose service/mounts with rg/Get-Content; commands match current files. No jobs or tests executed.
- Risks: clean overwrites prepared outputs; RFM overwrites the selected date partition. Next action: user runs commands and reviews k_scores.csv before choosing k. No source/data changes or commit/push.

## KMeans review fixes - 2026-09-23

- Owner: Codex; branch: `initial-setup`. Added a distinct log-feature-vector guard before KMeans fitting and an occupied-cluster count guard after fitting. Removed obsolete ranking documentation.
- Validator and writer now require explicit k; Cluster must be int/bigint with values in [0, k). Updated existing callers. Subset writes remain allowed; full occupied-cluster validation is performed by segmentation, not the writer.
- Added focused tests for insufficient distinct vectors, collapsed model predictions, missing/null/negative/out-of-range/fractional/wrong-type Cluster values, accepted boundaries, required k, and rejection before writing.
- Offline k-selection Spark session now uses UTC and ANSI mode like production. No notebook or dataset changes; no commit/push.
- Verification: Python ast.parse passed for scripts/pyspark_rfm.py, scripts/select_kmeans_k.py and tests/test_pyspark_rfm.py. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` reported 1 skipped because PySpark is unavailable. Docker is absent from PATH; Java 17 is present. Removed trailing whitespace in the two edited RFM files after diff checking.
- Risk/next action: runtime Spark tests still require the project Docker environment. Callers outside this repository must now supply k to validate_rfm_output/write_rfm.


## Revised KMeans and cluster EDA review - 2026-09-23

- Owner: Codex; branch: `initial-setup`. Reviewed source/tests, all 35 notebook cells including saved plots, offline k-selection helper and saved CSV. No source, test, notebook, or dataset changes.
- Findings: customer-count guard does not guarantee k distinct feature vectors/occupied clusters; Cluster validation permits fractional IDs and omits upper bounds when k is absent; obsolete rank docstring contradicts implementation. Daily refitting requires model-version-specific names. Offline helper does not set the UTC timezone used by production.
- Direct pandas read of current dated Parquet agrees with notebook cluster sizes: 0=1050, 1=691, 2=1528, 3=1065; 4334 unique customers, no nulls. Median R/F/M: 0=82.5/3/1018.60; 1=10/10/3901.81; 2=159.5/1/236.03; 3=17/3/812.68. Monetary shares: 17.95%, 65.53%, 4.60%, 11.92% respectively.
- Suggested names for this snapshot: 0=repeat customers needing reactivation; 1=active high-value customers; 2=low-value infrequent customers; 3=active mid-value customers. Avoid asserting new customers or confirmed churn from RFM alone.
- k_scores.csv: silhouette k=2 is 0.6238, k=3 is 0.5005, k=4 is 0.4918. Four clusters are an interpretable analytical choice, not the silhouette maximum; multi-seed stability remains untested.
- Verification: Python ast.parse passed for the three reviewed scripts/test files and every notebook code cell; no saved notebook errors. Read-only pandas groupby/quantile checks passed. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` reported 1 skipped (PySpark unavailable), so Spark runtime remains unverified. Temporary extracted plot images removed after inspection.
- Next action: user considers review fixes, adds profile tables to EDA, and runs RFM tests in Docker. No commit or push.


## K-means RFM review - 2026-09-23

- Owner: Codex; branch: `initial-setup`. Reviewed current local RFM implementation, cleaning contract, tests, and notebook evidence. No source, tests, notebooks, or datasets changed.
- Findings: rank-based business names do not establish behavioral segment meaning; daily refitting does not ensure stable segment definitions; k=5 selection evidence is absent from inspected repository/notebooks; validator does not enforce cluster/rank/name mapping and writer omits k; row-count guard misses insufficient distinct feature vectors; old rule-based tests and single-customer fixtures are incompatible with K-means defaults. Small-population warning in main is unreachable for fewer than five customers.
- Verification: Python ast.parse passed for scripts/pyspark_rfm.py and tests/test_pyspark_rfm.py. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` reported 1 skipped because PySpark is unavailable; runtime behavior remains unverified. Docker not found on PATH.
- Read-only pandas/NumPy exploration of curated Parquet before 2011-12-10: 4,334 customers; 4,333 distinct RFM vectors; raw skew R/F/M = 1.243/11.949/19.578, log1p skew = -0.326/1.214/0.398; log F/M correlation = 0.808. Exploratory Monetary uses floating arithmetic, not a replacement for Spark decimal verification. Notebook keyword scan initially hit console encoding error; rerun using escaped output succeeded and found only a future K-means suggestion, no k-selection experiment.
- Next action: revise segment interpretation, validate k experimentally, update output contract and K-means tests, then verify in Docker. Review only; no commit or push.

## RFM segmentation pushed to main - 2026-09-23

- Owner: Codex; target: main. Commit 5768d56 contains only scripts/pyspark_rfm.py and tests/test_pyspark_rfm.py. Includes FM_score, Segment, output validation, segment tests, and the four missing segmentation calls in existing tests. Preserved the user's commented historical code; normalized trailing whitespace in committed copies only.
- Verification: Python ast.parse passed for both files; whitespace-token comparison with local originals passed. git diff --check and git diff --cached --check passed; staged paths were exactly the two authorized files. python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py reported 1 skipped because PySpark is unavailable; updated runtime tests remain unverified here.
- git push origin HEAD:main succeeded (cae0638 -> 5768d56). Worktree retained at C:/Users/Lenovo/AppData/Local/Temp/retail-rfm-segments-20260923. Original working files and branch preserved.
- Next action: run the updated test module in Docker. No further publishing required.

## RFM partition filtering pushed to main - 2026-09-23

- Owner: Codex; target: main. Commit cae0638 contains only scripts/pyspark_rfm.py and tests/test_pyspark_rfm.py: year/month history filtering and two focused tests. Removed trailing whitespace only in the committed copies.
- Verification: Python AST parsing passed for both files and AST equality confirmed no semantic change from local copies. git diff --check and git diff --cached --check passed; staged paths were exactly the two requested files. python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py reported 1 skipped because PySpark is unavailable, so runtime tests remain unverified in this environment.
- git push origin HEAD:main succeeded (b260d6b -> cae0638). Isolated worktree retained at C:/Users/Lenovo/AppData/Local/Temp/retail-rfm-main-20260923. Original initial-setup working tree and unrelated files preserved.
- Next action: run the RFM tests in Docker for runtime verification. No further publishing required.

## RFM run guidance - 2026-09-23

- Owner: Codex; branch: `initial-setup`. Explained Docker commands in order: RFM tests, cleaning, RFM execution. No source or data changes.
- Verification: Python ast.parse on scripts/pyspark_clean.py, scripts/pyspark_rfm.py and tests/test_pyspark_rfm.py passed for all 3 files. Checked CLI flags and mounts against source and Compose. Runtime tests/jobs not run.
- Risks: cleaning overwrites prepared outputs; RFM replaces the selected date partition. Next action: user runs each command after the preceding step succeeds.

## RFM pushed to main - 2026-09-22

- User clarified the destination is `main`. Cherry-picked `8254828` onto current `origin/main` in isolated worktree `C:/Users/Lenovo/AppData/Local/Temp/retail-rfm-main-20260922`, producing `b260d6b`; `git push origin HEAD:main` succeeded.
- Verification: `git diff --name-only origin/main HEAD` before push listed only the two RFM Python files; `git diff --check origin/main HEAD`, Python AST parsing for both files, and `git diff --exit-code 8254828 HEAD -- scripts/pyspark_rfm.py tests/test_pyspark_rfm.py` passed.
- Notebook excluded; original working tree remains on `initial-setup` with local files preserved. Runtime tests remain unverified due to missing PySpark as recorded below. No further publishing action needed; isolated worktree retained.

## RFM source and tests pushed - 2026-09-22

- Owner: Codex; branch: `initial-setup`. User authorized committing and pushing the two RFM Python files, excluding `notebooks/EDA_RFM_Parquet.ipynb`.
- Commit `8254828` contains only `scripts/pyspark_rfm.py` and `tests/test_pyspark_rfm.py`; removed trailing whitespace from the test file, with no logic changes. `git push origin initial-setup` succeeded, also publishing the branch's existing main-integration history.
- Verification: `python -m py_compile scripts/pyspark_rfm.py tests/test_pyspark_rfm.py` and `git diff --cached --check` passed. `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py` reported 1 skipped (exit 5) because PySpark is unavailable; runtime behavior was not verified this turn.
- Notebook, local fixtures, and project-note changes remain uncommitted. Next action: run the full RFM test module in the project Docker/Linux environment when available.

## Refreshed RFM notebook review - 2026-09-21

- Owner: Codex; branch: `initial-setup`. Read-only notebook review; no source, notebook, or dataset changes.
- Verification: Python stdin JSON/AST scan of all 46 cells found no syntax errors or saved error outputs; saved execution counts run through 36. Direct pandas Parquet read confirms saved counts (391,057 transactions, 18,402 invoices, 4,334 customers) and RFM describe statistics. Output has zero nulls or duplicate customers. `git diff --check` passed.
- Previously invalid df.loc[] cell is removed; saved RFM results now show customer 14646 Frequency 72/Monetary 279138.02 and customer 12748 Frequency 206, matching current output.
- Remaining minor notebook issues: introductory cleaning description is outdated; unique InvoiceDate is not the general Frequency definition; printing every StockCode produces excessive output. Next action: optional notebook presentation cleanup. No pipeline rerun, commit, or push.


## Latest RFM review after fee exclusion - 2026-09-21

- Owner: Codex; branch: `initial-setup`. Reviewed saved notebook, RFM implementation/tests, and current Parquet without modifying source, notebook, or datasets.
- Verification: Python stdin pandas/Decimal independent full recomputation matched all 4,334 customer IDs and all eight metrics/score fields with zero mismatches. Input: 391,057 rows, zero nulls/duplicates/cancellations/nonpositive or nonfinite values/excluded fee lines. Output: zero nulls/duplicate customers, total Monetary 8,735,922.64; both datasets have _SUCCESS markers.
- Raw CSV comparison: 541,909 raw rows -> 392,692 eligible rows -> 391,057 after removing 1,635 fee rows. All seven columns excluding Description matched as row multisets; 535 descriptions differ in CSV quote parsing between Spark and pandas (does not affect current RFM metrics). Initial strict UTF-8 read failed; replacement decoding matches Spark behavior for this comparison.
- Notebook findings: saved outputs are stale (392,692 transactions, 4,338 customers); cell 38 (one-based) contains invalid `df.loc[]`; introduction describes outdated cleaning policy. Cell 32 counts unique InvoiceDate rather than distinct InvoiceNo, so it is not a Frequency check. Source and test AST checks passed; notebook syntax scan identified that single invalid cell.
- Current customer 14646: Recency 2, Frequency 72, Monetary 279138.02, score 555. Customer 12748: Recency 1, Frequency 206, Monetary 31650.78, score 555.
- Runtime Spark tests were not run because Docker is not on this environment's PATH. `git diff --check` passed. Next action: remove/complete notebook cell 38, refresh its cleaning description, restart kernel and run all cells; optionally address CSV quote parsing in a separate cleaning task. No commit/push or data regeneration.


## Latest main pull preserving local code - 2026-09-21

- Owner: Codex; branch: `initial-setup`. `git pull --no-rebase --no-edit origin main` integrated `1f07bb1` successfully. Local notes merged with upstream notes.
- Verification: Python SHA256 comparison against the pre-pull backup passed for all 23 local working files outside project notes; `git merge-base --is-ancestor origin/main HEAD` passed; no unresolved conflicts. `git diff --check` passed.
- Backup: `C:/Users/Lenovo/AppData/Local/Temp/retail-pull-6rdl79no`; notes stash retained. No push or data regeneration. Runtime tests not run for this synchronization.
- Next action: run the updated cleaning pipeline when needed; existing Parquet outputs have not been regenerated.


## POSTAGE and Manual RFM assessment - 2026-09-21

- Read-only data assessment; no pipeline, notebook, or dataset changes.
- Verification: Python stdin/pandas read of curated Parquet and customer aggregations before/after excluding normalized descriptions POSTAGE and MANUAL completed successfully. POSTAGE: 1,099 rows, 77,803.96 amount; Manual: 279 rows, 53,419.93 amount. Combined share: 1.4765% of current total.
- Among retained customers, 88 change invoice frequency and 20 change last purchase calendar date; 3 customers have only these lines and disappear on exclusion. Comparison uses all current curated rows.
- Risks: descriptions alone do not establish the business meaning of Manual; large unit price alone is insufficient to classify a bad transaction. Next action: agree a merchandise-only versus total-payment RFM policy before changing filters. No commit or push.


## Full RFM output review - 2026-09-21

- Read the saved EDA notebook: 16 cells, no saved outputs or added RFM analysis cells. Editor changes may be unsaved; notebook was not modified.
- Verification: Python stdin script using pandas and Decimal independently recomputed metrics from all 392,692 curated rows before 2011-12-10, then compared all 4,338 customers against the RFM Parquet output. Zero mismatches in LastPurchaseDate, Recency, Frequency, Monetary, or the three percent-rank scores; combined score strings matched. No duplicate output customers or null output values.
- Customer 14646: last purchase 2011-12-08 12:12:00, Recency 2, Frequency 73, Monetary 280206.02.
- Scope: validates the saved output against current curated input and scoring rules, not the business validity of every raw transaction. Next action: save current notebook cells/outputs for notebook-specific review. No source or data changes, commit, or push.

## Main schema update pull - 2026-09-21

- Owner: Codex; branch: `initial-setup`. `git pull --no-rebase --no-edit origin main` integrated `be75e6d` via merge `ae6384b`, including cleaning column types, tests, fixture, and EDA updates.
- Restored local notes and resolved their TODO conflict. SHA256 comparisons verified all 23 local working files outside project notes unchanged. `git merge-base --is-ancestor origin/main HEAD` and `git diff --check` passed; no unresolved conflicts.
- Backup retained at `C:/Users/Lenovo/AppData/Local/Temp/retail-pull-eea41fc34e454e9887e5ddfe5439a4c9`, plus notes stash. No push or data regeneration; runtime tests not run for this pull.
- Next action: validate updated cleaning output against the local RFM contract when running the pipeline.

## Main pull - 2026-09-21

- Owner: Codex; branch: `initial-setup`. Pulled `origin/main` through `9317688` using `git pull --no-rebase --no-edit origin main`; merge commit `ef3fbe8`.
- Incoming cleaning changes exclude cancellations and non-positive quantity/price from RFM input. Restored local notes and resolved the TODO conflict while preserving both branches' progress.
- Verification: SHA256 checks against the pre-pull backup passed for all 23 local working files outside project notes, including RFM source/tests, EDA notebook, and fixtures. `git merge-base --is-ancestor origin/main HEAD` and `git diff --check` passed; no unresolved conflicts.
- Backup retained at `C:/Users/Lenovo/AppData/Local/Temp/retail-pull-20260921-123ec73d500b4d91967ed443b7f8618e`; notes stash also retained. No push. Runtime tests and data regeneration were not performed for this synchronization.
- Next action: run the updated cleaning job in the configured Docker environment when ready.

## RFM Parquet EDA notebook - 2026-09-20

- Created `notebooks/EDA_RFM_Parquet.ipynb` with pandas loading, repository-relative path discovery, schema/null/duplicate summaries, descriptive statistics, transaction checks, and free EDA cells.
- Verification: executed a Python stdin check using `nbformat.validate`, compiled every code cell, executed all code cells against temporary Parquet generated from `tests/fixtures/sample_transactions.csv`, and asserted the empty-directory guard; all passed.
- Local `data/curated/RFM.parquet` currently reads as zero rows and zero columns. Full-data EDA remains dependent on providing the complete Parquet output; the notebook reports this explicitly. Source data and existing RFM code/tests were not modified.
- Next action: populate the dataset directory or set `PARQUET_PATH`, then run the notebook. No commit or push.

## Main integration preserving local work - 2026-09-20

- Owner: Codex; branch: `initial-setup`. `git pull --no-rebase --no-edit origin main` succeeded, creating merge commit `df4717a`.
- Restored local project notes and resolved their content conflicts while retaining upstream and local progress. RFM source, tests, and fixtures stayed in place throughout the pull.
- Verification: SHA256 comparisons against the pre-pull backup passed for all 22 local working files outside project notes; `git merge-base --is-ancestor origin/main HEAD` and `git diff --check` passed. No unresolved Git conflicts remain.
- Backups remain in `C:/Users/Lenovo/AppData/Local/Temp/retail-before-main-b53bca5a38324051898037a9153ea4e7` and Git stashes. No push performed.
- Next action: continue local RFM work and check compatibility with the upstream cleaning output; runtime tests were not run for this Git synchronization.

## Restore working changes - 2026-09-20

- Applied `stash@{0}` successfully on `initial-setup` after the branch switch was rejected; restored all saved tracked changes and untracked files, including RFM tests and data fixtures.
- Verification: `git hash-object` for both RFM Python files matched their respective `git rev-parse` blob IDs in the stash exactly; `git diff --check` passed.
- The stash remains as a backup. No branch switch, commit, or push occurred. No further Git changes planned.

## Latest pull verification - 2026-09-20

- Owner: Codex; branch: `initial-setup`.
- `git pull --ff-only` succeeded: `Already up to date.`
- Verification: `git status --short --branch` retained the existing local changes; `git rev-list --left-right --count HEAD...@{upstream}` returned `4 0` (ahead 4, behind 0).
- Risks: existing uncommitted work remains local. No commit or push performed.
- Next action: continue existing project work; no further pull action needed.

## GitHub synchronization - 2026-09-20

- Pulled the latest `initial-setup` changes from GitHub, producing merge commit `e6c403e`.
- Preserved the local versions of `scripts/pyspark_rfm.py` and `tests/test_pyspark_rfm.py` through the pull.
- Verification: `python -m py_compile scripts/pyspark_rfm.py tests/test_pyspark_rfm.py` passed; editor diagnostics reported no errors; local branch is ahead of `origin/initial-setup` by 4 commits and has no commits behind it.
- No commit or push performed.

## RFM contract and Parquet I/O tests - 2026-09-17

- Added a focused `validate_curated_contract` test for a missing required column.
- Added a `tmp_path`-based `write_rfm` test that checks the dated partition, a Parquet data file, partition discovery, and replacement of the previous row on a second write.
- Set the shared Spark test session timezone to UTC and made cutoff-sensitive sample timestamps explicitly UTC.
- Verification: `python -m py_compile tests/test_pyspark_rfm.py scripts/pyspark_rfm.py` passed; `python -m pytest -p no:cacheprovider tests/test_pyspark_rfm.py -q -k "not write_rfm"` with local Spark 3.5.9 configured passed 14 tests with 1 deselected in 74.47 seconds.
- The focused I/O test reached the Parquet write but could not complete on this Windows host because the local Hadoop native runtime raises `UnsatisfiedLinkError: NativeIO$Windows.access0`; Docker is unavailable and the installed WSL distribution has neither Java nor PySpark. Run the full module in the project Docker/Linux environment next.
- `git diff --check` passed with line-ending warnings only. No commit or push.

## RFM test consolidation - 2026-09-16

- Merged all RFM tests into tests/test_pyspark_rfm.py with shared imports and one module-scoped Spark fixture. Translated existing test comments into English.
- Removed tests/test_rfm_completion.py after preserving its test cases in the merged module; no RFM implementation changes.
- Verification: `python -m pytest -p no:cacheprovider tests/test_pyspark_rfm.py -q` with local Spark 3.5.9 configured: 13 passed in 59.83 seconds, exit 0. Windows still prints `ERROR: Access denied` after completion.
- Next action remains Docker Parquet round-trip/rerun and full-data verification. No commit or push.

## RFM update - 2026-09-16

- Replaced ntile with percent_rank without CustomerID ordering or a neutral-score override. Constant metrics and singleton populations score 1. Score groups may be uneven.
- Added finite-price checks and a reusable output validator for metrics, score codes, duplicate customers, and cutoff consistency. The writer validates before replacing the selected date partition.
- Preserved the canonical curated schema and daily-partition writing strategy; standardized the RFM script and new tests in English.
- Added tests/test_rfm_completion.py; preserved existing user tests.
- Verification: `python -m pytest -p no:cacheprovider tests/test_pyspark_rfm.py tests/test_rfm_completion.py -q` with local Spark 3.5.9 Python libraries and SPARK_HOME configured: 13 passed in 62.68 seconds, exit 0. Windows printed `ERROR: Access denied` after pytest completed.
- `git diff --check` passed with line-ending warnings only. English-only source check found no non-ASCII characters in the changed script or new test module.
- Remaining: actual Parquet round-trip/rerun verification and full-data execution in the project Docker image. Direct partition overwrite is not guaranteed atomic on failure. Monetary uses four decimal places for operands and two for the customer total.
- No commit or push performed. Earlier state below is historical and may describe checks before this update.

## Current state

The collaborative project scaffold and local-mode PySpark Docker configuration are complete. On `main`, the EDA notebook explains the zero-price records, and the notebook plus `scripts/pyspark_clean.py` use stable business types before writing separate RFM-ready and anomaly Parquet datasets.

## Completed

- Converted every comment, docstring, CLI description/help string, validation message, and log message in `scripts/pyspark_rfm.py` to English without changing its processing logic.
- Renamed the RFM input guard to `validate_curated_contract` and limited it to the five required RFM columns, a canonical curated schema, non-empty input, required values, non-blank identifiers, and positive quantity/price assertions.
- Kept the existing RFM output validation for Recency, Frequency, Monetary, and component scores.
- Added focused Spark tests covering valid curated data, a noncanonical schema, and an unclean numeric value.
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

- `rg -n "[^\\x00-\\x7F]" scripts/pyspark_rfm.py` -> no matches.
- `python -m py_compile scripts/pyspark_rfm.py tests/test_pyspark_rfm.py` -> passed.
- `python -m pytest -q -p no:cacheprovider tests/test_pyspark_jobs.py tests/test_pyspark_rfm.py` -> 1 passed, 1 skipped; the Spark test module was skipped because PySpark is not installed in the host environment.
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
- The new RFM contract tests still need to run inside the project image because the host does not have PySpark installed.
- The future cleaning job must emit the canonical RFM types: string CustomerID/InvoiceNo, timestamp InvoiceDate, integer Quantity, and double UnitPrice.
- Earlier local RFM verification could not use Docker; upstream reports successful Docker cleaning-job verification.
- The anomaly-threshold method is not selected yet.
- Cell 15's seven reason groups are keyword-based interpretations of free-text `Description` values; 51 rows remain explicitly classified as unclear rather than being over-interpreted.
- The remaining zero-price rows do not contain a definitive reason label; the new cell distinguishes evidence-backed invoice contexts and explicitly treats gifts/promotions versus missing prices as unresolved possibilities.
- The four invoice-level explanations for the seven suspicious rows remain evidence-based hypotheses because the source data has no explicit reason field.
- Pytest created an inaccessible ignored directory named `pytest-cache-files-phri2efg`; removal failed with `Access is denied`, so its ACL was not altered.
- PySpark 4.2.0 emits an upstream warning that some features may not fully support pandas 3.x; this job does not use pandas APIs.
- Host Spark 3.5.9 cannot commit Parquet on Windows because the local Hadoop installation lacks `hadoop.dll`; use the Docker workflow, which completed successfully.
- `.env.example` is currently deleted in the working tree by an unrelated change and was intentionally not restored as part of this task.
- The complete Docker test command reaches 4 passing Spark tests but its repository-structure test fails because the container mounts only `scripts/`, `tests/`, and `data/`; the host structure check also identifies the already-missing `.env.example`.

## Next action

Wire `scripts/pyspark_clean.py` into the `submit_pyspark_etl` DAG task, then implement the downstream RFM metrics and anomaly-threshold jobs.
Local follow-up: verify the RFM input contract against the new cleaning output and implement raw-data validation.

## Pull recheck preserving RFM - 2026-09-20

- Owner: Codex; branch: `initial-setup`.
- `git pull --no-rebase origin initial-setup` succeeded: Already up to date.
- Verification: SHA256 comparisons against temporary backups passed for `scripts/pyspark_rfm.py` and `tests/test_pyspark_rfm.py`; both files are unchanged.
- `git status --short --branch`: ahead of origin by 4 commits; existing local changes remain. No push performed.
- Risks: existing uncommitted work remains local. Next action: continue RFM work.
