# Current Handoff

## UI sensor deadlock demo - 2026-10-15

- Added isolated retail_sensor_deadlock_demo.py defining three manual-only single-task DAGs (poke/reschedule/producer), dedicated retail_sensor_demo pool expected to have 1 slot. No business DAG modifications. Sensor waits for per-key hashed file under data/demos/sensor_deadlock; producer creates it. Poke timeout 180 seconds/retries zero bounds resource hold, reschedule interval 15 seconds. UI instructions require sensor running before triggering producer and fresh keys across trials.
- docs/DEMO_SENSOR_DEADLOCK_VI.md covers pool creation, expected resource cycle, timeout recovery, reschedule comparison and existing daily reschedule configuration. Pool not created and no real runs triggered; user performs demo in UI.
- Verification: `docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_sensor_deadlock_demo.py` -> 2 passed in 5.05s (signal isolation, pool/mode/timeout, serialization). No live scheduler deadlock claimed.

## UI idempotency demo - 2026-10-15

- Added BRANCH_REUSED log after successful validation of an existing branch publication. Existing skip-Spark behavior unchanged; no new Param/task. docs/DEMO_IDEMPOTENCY_VI.md demonstrates clearing only successful detect_anomalies in the same all-in-one run, comparing audit_hash/etl_hash/manifest in UI logs. Can follow atomicity recovery directly.
- Scope: published branch snapshot reuse, not whole-DAG or exactly-once Asset effects. Do not clear ETL/upstream or create a new run for this comparison. No real UI demo triggered or data changed.
- Verification: `docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_branch_publication.py` -> 20 passed in 10.69s (Spark mocked); scoped git diff --check passed. Existing user changes preserved.

## UI atomicity demo - 2026-10-15

- Added default-off demo_fail_audit_once Param to both daily DAGs. Shared branch operator injects AirflowFailException after successful Spark and before audit publication only for opted-in manual runs. Run-local demo_audit_failure.json makes failure one-shot; UI Clear audit + downstream recovers without changing Params/code. Published branches bypass injection. No model/data edits or real demo DagRun triggered.
- docs/DEMO_ATOMICITY_VI.md explains all-in-one UI failure/recovery and optional Asset variant. Scope is publication atomicity, not partial Parquet rollback. Failed attempt may leave unadvertised artifacts; clear reruns Spark. Scheduled/backfill unaffected. Existing user edits to prepare_daily_landing.py preserved.
- Verification: `docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_branch_publication.py tests/test_allinone_dag.py tests/test_historical_boundaries.py tests/test_retail_dag.py` -> 45 passed in 14.15s. Spark submit mocked in publication fixture tests; checks no publication/event/completion and old pointer retained, recovery, published retry reuse, default/scheduled bypass. Historical final-day/serialization tests now pass too, completing the prior interrupted date-fix verification. Full-data UI demo not executed.

## Restore bounded training schedule - 2026-10-15

- User explicitly chose historical logical dates over opening the schedule to 2026. Restored training end_date=2011-11-01 UTC, superseding the removal below. Manual business cutoff remains independent. DAG doc_md, SETUP, K selection and architecture guide now explain historical logical dates and avoiding duplicate logical dates between report/train runs.
- Regression checks all five task end_dates and eligible 2011/ineligible 2026 dates. `docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_retail_dag.py tests/test_k_selection_context.py` -> 13 passed in 7.71s. git diff --check passed. No service restart, new runs, data changes or commit. Existing empty runs unchanged; user should trigger a new run with an unused historical logical date after reparse.

## Training manual run end_date fix - 2026-10-15

- Inspected installed airflow.models.dagrun.DagRun.verify_integrity: non-backfill runs with logical_date after task.end_date are filtered from TI creation. Removed training DAG end_date (2011-11-01), retaining monthly cron and historical cutoff validation. Existing user/uncommitted K integration preserved. No DB edits, run creation, service restart or model training.
- Added regression asserting all five training tasks have no end_date and accept a 2026 logical date. `docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_retail_dag.py tests/test_k_selection_context.py` -> 13 passed in 7.40s; git diff --check passed. Docs updated for five tasks and unbounded monthly scheduling.
- User should allow DAG reparse then trigger NEW report-only run with historical training_cutoff/ETL manifest. Empty success runs are unchanged. Monthly schedule can now create current-period runs while unpaused; pause after demo. Daily/all-in-one historical end_date remains and can similarly filter explicit current logical dates; use historical logical dates there or address separately. No end-to-end Spark report success claimed.

## Current DAG architecture guide - 2026-10-15

- Added docs/KIEN_TRUC_DAG_VI.md at user request: five DAGs including the new K report, infrastructure/Connections/pool, task/Param/timeouts, historical dates, model lifecycle, Asset versus all-in-one, helper file map, manifest/XCom, lineage, operating sequence and limits. Existing runtime changes retained, no new runtime edits in this task.
- Verification: source review of DAGs, Compose, Dockerfile, requirements and support/runtime modules; Python AST counted 6/5/1/1/12 tasks matching the document; all local Markdown targets exist; git diff --check passed. Documentation only, no services or pipeline runs. Prior K runtime verification limitation remains explicit. SETUP still has a legacy reference to three training tasks; the new guide reflects five.

## Manual K report in training DAG - 2026-10-15

- Owner Codex, branch main. Added evaluate_k Spark task and report_only ShortCircuit gate; normal runs report then train with unchanged manually supplied rfm_k (default 2). Report-only skips train and publish. k_max defaults 8. Daily scoring unchanged.
- select_kmeans_k supports --pipeline-context, validates frozen ETL hash/cutoff, reads exact transaction output and uses context seed. Saves CSV/optional PNG and report.json provenance under analytics/k_selection/<run_key>. Existing standalone CLI retained. Human reviews report then creates a new run with same etl_manifest/cutoff and chosen rfm_k. docs/K_SELECTION_VI.md and DAG doc_md explain workflow.
- Verification: `python -m unittest discover -s tests -p test_k_selection_context.py -v` -> 2 passed (binding, changed hash/cutoff, wrong kind, CLI ambiguity); AST parse of four changed Python files and `git diff --check` passed. Updated test_retail_dag topology/gate assertions but could not run Airflow/PySpark: host lacks dependencies and Docker daemon unavailable even outside sandbox. No full-data run, service changes, commit or push.
- Risk/next action: report adds Spark runtime and blocks training on report failure; run `python -m pytest -q -p no:cacheprovider tests/test_retail_dag.py tests/test_k_selection_context.py tests/test_retail_training.py` in project Docker when available, then report-only fixture/demo. Existing unrelated handbook plan untouched.

## All-in-one branching and conditional consumers - 2026-10-15

- Owner Codex, branch main. User requested branching and conditional tasks in the all-in-one DAG. Added parameter-driven BranchPythonOperator for Marketing versus skip_marketing and ShortCircuitOperator for Dashboard; enable_marketing/enable_dashboard default true. Twelve tasks, no Asset changes, existing daily DAG untouched.
- finish_pipeline is the sole leaf with none_failed_min_one_success; completion is a direct upstream, so core publication remains mandatory even with both consumers disabled. ShortCircuit skips only its direct Dashboard child, not the final join. Conditions are configuration switches, not data-dependent audience/anomaly rules. Existing consumer demo behavior retained.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_allinone_dag.py tests/test_branch_publication.py tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py` -> 62 passed in 18.21s. Added actual ShortCircuit execute/skip-boundary tests afterwards; final `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_allinone_dag.py` -> 10 passed in 8.19s. Includes DAG serialization and all four switch combinations. `git diff --check` passed (CRLF warnings only). Local Ruff module unavailable; no lint success claimed.
- Updated docs/ALLINONE_DAG_VI.md topology, trigger rules and manual parameters. No full-data or scheduler DagRun executed, no external messages, commit or push. Optional next step: manually demonstrate switches with an eligible model and historical business_date.

## All-in-one daily alternative - 2026-10-13

- Owner Codex, branch main. Added ecommerce_etl_dag_allinone.py with eight tasks: original daily pipeline plus direct Marketing after RFM and Dashboard after notify. Cron/historical dates, pool, model requirement and optional Spark listener retained. No inlets/outlets, Asset scheduling or event emission in the new DAG; existing four DAGs remain available. Monthly model training remains preparation, not moved into daily scoring.
- Shared branch gate now populates outlet_events only for declared branch outlets. notify_completion accepts emit_assets=False and returns small metadata for XCom while retaining full publication checks. New allinone consumers reject other-run identity; Marketing validates RFM marker without Audit, Dashboard validates the immutable completion from XCom using extracted shared validate_snapshots. OpenLineage recognizes the new daily ID and both inline consumers; it does not schedule Assets.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_allinone_dag.py tests/test_branch_publication.py tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py` -> 58 passed in 15.17s. Includes no outlet_events in runtime fixture, early Marketing with missing Audit, blocked aggregate/Dashboard, frozen retry, different-run rejection, Spark failure and unchanged old Asset behavior. New DAG serialization included.
- docs/ALLINONE_DAG_VI.md records topology, demo limits and run instructions. Output generations use distinct DAG/run keys but date pointer remains shared: choose one daily variant for a demo to avoid duplicate work. No production/full-data run, training, service recreation, commit/push or external campaign dispatch.
- Ruff on six changed Python files and git diff --check passed. DagBag imported and serialized all five DAGs; an extra diagnostic accessing obsolete timetable.summary failed after successful assertions, corrected to print its class name.

## Optional Spark OpenLineage listener - 2026-10-13

- Owner Codex, branch main. Added LineageSparkSubmitOperator for ETL/training and as the base of both branch operators; retained publication gates and six-task daily topology. RetailExtractor registers the new operator. Listener attaches at execute time only, defaults off, honors global collection disable. Separate Java file/composite transport; parent/root injected by installed Spark provider. No DAG-parse downloads or Spark imports.
- Pinned Scala 2.13 OpenLineage JAR 1.52.0 with Maven Central SHA-512. Installer verifies before atomic rename; runtime rechecks checksum and PySpark 4.2.0. Downloaded JAR is ignored under data/lineage/jars. Both --jars and driver classpath are needed to avoid Spark 4 extension classloader warnings. UUID file per submission under logs/spark_openlineage; optional local Marquez HTTP timeout 3s, best-effort composite. Disabled spark_logicalPlan facet, retained column lineage.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_spark_lineage.py tests/test_branch_publication.py tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py` -> 61 passed in 14.63s. Ruff passed on nine changed Python files. All four production DAGs imported/serialized, remained paused; zero active/queued runs after fixtures.
- Real local[1]/768m fixtures: `tests/spark_lineage_smoke.py --marquez` verified CSV->Parquet, schema, revenue from qty/price column lineage, parent and SQL run COMPLETED on Marquez (run 01a1355a-1936-71b6-8a09-0b27c2d01d76); `--broken-http` verified computation and local capture with unavailable HTTP. `tests/spark_airflow_lineage_smoke.py` dag.test passed and matched Spark parent to actual Airflow task UUID 01a135a1-5fa1-770f-bdc7-b75774fe7655. Evidence paths in docs/SPARK_OPENLINEAGE_VI.md; logs ignored. Initial smoke exposed classloader warnings and verification mistakes (Marquez API route/job facet comparison), corrected and reverified.
- Existing actual .env unchanged, no production service recreation, full-data Spark, training, pool change or commit/push. Fixture Spark DAG added paused/schedule=None in its own bundle. Prior staged .env.example sanitation retained and flags appended unstaged; user Jinja comment retained with line wrap. Current main was already 5817dfb on origin before this work.
- Scope limits: physical Spark namespace file is distinct from Airflow logical file:// datasets; parent links jobs, physical_path supports manual dataset matching. No automatic alias merging, guaranteed transport replay, retention or full ML/UDF column coverage. Listener ready as opt-in; production overhead and complete retail run remain unmeasured. Activation/revert commands in docs/SPARK_OPENLINEAGE_VI.md.
- Final configuration verification: merged Compose config --quiet and git diff --check passed. An initial YAML indentation error on the two new flags was corrected before this successful check; no service was recreated from that draft.

## Independent analytics branches and aggregate Dashboard - 2026-10-12

- Owner Codex, branch main. Supersedes the earlier same-day RFM-only split. Both compute_rfm_metrics and detect_anomalies now use subclasses of a shared BranchSparkSubmitOperator: Spark -> branch-specific manifest/output gates -> atomic immutable publication marker -> own Asset event. Audit never reads RFM manifests to publish; RFM never reads Audit manifests. Six daily tasks and one-slot Spark pool retained.
- Notify validates both frozen branch publications before existing full completion publication, then emits RETAIL_SNAPSHOT_ASSET and retains the curated transaction outlet. It no longer emits either branch Asset. Dashboard schedule is only [RETAIL_SNAPSHOT_ASSET]; its consumer reads the exact completion manifest from the event and checks hash/identity/stages/markers/model/output readiness. Marketing remains RFM-only. Removed multi-Asset join/orphan/replay logic; no extra task, DAG, service or Parquet copy.
- RFM v2 metadata retained; audit v3 identifies only its own publication; aggregate v3 includes both hashes/markers and completion hash/path. Both branches reuse immutable advertised snapshots on retry. New data generations need new DagRuns. Existing Dashboard runs queued on old branch events fail closed; use a new run after upgrade, with no historical replay. Consumers remain metadata/log demos.
- Updated branch/aggregate OpenLineage including START/FAIL/fallback guards, Asset/design/lineage/setup/report docs, and regenerated docs/TOM_TAT_HE_THONG_DAG_VI.docx. The old 11/10 full-data evidence remains historical. Existing comprehensive handbook plan untouched.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_branch_publication.py tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py` -> 54 passed in 13.10s. Includes both completion orders with the other branch missing, branch failures/no event, retry reuse, corrupt markers/generations, aggregate storage failure, invalid snapshot rejection, date-pointer independence and provider lineage fallback. Direct DagBag import and DagSerialization.serialize_dag passed for all four DAGs; Dashboard schedule verified aggregate-only.
- Ruff 0.15.7 check passed on 12 touched runtime/test files; git diff --check passed (pre-existing CRLF warnings only). Word builder ZIP/XML/content checks passed; pagination remains unrendered. No production/Spark run, resource changes, external business messages, commit or push. Next optional step is an isolated scheduler demonstration; no latency claim made.

## Early RFM Asset publication - 2026-10-12

- Owner Codex, branch main. User requested Marketing to consume RFM without waiting for Audit. Daily retains six tasks/dependencies; RfmSparkSubmitOperator emits only RFM after Spark and an execute-time publication gate. notify_completion keeps all_success and only curated/anomaly outlets. Consumer schedules unchanged.
- New retail_support/publication.py validates context/ETL/model/output markers and atomically freezes rfm_publication.json. Event v2 has run/cutoff identity, context/ETL/RFM hashes and its own manifest; only final events have completion pointers. Retry after marker publication reuses the same snapshot without Spark. Changed generations after publication require a new DagRun; audit/notify can recover independently.
- Executive matches final events to early events by identity and hashes; ignores orphan early events from failed Audit runs, fails missing/mismatched final pairs, and preserves all-legacy v1 support without mixing versions. Consumers remain log-only demos. Late recovery of a run whose early event was already consumed may require replaying the RFM task (snapshot reuse) to satisfy AND; this is documented, not a durable join queue.
- Lineage registers the subclass, publishes RFM customers at its task COMPLETE, treats it as notify input, and guards START/FAIL/fallback against declared-outlet leakage. Updated DAG design, Asset/lineage docs, SETUP/REPORT and regenerated concise DOCX; old 11/10 full-data evidence remains explicitly historical.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py` -> 42 passed in 9.41s (final run). Covers no-audit readiness, Spark/gate/disk failures, frozen retry, final Audit recovery, orphan/mismatched events and real provider fallback. Direct DagBag import found all four DAGs without errors; DagSerialization.serialize_dag(daily) passed. Earlier inspection commands used obsolete DagBag/SerializedDAG APIs; corrected after reading installed 3.3 sources.
- Ruff 0.15.7 check passed on ten changed Python files; obtained through uvx cache after no local installation was found. `git diff --check` passed; DOCX builder ZIP/XML/content verification passed (four designed pages, rendering not checked).
- No full-data Spark run, scheduler event demo, external business messages, resource configuration changes, commit or push. One-slot Spark pool unchanged; no latency savings claimed. Next optional operational validation: isolated scheduler demo of early RFM and failing Audit, then a new full-data run if requested. Preserve unrelated pre-existing changes.

## Concise Vietnamese DAG Word briefing - 2026-10-12

- Owner Codex, branch main. Created docs/TOM_TAT_HE_THONG_DAG_VI.docx and reproducible standard-library builder docs/handbook/build_dag_summary.py. Scope follows the current request for a short briefing, separate from the older comprehensive handbook plan (left unchanged).
- Covers four production DAGs, six daily tasks, current 14-rule audit, RFM/model lifecycle, historical cutoff, retries/publication, Assets, lineage, measured results and limitations. Prioritizes current source and 2026-10-11 evidence over stale REPORT/design statements. Approximately 2,031 whitespace-separated words with four designed pages.
- Verification: `python docs/handbook/build_dag_summary.py` succeeded; ZIP integrity, all package XML, three explicit page breaks, Vietnamese replacement-character check and key factual anchors passed. `git diff --check` passed (existing CRLF warnings only). Word/LibreOffice renderer unavailable in checked installation paths; rendered pagination was not verified and can vary by fonts/client.
- No pipeline/test suite rerun, runtime/config/data changes, commit or push. Existing handbook preserved. Deliver the DOCX; no operational next action required for this documentation task.

## Marquez local backend and UI - 2026-10-11

- Owner Codex, branch main. User approved next step. Added optional docker-compose.lineage.yaml: Marquez API/UI 0.51.1 and separate Postgres 16 volume, loopback ports 5000/3000, search disabled, no automatic restart. RAM caps 768/256/256 MiB; measured total ~285 MiB after fixture. Base Compose still file-only; override enables composite local-file + HTTP, timeout 3s/no retry. Downloaded only two Marquez images; reused PostgreSQL and Airflow images.
- Applied override to existing three Airflow services with --no-build --no-deps; backend/UI left available at http://localhost:3000. All five DAGs paused; active/queued runs zero. No production data execution, Spark, training, historical-event replay, commit or push.
- Verification: merged `docker compose -f docker-compose.yaml -f docker-compose.lineage.yaml config --quiet` passed. `docker exec ecommerce-airflow-airflow-scheduler-1 bash -c 'python /opt/airflow/tests/lineage_smoke.py > /opt/airflow/logs/marquez-smoke.log 2>&1'` succeeded, task/DAG ~20s. `docker exec ecommerce-airflow-airflow-scheduler-1 python /opt/airflow/tests/verify_marquez.py` passed: run 01a12afe-c21f-7ad8-9783-8c46db89e86b COMPLETED, local START/COMPLETE match, fixture custom facet retained, exact input/output, three graph nodes. An initial verification ran before smoke completed and found the old file-only run (404); rerun after task completion passed. Evidence logs/marquez-verification.json. Host and container HTTP checks for API/UI returned 200; AST and git diff --check passed.
- Docs: docs/OPENLINEAGE_VI.md has opt-in start, file-only restore/stop commands, limits and fixture distinction. Existing smoke script's docstring now reflects configured network transport. New tests/verify_marquez.py is a manual integration assertion script, not a pytest test. No Spark listener/column lineage or guaranteed replay; custom schema URI remains local. Next optional work: actual retail run with collector or Spark listener, only when requested.

## OpenLineage Airflow-only collection - 2026-10-11

- Owner Codex, branch main. User approved step one with resource limits. Pinned already-installed provider 2.20.0/client 1.52.0; enabled local FileTransport and custom RetailExtractor in Compose, source code/full task info disabled, Spark injection disabled. Recreated only the existing three Airflow services with --no-build; no downloads, Marquez, backend or Spark listener.
- Extractor reads small run-specific context/stage/completion/model manifests. Stable logical staged/published dataset names carry physical generation facets; RFM consumes its frozen model; activation is checked before publishing the model dataset. Only successful publication exposes published outputs. Explicit failure and safe fallback inputs prevent the provider from reintroducing declared Asset outlets on START/FAIL. Provider transport errors are nonfatal and tested; delivery is best effort, no outbox/replay.
- Verification: Docker `python -m pytest -q -p no:cacheprovider tests/test_retail_lineage.py tests/test_retail_dag.py tests/test_consumer_events.py` -> 21 passed in 7.53s. `python /opt/airflow/tests/lineage_smoke.py` -> exit 0, isolated dag.test task success (~15s), actual file events checked for START/COMPLETE shared runId, fixture I/O and custom facet. Read-only extraction of 8 completed production task manifests also passed; no historical events emitted or dataset jobs rerun. Initial smoke failed before task execution because Airflow 3.3 requires bundle serialization; the fixture now uses its own logs/lineage_fixture bundle.
- Effective collector disabled=False; four production DAGs plus smoke fixture paused; active/queued runs zero. Events at logs/openlineage-*.json, smoke log at logs/openlineage-smoke.log. The fixture is not in the production dags folder. Documentation: docs/OPENLINEAGE_VI.md. No commit/push or external metadata transmission.
- Limits: production runs remain paused and were not rerun with collector; no Spark/column-level extraction, backend UI, retention policy or custom consumer-to-producer parent-run facet. Consumer dataset identities match published outputs; exact producer event identity remains in Airflow Asset events. Custom facet schema uses a local file URI for this local stage. Next optional stage: backend/UI after user approval.

## Full-data scheduler execution verified - 2026-10-11

- User deferred lineage/OpenLineage and requested executing the current stack first, with minimal resource use. Owner Codex, branch main. Bootstrap ETL cutoff 2011-12-01 succeeded, then training_full_stack_20261011, daily_full_stack_20261011 and both automatically asset-triggered consumers succeeded: 4 DAGs, 11 tasks, try_number=1 throughout.
- Model 0141654d68d044d49d6b1d1fddfef689 is activated and explicitly pinned by daily. Daily read 541909 source rows, produced 391057 RFM transactions / 4334 customers; audit assessed 536641 rows / 25900 orders, flagged 46341 rows / 54 orders. All outputs remain versioned; original source SHA256 is unchanged. Previous date pointers were backed up under logs/full_stack_20261011/before.
- Verification: container `python /opt/airflow/logs/full_stack_20261011/verify.py` -> exit 0, verified=true. Read-only Arrow row counts, output markers, model artifacts/version, source/context/ETL hashes and 3 producer Asset events verified. Marketing consumed event 2; Executive consumed events 2 and 3; both were asset_triggered, not manual. Evidence and timing: docs/RUN_EVIDENCE_20261011.md, logs/full_stack_20261011/verification.json and task logs. No repeat test suite or extra Spark verification job.
- Pool remains one slot. Audit logged Java codegen 64 KB fallback and disk cache spill but succeeded without retry. Consumer actions remain logging demos; DAG remains full-snapshot mode. No OpenLineage installation or external messages. No commit/push. All four DAGs confirmed paused after cleanup; active/queued DagRun count is zero. Next work is OpenLineage only when resumed by user.

## Pull and conflict resolution completed - 2026-10-11

- Owner: Codex; branch: `main`. Completed the previously paused rebase, fetched origin/main `709eb68`, and rebased the local training commit as `5da83da`. `git pull --rebase origin main` fetched successfully but refused duplicated main entries in FETCH_HEAD; explicit `git rebase origin/main` completed the integration. Main is ahead by one commit and behind by zero; no push.
- Resolved notes by retaining both sides. Preserved every upstream RFM function body, including incremental store and load-only daily CLI. Added fit/apply adapters for the local registry DAG; fit delegates to upstream train_rfm_kmeans.train_model, whose source now participates in the code fingerprint. Kept --pipeline-context dispatch and documented that the DAG still uses full ETL snapshots, not the incremental CLI store.
- Restored local Asset DAG edits after integration. Nine local DAG/support/doc/test files matched the pre-pull backup after newline normalization; subsequently corrected only the XCom mock return value in the new test. Asset changes remain uncommitted. Backup: `C:/Users/ADMIN/AppData/Local/Temp/retail-before-pull-20261011-njumzqmf`; stash `codex-preserve-asset-dags-before-pull-20261011` retained.
- Verification in ecommerce-airflow-airflow-scheduler-1: `python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py tests/test_retail_training.py tests/test_retail_contracts.py tests/test_retail_models.py tests/test_retail_dag.py tests/test_retail_pipeline.py` -> 133 passed in 340.05s, 10 upstream PySpark/pandas/toJSON warnings. After restoring Asset code, `python -m pytest -q -p no:cacheprovider tests/test_retail_dag.py tests/test_consumer_events.py` -> 14 passed in 8.03s. Direct Airflow DagBag parse found all four DAGs with no errors; `airflow dags list-import-errors` returned No data found. AST/unique-definition checks passed for 32 files, substantive note lines from both revisions retained, git diff --check passed, origin/main ancestry passed, no unmerged paths or rebase state remain.
- Docker Desktop was started with approval and left running. No production data job, external message, push, or consumer-trigger demo was executed. Next action: review the uncommitted Asset feature; perform a separate scheduler demo if requested. The initial database-backed DAG list had not yet refreshed the new consumers; direct parsing verified their definitions.

## Two-tier anomaly rule base: final review fixes - 2026-10-09

- Owner: Claude. Branch `feature/anomaly-rule-base` was merged fast-forward into `main` and deleted at the user's request on 2026-10-09; the merged `main` passed the Docker suite (98 passed) and the host structure test, then was pushed to origin/main.
- The fresh whole-branch review returned "with fixes" (0 Critical, 4 Important, 4 Minor; one Minor raised to Important). All five were fixed, each with a test that failed first:
  - Not-applied checks no longer carry evidence.
  - The customer-level quantity baseline needs a stock median from >= 30 identified lines.
  - Quantity = INT_MIN no longer overflows.
  - Robust-z gate coverage was added; a mutation check confirms the test catches a disabled gate.
  - Every number in doc sections 4 and 9 now comes from tables E8b-E9b in `anomaly_evidence.py` and R5b/R5c in `anomaly_summary.py`. E5b mirrors the new reliable-stock rule.
- Verification:
  - Docker suite `python -m pytest -q -p no:cacheprovider tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py tests/test_anomaly_evidence.py` -> 98 passed.
  - Full-data anomaly re-run: exit 0 in 160 s; 536,641 rows; 46,341 rows flagged (166 high, 2,178 medium, 43,997 low); 54 invoices flagged.
  - E5b pandas percentages match the Spark baseline levels exactly (customer 79.06%, customer_stock 14.00%, insufficient 35,054 rows).
- Deferred minors:
  - `assess_orders` channel uses `F.trim`, not `_normalized`.
  - The doc writes |ΣQ| but the code compares Σ|Q|.
  - No test covers `outside_scope` orders.
- Open decisions for the user: the doc 9.3 recalibration options. The DAG `detect_anomalies` stage has still not been re-run end-to-end through Airflow.

## Two-tier anomaly rule base - 2026-10-08

- Owner: Claude; branch: `feature/anomaly-rule-base` (from `main` 57e6afe). Plan `.agent/plans/archive/2026-10-08-anomaly-rule-base.md`. Not pushed or merged.
- `scripts/pyspark_anomalies.py` now classifies rows (`record_type`) and invoices (`channel`: identified, retail_web via DOT, retail_other, internal), then runs 14 checks: 7 tier-1 data rules (ISO/IEC 25012 / DAMA) and 7 tier-2 business rules (contextual quantity/price deviation with customer -> channel robust baselines, zero-price lines, stock counts, accounting entries, unmatched returns). Each flag has value_at_risk (GBP) and severity. `assess_orders` moved here from `retail_pipeline.py` with customer -> channel robust baselines (replaces global mean + 3 sigma). CLI `--iqr-multiplier` replaced by `--z-threshold` (3.5).
- Full documentation, evidence (E1-E9), rules and results: `docs/ANOMALY_DETECTION_VI.md`. Evidence script `scripts/anomaly_evidence.py` (pandas), result summary `scripts/anomaly_summary.py` (Spark).
- Verification: Docker `python -m pytest -q -p no:cacheprovider tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py tests/test_anomaly_evidence.py` -> 94 passed; host structure test passed (called directly, host has no pytest). Full-data clean + anomaly spark-submit exit 0 (252 s); 536,641 rows preserved; record_type/channel counts match independent pandas evidence exactly; 46,933 rows and 54 invoices flagged.
- Risks: contextual rules over-flag low-value normal behaviour (83-99.7% of their flags have MAD = 0; price flags mostly quantity-tier pricing) — recalibration options in doc section 9.3 await user approval. DAG `detect_anomalies` stage not re-run end-to-end through Airflow (stack not initialized this session). Handbook and `docs/DAG_DESIGN_VI.md` still describe the old nine checks / mean + 3 sigma. Docker image was built with `--add-host` pinning PyPI to a fast CDN edge because two Fastly edges served ~40 KB/s; normal `docker compose build` works when the edges are healthy.
- Next action: user decides on section 9.3 recalibration, then review/merge the branch.
## Asset-driven consumer DAGs - 2026-10-11

- Owner: Codex; existing detached HEAD/rebase preserved. Added centralized Assets, outlets only on successful daily publication, and PythonOperator outlet_events metadata (cutoff, producer identity, date pointer and per-run completion manifest). Daily six-task topology and training behavior remain intact.
- Added marketing and executive consumer DAGs as explicit logging demos, matching the supplied examples; no external messages/vouchers or dashboard writes. Consumers handle all coalesced events, deduplicate repeated publications, and reject missing/mismatched event metadata. Executive checks matching publication sets beyond the AND schedule. Setup and limitations: docs/ASSET_SCHEDULING_VI.md.
- Verification: `python -m unittest discover -s tests -p test_consumer_events.py -v` -> 5 passed; Python AST check -> 10 DAG/support/test files passed; `git diff --check` passed. Added Airflow regression tests for schedule declarations, emitter placement, and success/failure metadata ordering; these were not executed because local Python lacks Airflow/pytest and Docker daemon is stopped (confirmed outside sandbox).
- Next validation when Docker is running: `python -m pytest -q tests/test_retail_dag.py tests/test_consumer_events.py` in Airflow container, then `airflow dags list-import-errors`; unpause consumers before a new producer demo run. Actual scheduler event delivery is not yet verified. No commit/push, rebase continuation, dataset changes or unrelated staged edits.

## Rebase conflict resolution - 2026-10-08

- Owner: Codex; branch: `main` (rebase). Resolved HANDOFF/TODO by retaining entries from both sides; preserved `fit_rfm_model` plus the existing apply and legacy wrapper in scripts/pyspark_rfm.py.
- Verification: `docker exec ecommerce-airflow-airflow-scheduler-1 python -m pytest -q -p no:cacheprovider tests/test_pyspark_rfm.py tests/test_retail_training.py` -> 59 passed in 168.78s, one upstream pandas/PySpark compatibility warning. Python AST parsing passed for all 23 source/DAG/test files; unique fit/apply/legacy functions and both sides' note entries verified. `git diff --check` passed.
- Next action: stage the three resolved files, then continue the pending rebase and push main. This resolution task does not run rebase --continue or push. Full-data scheduler execution was not rerun.

## Two-DAG code implemented and fixture-verified - 2026-10-06

- User explicitly requested coding the revised design. Kept the six daily tasks and ETL/audit algorithms. Split fit/apply in pyspark_rfm, added retail_models/retail_training and a three-task retail_rfm_training_dag; daily pins model and only transforms. Legacy standalone fit-and-score remains supported.
- Full PipelineModel artifacts and atomic registry retain scaler/centroid identity; context pins model or ETL generation across retries. Temporal/source/feature/hash gates fail closed. Bootstrap ETL-only supplies the first training snapshot without daily success. ETL publishes a small source/cutoff index. Both DAGs use the existing one-slot pool.
- Docker Desktop was initially stopped; started it with approval, existing Compose services became healthy. Installed pinned Ruff 0.15.7 only under container /tmp/retail-lint-tools. No production model/data generations were created or deleted; Spark lifecycle tests used pytest temporary namespaces. Both DAGs remain paused.
- Verification: Docker `python -m pytest -q tests/test_retail_contracts.py tests/test_retail_models.py tests/test_retail_dag.py` -> 19 passed in 9.03s. Docker `python -m pytest -q tests/test_retail_training.py tests/test_retail_pipeline.py tests/test_pyspark_rfm.py tests/test_pyspark_clean.py tests/test_pyspark_anomalies.py` -> 75 passed, one upstream pandas/PySpark warning, 245.82s. Lifecycle covers real Spark bootstrap/train/save/load/activation/daily/audit/publication/read-back, blocked fit during daily and one-customer scoring with k=2.
- Ruff check dags/scripts/tests passed; formatter check for seven changed runtime/orchestration files passed. `airflow dags list-import-errors` returned No data found; list recognized both DAGs. Local AST/link checks and git diff --check passed. No scheduler full-data two-DAG run, JVM crash injection or runtime pool concurrency experiment was performed.
- SETUP documents required first-model bootstrap and training before daily, historical eligibility, retry, missing model and stale lock recovery. IMPLEMENTATION/REPORT/design distinguish new fixture evidence from the old 29/09 full-data scheduler run. Explicit per-run model_version supports rollback; no global reactivation command, campaign cohort freeze or rolling window added.
- Next operational action: follow SETUP to bootstrap data through 2011-11-30, train cutoff 2011-12-01, then run daily at a later eligible cutoff. Old run contexts fail current code-version checks by design; preserve outputs and start new runs. No commits/pushes. Handbook work remains separate.

## Two-DAG design revision - 2026-10-06

- User requested minimal changes to the DAG design following discussion of daily inference and periodic training. Updated `docs/DAG_DESIGN_VI.md` and REPORT status only; runtime code, data, environment and existing evidence were preserved.
- Daily retains six task IDs/topology; new training design has metadata validation, Spark training and atomic model publication. Training reads committed ETL snapshots, reuses RFM aggregation and bootstraps from ETL-only execution without requiring daily completion. Full PipelineModel, version pinning, temporal eligibility, retry/backfill, model failure and shared pool contracts are specified.
- Kept cumulative history and daily percent_rank rules, with explicit limits on Marketing stability. Removed daily occupied-k requirement and separated training gates; added planned acceptance tests T29–T35 and a minimal migration sequence.
- Verification: `git diff --check` passed (line-ending notices only); documentation structure/relative Markdown links and changed-file scope checked. No runtime tests needed for documentation-only edits; no claim that the second DAG or inference mode is implemented.
- Next action: implement section 16.1 if requested, then run focused regression and two-DAG bootstrap/scoring smoke tests. Existing handbook work remains separately in progress.

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

## RFM supervised publication - 2026-10-19

Owner Codex, branch codex/rfm-supervised-20261019, based on f671393. User authorized push. Standalone RFM classifier source/test/docs included; divergent local shared workflow preserved as docs/patches/rfm_local_workflow.patch rather than replacing upstream model registry. Syntax and six actual extracted CLI cases pass; Spark unavailable today. Historical test evidence retained with dates. No data/models/notebooks or unrelated source included. Next: adapt classifier into upstream model registry and run Docker verification before main integration.

2026-10-19 correction: prior publication withdrawn with lease; model predicts numeric Cluster directly. All naming/holding/prior-label policy removed. Owner Codex, branch codex/rfm-supervised-20261019. 3 contract tests and syntax checks pass; Spark unavailable. No new commit/push; runtime review pending.

2026-10-19: User authorized republishing corrected direct-cluster RFM. Owner Codex; branch codex/rfm-supervised-20261019. Prepared exact source/test/docs scope; no VIP/prior-label policy in executable changes. 3 contract tests and 5 syntax checks pass. Shared workflow retained as unapplied patch; Docker/Spark validation remains pending.

## RFM peer review correction - 2026-10-21

Owner Codex, branch codex/rfm-supervised-20261019. Implemented temporal grid selection on new-customer validation macro-F1, all/seen/new cohorts, observed-union macro-F1, validation-only gate and single pre-refit test evaluation. Refit student at latest cutoff with fixed teacher; schema v3 records evaluated/deployed IDs. Strict model/metadata contracts and consistent partition schema. Integrated classifier with existing training context, registry hashes/activation and daily dispatch; monthly DAG defaults classifier. K report uses teacher cutoff. Removed obsolete workflow patch.

Verification: 35 distinct tests passed across focused runs, four platform-specific disk/lifecycle tests skipped; actual Spark grid/refit and daily adapter dispatch covered. 27 Python files parsed. Full-data quality, persistence and Airflow scheduler/DagBag remain unverified here; see docs/RFM_SUPERVISED_VI.md. No commit/push. Original initial-setup checkout has divergent shared workflow and was not overwritten. Next: teammate review and project Docker acceptance suite before activation.

## RFM file consolidation - 2026-10-21

Owner Codex, branch codex/rfm-supervised-20261019. Moved pure classifier helpers into existing retail_contracts.py, registry tests into test_supervised_contracts.py, and verification/review notes into docs/RFM_SUPERVISED_VI.md. Removed four redundant files and updated imports/fingerprinting/references. No training or daily behavior change intended. Verification: pytest -q tests/test_supervised_contracts.py tests/test_retail_models.py tests/test_retail_contracts.py -> 20 passed in 5.28s. Docker/Airflow checks remain pending. No commit/push.

## RFM review publication - 2026-10-21

User authorized commit/push of consolidated RFM changes to codex/rfm-supervised-20261019. Remote branch checked at 518a976 with no divergence before publication. Includes classifier tuning/refit, registry and DAG integration, focused tests and consolidated guide. Verification retained: 35 distinct passes and four platform skips before consolidation; 20 focused passes after consolidation (overlapping, not additive); git diff --check passed. No model activation or main merge. Next: teammate review, Docker persistence/lifecycle and Airflow acceptance, then full-data evaluation. Earlier no-push notes describe earlier checkpoints.

## RFM follow-up review - 2026-10-21

Owner Codex; branch codex/rfm-supervised-20261019. Corrected K report training_cutoff vs teacher_cutoff, centralized RF candidate ranking, added K cutoff and registry metadata mismatch tests. Consolidated guide distinguishes teammate Docker/full-data evidence from local Windows checks, small validation/tied scores, and remaining standalone CLI/UI verification. Documented targeted Airflow recreation from deployed checkout; do not switch relative data mounts to review worktree. Corrected TODO table and timestamp. Follow-up verification: pytest tests/test_k_selection_context.py tests/test_supervised_contracts.py tests/test_supervised_rfm.py -k "not real_temporal_grid" -> 19 passed, 3 Windows filesystem skips, 1 deselected (81.74s). Includes new cutoff/metadata mismatch regressions and model-selection orchestration. Docker executable unavailable; no container recreation, project PySpark 4.2.0 rerun or real-data standalone CLI run claimed. No new commit/push.

RFM cleanup 2026-10-21: owner Codex, branch codex/rfm-supervised-20261019. User authorized GitHub cleanup only: untrack 24 generated outputs (byte-for-byte local preservation verified), ignore their directories, remove obsolete RFM_DAG_HANDOFF_VI.md, update notebook prerequisites. Code, input fixtures and notebooks retained. Separate cleanup commit; pending peer-review code changes remain unstaged.

2026-10-21: Codex simplified RFM reporting at user request: removed optional agreement metric; preserved macro-F1 selection and per-class diagnostics. Synchronized review checkout and Documents initial-setup; backup C:\Users\Lenovo\.codex\.chatgpt-projects\g-p-6a9cd2b108788191b22e1d7768f56837\rfm_metrics_backup_20261021_224259. Verification: test_supervised_contracts.py in Documents checkout: 7 passed in 1.78s. No commit/push.


## Standalone execution verified - 2026-10-22

Executed the real store-based standalone path using the local source dataset and
project Docker image, Spark 4.2.0. Separate Linux volume
`rfm-standalone-linux-20261021`; project/dataset mounted read-only.
Preparation used one customer bucket for this isolated demonstration.
All six stages succeeded: prepare snapshots, teacher CLI, classifier CLI,
daily CLI, identical daily retry, and output/bundle verification.

- Train/validation/test/refit cutoffs: 2011-08-03 / 2011-09-02 / 2011-10-02 / 2011-11-01.
- Snapshot customers: 3165 / 3321 / 3613 / 3970.
- Selected RF: 150 trees, maxDepth 8, from the default six configurations.
- Validation new customers: 156; macro-F1 0.9595400708913288.
- Test new customers: 448; macro-F1 0.9491816561335278 (pre-refit model only).
- Daily 2011-11-02: 3981 unique customers; cluster 0=2421, cluster 1=1560.
- Saved/loaded bundle successfully; evaluated/deployed model IDs differ.
- Daily retry succeeded through the existing exact-content comparison.
- Sum of six stage execution times: 274.40 seconds.

This is independent standalone evidence, not a reproduction of the teammate's
registry run. Selected parameters/test score differ; no claim of identical
partitioning, teacher or trained model between the two runs.
No serving-model activation or Airflow DAG execution was performed.
Docker Desktop was started; existing services auto-started. Test container is
stopped, and its isolated volume is retained. Windows native Hadoop write failed;
Linux volume run passed. Logs/artifacts are retained in the test volume;
result.json, snapshots.json and execution.json are exported to
`C:\Users\Lenovo\.codex\.chatgpt-projects\g-p-6a9cd2b108788191b22e1d7768f56837\rfm_standalone_docker_20261021`.
No commit/push.

## Main publication authorized - 2026-10-22

Owner Codex. User explicitly requested direct main publication of RFM and required DAG dependencies. origin/main=f671393 is ancestor of review branch; normal fast-forward push only. Final corrections: separate K report cutoffs, shared RF ranking, metadata mismatch regression, remove optional agreement reporting. Rewrote RFM_SUPERVISED_VI.md as operator handoff with bootstrap/training/daily Params and prerequisites; linked SETUP. Docker scoped integration suite: 47 passed, 0 skipped, 1 pandas compatibility warning in 290.20s. Standalone real-data six-stage run already passed. Original Documents anomaly/notebook changes excluded. Live scheduler verification remains operator task; no model activation performed.
