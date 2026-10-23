# Project tasks

ETL reuse: DONE — Owner Codex; branch main; 38 focused Docker tests passed.

| Status | Task | Owner | Branch | Dependency |
|---|---|---|---|---|
| READY TO PUSH | Publish scoped RFM corrections and DAG operator guide to main | Codex | codex/rfm-supervised-20261019 -> main | User authorized 2026-10-22; 47 Docker tests passed; live scheduler pending |
| IMPLEMENTED; DOCKER VERIFICATION PENDING | Address RFM peer review: temporal tuning/cohorts/refit, model validation, registry/DAG integration and real tests | Codex | codex/rfm-supervised-20261019 | Baseline published 45b4893; teammate reports Docker 25 passed; local environment limits remain |
| DONE | Isolated UI sensor pool deadlock demonstration | Codex | main | 2 tests passed; UI pool setup documented; live demo not triggered |
| DONE | Minimal UI idempotency demo with published branch reuse log | Codex | main | 20 publication tests passed; docs/DEMO_IDEMPOTENCY_VI.md; no real UI run |
| DONE | Add opt-in one-shot audit pre-publication failure for UI atomicity demo | Codex | main | 45 Docker tests passed; docs/DEMO_ATOMICITY_VI.md; no full-data demo run |
| DONE | Include full final UTC day for historical manual runs across three cron DAGs | Codex | main | Boundary/serialization tests included in 45 passing tests; current 2026 logical dates still excluded |
| DONE | Restore historical training end_date and document explicit historical manual logical dates | Codex | main | Supersedes unbounded scheduling fix; 13 focused Docker tests passed; historical dates documented |
| DONE | Remove training end_date blocking current manual logical dates | Codex | main | Installed date filter verified; 13 focused Docker tests passed; schedule now unbounded, pause after demo |
| DONE | Document current five-DAG architecture, configuration and supporting files in Vietnamese | Codex | main | docs/KIEN_TRUC_DAG_VI.md; source review, AST task counts, local links and diff checks passed |
| DONE | Add manual K report to monthly training DAG using frozen ETL context | Codex | main | 2 context tests and syntax/diff checks passed; Docker unavailable, Airflow/Spark runtime checks pending |
| DONE | Add explicit branching and conditional consumers to all-in-one DAG | Codex | main | 62 regression tests passed; final 10 all-in-one tests passed including ShortCircuit skip boundary; docs updated; no full-data run |
| DONE | Add ecommerce_etl_dag_allinone using direct task dependencies without Asset scheduling/events | Codex | main | 58 focused tests passed; eight tasks; same-run manifests/XCom; existing DAGs retained; no full-data run |
| DONE | Add opt-in Spark OpenLineage listener with pinned JAR, Airflow parent and isolated Spark verification | Codex | main | 61 tests passed; real Spark schema/column/Marquez, broken HTTP and Airflow parent verified; default off; no full retail run |
| DONE | Independent RFM/audit publications and aggregate-only Dashboard Asset | Codex | main | 54 focused tests; four DAGs import/serialize; Ruff passed; docs/Word updated; no production run |
| DONE | Publish validated RFM Asset from compute task; decouple Marketing and retain matched Executive snapshots | Codex | main | 42 focused tests passed; four DAGs import/serialize; Ruff passed; docs and Word updated; no full-data rerun |
| DONE | Create concise Vietnamese Word summary of current DAG system | Codex | main | docs/TOM_TAT_HE_THONG_DAG_VI.docx; 4 designed pages; ZIP/XML/content checks passed; no runtime changes |
| DONE | Add optional local Marquez backend/UI and verify lightweight Airflow HTTP delivery | Codex | main | Smoke success; API run/facet/3-node graph verified; UI/API HTTP 200; no Spark rerun |
| DONE | Add Airflow OpenLineage collection from manifests, local file transport and focused fixture checks | Codex | main | 21 tests passed; real provider fixture START/COMPLETE verified; no Spark/full-data rerun; docs/OPENLINEAGE_VI.md |
| DEFERRED | Document data lineage and observability, then consider OpenLineage | Codex | main | User requested full current DAG execution first on 2026-10-11 |
| DONE | Run full-data bootstrap, training DAG, daily DAG and both asset consumers | Codex | main | 4 DAGs / 11 tasks success first try; Parquet/hash/event verification passed; docs/RUN_EVIDENCE_20261011.md |
| DONE | Integrate origin/main 709eb68 with local training DAG; preserve upstream incremental CLI and local asset work | Codex | main | 133 regression tests + 14 Asset DAG tests passed; four DAGs parsed; no unresolved conflicts |
| DONE | Final-review fix pass for anomaly rule base | Claude | `feature/anomaly-rule-base` | 98 Docker tests passed; full-data re-run exit 0 |
| DONE | Run DAG `detect_anomalies` end-to-end through Airflow after merge | Codex | main | daily_full_stack_20261011 success; 536641 rows, 54 flagged orders; 14-rule audit |
| DONE | Two-tier anomaly rule base by record type, channel and per-customer baseline; move assess_orders into pyspark_anomalies.py; document in docs/ANOMALY_DETECTION_VI.md | Claude | `feature/anomaly-rule-base` | 94 Docker tests passed; full-data run exit 0; `.agent/plans/archive/2026-10-08-anomaly-rule-base.md` |
| TODO | Recalibrate contextual anomaly rules (quantity-tier pricing, MAD floor, minimum value_at_risk) per docs/ANOMALY_DETECTION_VI.md section 9.3 | Unassigned | - | User approval of new constants |
| TODO | Update handbook and DAG design docs to the two-tier anomaly rule base | Unassigned | - | Merge of `feature/anomaly-rule-base` |
| DONE | Add publication assets and two event-driven demo consumers | Codex | main | Docker: 14 DAG/event tests passed and all four DAGs parse; scheduler-trigger demo remains unrun |
| DONE | Resolve rebase conflicts preserving upstream notes and local training/scoring | Codex | `main` (rebase) | 59 focused Docker tests passed; AST and note preservation checks passed; ready to continue rebase |
| DONE | Implement two-DAG training/scoring, model contracts, bootstrap and focused regression | Codex | `main` | 94 tests passed; lint/import checks passed; real-data bootstrap and scheduler demo documented in SETUP |
| DONE | Revise DAG design for daily scoring and separate versioned model training; minimal runtime migration | Codex | `main` | User request 2026-10-06; implementation tracked in the row above |
| DONE | Match Online Retail guide image usage to anomalies/RFM guides and delete notebooks/assets at user request | Codex | `main` | No embeds or orphan captions; 12 sections/links/syntax verified; notebook hashes unchanged |
| DONE | Convert Online Retail EDA DOCX to Markdown guide, fill missing explanations and remove source DOCX after verification | Codex | `main` | 12 sections, 9 images and CSV-backed checks passed; DOCX deleted at user request |
| DONE | Publish complete committed local snapshot to main at user request | Codex | `initial-setup` -> `main` | Explicit destination correction 2026-10-03 |
| DONE | Publish all modified/untracked nonignored local files including Parquet/fixtures at user request | Codex | `initial-setup` | Explicit user authorization 2026-10-03 |
| DONE | Organize RFM EDA/comparison notebooks and write Vietnamese guide | Codex | `initial-setup` | Existing notebooks and local Parquet |
| IN PROGRESS | Create comprehensive Vietnamese Word architecture and end-to-end project handbook, traced to assignment and updated rubric | Codex | `main` | airflow_subject.docx, book-backed design, actual code/evidence |
| DONE | Implement and verify official six-task DAG, landing/manifests, Spark contracts, sensor and deadline declaration | Codex | `main` | 87 tests and full scheduler run success; limits in docs/IMPLEMENTATION_VI.md |
| DONE | Design assignment DAG with book/page references, requirement traceability and acceptance matrix (documentation only) | Codex | `main` | `docs/DAG_DESIGN_VI.md`; scope aligned to updated rubric |
| DONE | Align DAG design with user-updated 25/25/25/15/10 rubric, Sensors and Airflow 3 deadline monitoring | Codex | `main` | Updated rubric supplied by user on 2026-09-28; T01–T28 planned |
| DONE | Implement PySpark anomaly detector per accepted scope | Codex | `main` | `.agent/plans/archive/2026-09-23-pyspark-anomalies.md` |
| DONE | Write Vietnamese A-G anomalies EDA interpretation guide | Codex | `main` | Current executed notebook outputs |
| DONE | Validate description keyword flags against product names by StockCode | Codex | `main` | User correction; anomalies EDA |
| DONE | Implement and execute standalone anomalies EDA notebook | Codex | `main` | Approved EDA-only plan; local CSV and Parquet |
| DONE | Create collaborative project scaffold | Codex | `initial-setup` | None |
| DONE | Pull latest GitHub main on 2026-09-22; verified HEAD equals origin/main at b260d6b | Codex | `main` | None |
| DONE | Build and smoke-test Docker environment | Codex | `main` | Official scheduler run on 2026-09-29 |
| DONE | Select the Kaggle dataset and document its Docker-first download | Team | `initial-setup` | None |
| DONE | Explain non-cancelled negative-quantity rows in EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Explain the remaining zero-unit-price rows below EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Inspect the seven customer-linked all-zero-price rows in EDA | Codex | `main` | `data.csv` |
| DONE | Implement raw-data validation | Codex | `main` | Landing sensor and SHA-256 contracts |
| DONE | Implement PySpark cleaning job | Codex | `main` | Dataset schema |
| DONE | Tighten RFM cleaning filters and regenerate Parquet | Codex | `main` | Existing cleaning job |
| DONE | Exclude service and fee lines from RFM Parquet and refresh related checks/docs | Codex | `main` | Existing cleaning job and raw dataset |
| DONE | Standardize retail column types across EDA, Spark, and Parquet outputs | Codex | `main` | Raw dataset schema |
| DONE | Implement RFM job | Codex | `main` | 4,334 customer outputs in official scheduler run |
| DONE | Implement anomaly-detection job from 2026-09-23 plan | Codex | `main` | `.agent/plans/archive/2026-09-23-pyspark-anomalies.md` |
| DONE | Replace DAG placeholders with working operators | Codex | `main` | All six tasks success in official_smoke_20260929 |
| DONE | Add Spark logic tests | Codex | `main` | Spark jobs |
| TODO | Complete report, setup evidence, and presentation | Unassigned | - | Working pipeline |
| DONE | Diagnose comparison notebook PROJECT_ROOT NameError and verify initialization snippet | Codex | `initial-setup` | User traceback; notebook on disk is empty |
| DONE | Publish four KMeans source/test/report files as 0185d1e | Codex | `main` (isolated worktree) | User authorization |
| DONE | Diagnose supplied RFM test log and restore missing LongType import | Codex | `initial-setup` | User Docker test log |
| DONE | Provide Docker commands for cleaning, k selection and RFM with k | Codex | `initial-setup` | Current CLI and Compose |
| DONE | Fix reviewed KMeans guards, validation, docs and UTC configuration | Codex | `initial-setup` | Review findings |
| DONE | Review revised KMeans source/tests and cluster EDA naming | Codex | `initial-setup` | Saved notebook and local results |
| DONE | Review K-means RFM logic, assumptions, validation, and tests | Codex | `initial-setup` | Current local RFM source |
| DONE | Publish RFM segmentation and updated tests to main as 5768d56 | Codex | `main` (isolated worktree) | GitHub access |
| DONE | Push RFM partition filtering and its tests to main as cae0638 | Codex | `main` (isolated worktree) | GitHub access |
| DONE | Explain test, clean, then RFM Docker commands | Codex | `initial-setup` | Existing Docker configuration |
| DONE | Apply only RFM commit to main and push as b260d6b | Codex | `main` (isolated worktree) | GitHub access |
| DONE | Commit and push RFM source/tests as 8254828; excluded EDA RFM notebook | Codex | `initial-setup` | GitHub access |
| DONE | Recheck refreshed RFM notebook outputs against current Parquet | Codex | `initial-setup` | Saved notebook |
| DONE | Review latest RFM notebook, code, and full output | Codex | `initial-setup` | Local Parquet |
| DONE | Pull origin/main 1f07bb1 preserving local code; SHA256 verified | Codex | `initial-setup` | Remote main |
| DONE | Assess POSTAGE and Manual impact on local curated RFM metrics | Codex | `initial-setup` | Local curated Parquet |
| DONE | Independently verify full RFM output against curated Parquet | Codex | `initial-setup` | Local Parquet outputs |
| DONE | Pull main schema update be75e6d and preserve local files | Codex | `initial-setup` | Remote main |
| DONE | Pull main updates on 2026-09-21 and preserve local files | Codex | `initial-setup` | Remote main |
| DONE | Create notebook to read curated RFM Parquet for EDA | Codex | `initial-setup` | Local RFM.parquet dataset |
| DONE | Integrate origin/main while preserving local work | Codex | `initial-setup` | Remote main and local backup |
| DONE | Restore initial-setup working changes after cancelled branch switch | Codex | `initial-setup` | Saved stash |
| DONE | Pull latest GitHub changes while preserving local RFM files | Codex | `initial-setup` | Remote repository |
| DONE | Add missing-column and Parquet overwrite tests for RFM | Codex | `initial-setup` | Existing RFM tests |
| DONE | Consolidate RFM tests into one module | Codex | `initial-setup` | Existing RFM tests |
| TODO | Build and smoke-test Docker environment | Unassigned | - | Docker Desktop |
| TODO | Implement raw-data validation | Unassigned | - | `data/raw/data.csv` |
| DONE | Complete RFM percent-rank scoring and output guards | Codex | `initial-setup` | Curated schema |
| TODO | Verify RFM Parquet reruns and full-data execution in Docker | RFM owner | `initial-setup` | Curated Parquet |
| DONE | Refine RFM curated-input contract validation | Codex | `initial-setup` | Curated schema |
| TODO | Implement RFM job | Unassigned | - | Curated schema |
| TODO | Implement anomaly-detection job | Unassigned | - | Threshold decision |
| TODO | Replace DAG placeholders with working operators | Unassigned | - | Spark jobs |
| DONE | Recheck GitHub pull; verify both local RFM files unchanged by SHA256 | Codex | `initial-setup` | Remote repository |
| DONE | Pull current upstream with --ff-only; confirmed already up to date on 2026-09-20 | Codex | `initial-setup` | Remote repository |
| DONE | Pull origin/main 4fe270f preserving 39 local files by SHA256 (2026-10-03) | Codex | `initial-setup` | Backup and retained stash |
| DONE | Prepare standalone supervised RFM publication and preserve local workflow patch | Codex | codex/rfm-supervised-20261019 | Adapt registry integration separately; preserve upstream DAG |
| IMPLEMENTED; ENVIRONMENT PENDING | Follow-up peer review: K cutoffs, ranking, tests, documentation and environment check | Codex | codex/rfm-supervised-20261019 | Docker availability |

## Historical notes




2026-10-19 correction: prior publication withdrawn with lease; model predicts numeric Cluster directly. All naming/holding/prior-label policy removed. Owner Codex, branch codex/rfm-supervised-20261019. 3 contract tests and syntax checks pass; Spark unavailable. No new commit/push; runtime review pending.

2026-10-19: User authorized republishing corrected direct-cluster RFM. Owner Codex; branch codex/rfm-supervised-20261019. Prepared exact source/test/docs scope; no VIP/prior-label policy in executable changes. 3 contract tests and 5 syntax checks pass. Shared workflow retained as unapplied patch; Docker/Spark validation remains pending.

DONE: Consolidate RFM helper/tests/docs; 20 focused tests passed. Owner Codex; branch codex/rfm-supervised-20261019.

2026-10-21: User authorized publishing consolidated RFM review fixes. Owner Codex; branch codex/rfm-supervised-20261019. Scope: RFM scripts, registry/DAG integration, tests and documentation only. Docker acceptance remains pending.

RFM cleanup 2026-10-21: owner Codex, branch codex/rfm-supervised-20261019. User authorized GitHub cleanup only: untrack 24 generated outputs (byte-for-byte local preservation verified), ignore their directories, remove obsolete RFM_DAG_HANDOFF_VI.md, update notebook prerequisites. Code, input fixtures and notebooks retained. Separate cleanup commit; pending peer-review code changes remain unstaged.

2026-10-21: Codex simplified RFM reporting at user request: removed optional agreement metric; preserved macro-F1 selection and per-class diagnostics. Synchronized review checkout and Documents initial-setup; backup C:\Users\Lenovo\.codex\.chatgpt-projects\g-p-6a9cd2b108788191b22e1d7768f56837\rfm_metrics_backup_20261021_224259. Verification: test_supervised_contracts.py in Documents checkout: 7 passed in 1.78s. No commit/push.

DONE 2026-10-22: Codex standalone real-data verification: six stages passed on Docker Spark 4.2.0, including daily retry. See RFM_SUPERVISED_VI.md for independent evidence. No activation/push.
