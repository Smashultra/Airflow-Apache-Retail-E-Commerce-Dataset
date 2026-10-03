# TODO

| Status | Task | Owner | Branch | Dependency |
|---|---|---|---|---|
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
