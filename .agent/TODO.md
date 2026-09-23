# TODO

| Status | Task | Owner | Branch | Dependency |
|---|---|---|---|---|
| DONE | Implement PySpark anomaly detector per accepted scope | Codex | `main` | `.agent/plans/archive/2026-09-23-pyspark-anomalies.md` |
| DONE | Write Vietnamese A-G anomalies EDA interpretation guide | Codex | `main` | Current executed notebook outputs |
| DONE | Validate description keyword flags against product names by StockCode | Codex | `main` | User correction; anomalies EDA |
| DONE | Implement and execute standalone anomalies EDA notebook | Codex | `main` | Approved EDA-only plan; local CSV and Parquet |
| DONE | Create collaborative project scaffold | Codex | `initial-setup` | None |
| DONE | Pull latest GitHub main on 2026-09-22; verified HEAD equals origin/main at b260d6b | Codex | `main` | None |
| TODO | Build and smoke-test Docker environment | Unassigned | - | Docker Desktop |
| DONE | Select the Kaggle dataset and document its Docker-first download | Team | `initial-setup` | None |
| DONE | Explain non-cancelled negative-quantity rows in EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Explain the remaining zero-unit-price rows below EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Inspect the seven customer-linked all-zero-price rows in EDA | Codex | `main` | `data.csv` |
| TODO | Implement raw-data validation | Unassigned | - | `data/raw/data.csv` |
| DONE | Implement PySpark cleaning job | Codex | `main` | Dataset schema |
| DONE | Tighten RFM cleaning filters and regenerate Parquet | Codex | `main` | Existing cleaning job |
| DONE | Exclude service and fee lines from RFM Parquet and refresh related checks/docs | Codex | `main` | Existing cleaning job and raw dataset |
| DONE | Standardize retail column types across EDA, Spark, and Parquet outputs | Codex | `main` | Raw dataset schema |
| TODO | Implement RFM job | Unassigned | - | Curated schema |
| DONE | Implement anomaly-detection job from 2026-09-23 plan | Codex | `main` | `.agent/plans/archive/2026-09-23-pyspark-anomalies.md` |
| TODO | Replace DAG placeholders with working operators | Unassigned | - | Spark jobs |
| DONE | Add Spark logic tests | Codex | `main` | Spark jobs |
| TODO | Complete report, setup evidence, and presentation | Unassigned | - | Working pipeline |
