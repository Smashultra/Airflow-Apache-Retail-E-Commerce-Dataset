# TODO

| Status | Task | Owner | Branch | Dependency |
|---|---|---|---|---|
| DONE | Create collaborative project scaffold | Codex | `initial-setup` | None |
| TODO | Build and smoke-test Docker environment | Unassigned | - | Docker Desktop |
| DONE | Select the Kaggle dataset and document its Docker-first download | Team | `initial-setup` | None |
| DONE | Explain non-cancelled negative-quantity rows in EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Explain the remaining zero-unit-price rows below EDA cell 15 | Codex | `main` | `data.csv` |
| DONE | Inspect the seven customer-linked all-zero-price rows in EDA | Codex | `main` | `data.csv` |
| TODO | Implement raw-data validation | Unassigned | - | `data/raw/data.csv` |
| DONE | Implement PySpark cleaning job | Codex | `main` | Dataset schema |
| DONE | Tighten RFM cleaning filters and regenerate Parquet | Codex | `main` | Existing cleaning job |
| TODO | Implement RFM job | Unassigned | - | Curated schema |
| TODO | Implement anomaly-detection job | Unassigned | - | Threshold decision |
| TODO | Replace DAG placeholders with working operators | Unassigned | - | Spark jobs |
| DONE | Add Spark logic tests | Codex | `main` | Spark jobs |
| TODO | Complete report, setup evidence, and presentation | Unassigned | - | Working pipeline |
