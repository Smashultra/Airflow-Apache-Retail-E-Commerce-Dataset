# Technical Decisions

Use `YYYY-MM-DD HH:mm UTC+07:00` in every decision heading.

## 2026-10-08 23:30 UTC+07:00 - Two-tier anomaly rule base

- Status: Implemented on `feature/anomaly-rule-base` at the user's request; thresholds pending recalibration review.
- Context: The previous detector used six business checks, global per-product IQR and a global mean + 3 sigma invoice threshold. EDA showed missing CustomerID is invoice-level, DOT invoices are almost all unidentified, retail prices are ~1.97x wholesale on the same product, and stock-count rows are a distinct record type.
- Decision: Classify rows (record_type) and invoices (channel) before any rule. Tier 1 data rules map to ISO/IEC 25012 / DAMA dimensions; a missing CustomerID is a channel attribute, not a data anomaly. Tier 2 business rules follow Chandola et al. (2009) point/contextual/collective types. Contextual checks use the Iglewicz-Hoaglin modified z-score (3.5) on log values with hierarchical baselines (customer x stock >= 5, customer >= 20, channel x stock >= 30; orders: customer >= 5, channel >= 30) plus materiality limits fixed in advance (quantity x3, price +-30%, order x3). Each flag carries value_at_risk and severity (high >= 1,000 GBP, medium >= 100 GBP). `assess_orders` lives in `pyspark_anomalies.py`.
- Consequences: Output schema changed (14 ordered checks with tier/severity; `--z-threshold` replaces `--iqr-multiplier`). Low-severity contextual flags are numerous on this data (see doc 9.3). `build_output` checkpoints lineage and is eager.
- Revisit when: the user approves recalibrated constants, labelled outcomes become available, or online scoring is required.

## 2026-09-29 13:23 UTC+07:00 - Official historical DAG execution contracts

- Status: Implemented under the user's explicit request to code the official DAG.
- Decision: Keep exactly six business tasks: Empty start, FileSensor-derived validation, three SparkSubmit tasks, and all-success publication. Use SDK timetable/deadline declarations compatible with Airflow 3.3.1. Manual runs require business_date; scheduled/backfill runs use a daily UTC interval bounded to dataset coverage.
- Decision: Bootstrap immutable UTF-8 daily files from the CP1252 source, checking CSV round-trip and SHA-256. Use a source-version name plus content hashes, including explicit zero-row dates. Original data remains unchanged.
- Decision: Pass one frozen `--pipeline-context` path to Spark rather than independent CLI parameters. Version outputs per attempt and publish only manifests whose code/source/context and ETL hashes agree. Preserve legacy CLI behavior and outputs.
- Decision: Add month-partitioned clean transactions, parse quarantine, customer inactivity labels and retrospective eligible-invoice totals above mean + 3 sample standard deviations (minimum 30 orders, GBP). Missing customer IDs remain eligible for order checks. Global RFM Window scoring retains its documented customer-scale limitation.
- Decision: Default to local[2], 3 GB driver and one retail_spark pool slot. The two analytics branches are logically independent but run sequentially under this resource profile. Other user workloads share Docker memory; do not modify them.
- Consequences: Full-history snapshots trade storage/work for independent historical reruns. Code changes require new runs. Local publication uses same-filesystem atomic JSON replacement plus an exclusive lock; crashed locks require inspection. Deadline callbacks log locally; no external messages or watchdog are configured.
- Revisit when: Distributed/incremental scale, measured two-JVM capacity, validated churn/anomaly outcomes, or externally delivered alerts become explicit requirements.

## 2026-09-28 17:55 UTC+07:00 - Updated assignment rubric is authoritative

- Status: Accepted from the user's explicit clarification.
- Context: The original DOCX batch assignment contained a conflicting Kafka/Streaming rubric and illustration. The user supplied a replacement rubric directly.
- Decision: Use DOCX sections 1–5 for functional requirements and the updated 25/25/25/15/10 rubric for grading. Kafka/Flume, streaming intent/recommendation and Cassandra are not required by this task.
- Consequences: The design now explicitly addresses Sensor usage, deadline/SLA monitoring compatible with Airflow 3.3.1, PEP8 and evidence for all five categories. These implementation choices remain proposals until implemented and tested; scope no longer awaits clarification.
- Revisit when: The user or instructor supplies revised requirements.

## 2026-09-09 11:11 UTC+07:00 - Shared project-state convention

- Status: Accepted
- Context: Multiple people and coding agents will modify the repository.
- Decision: Use Git and pull requests for every file change; use `.agent` only for current state, work ownership, plans, and durable decisions.
- Reason: This preserves an auditable history without duplicating every edit in Markdown files.
- Consequences: Contributors must update TODO and HANDOFF at task boundaries.
- Revisit when: The team adopts an external issue tracker as the project source of truth.

## 2026-09-09 11:11 UTC+07:00 - Use LocalExecutor for local development

- Status: Accepted
- Context: The existing Docker Compose used CeleryExecutor, Redis, Celery workers, and Flower, which are unnecessary for the assignment-scale local environment.
- Decision: Use Airflow LocalExecutor with PostgreSQL. Run PySpark jobs through `spark-submit --master local[*]` from the Airflow image instead of adding Spark master and worker containers.
- Reason: Fewer containers make setup and troubleshooting easier for all team members while preserving the separation between Airflow orchestration and Spark processing.
- Consequences: Redis, Celery workers, and Flower are removed. Airflow tasks and Spark jobs share resources provided to Docker on the local host. The report must distinguish this setup from production YARN, Kubernetes, or Databricks deployments.
- Revisit when: The project must demonstrate distributed workers, execute on multiple machines, or exceeds the resources available to Docker locally.

## 2026-09-09 11:11 UTC+07:00 - Generated files stay outside Git

- Status: Accepted
- Context: The initial commit contains Airflow logs, generated config, bytecode, and local environment values.
- Decision: Ignore generated config, logs, bytecode, full datasets, generated Parquet output, and `.env`.
- Reason: These files create noise, conflicts, and potential secret exposure.
- Consequences: Each contributor generates runtime files locally and commits only `.env.example` and directory placeholders.
- Revisit when: A small generated artifact is explicitly required as submission evidence.

## 2026-09-09 11:26 UTC+07:00 - Use the Kaggle UCI Online Retail dataset

- Status: Accepted
- Context: The assignment permits the standard UCI e-commerce dataset or the larger relational Olist dataset.
- Decision: Use Kaggle dataset `carrie1/ecommerce-data` and store its extracted `data.csv` at `data/raw/data.csv`.
- Reason: Its single-file, eight-column schema directly matches the assignment and is sufficient for cleaning, RFM, churn, and anomaly tasks.
- Consequences: The download remains outside Git and is mounted in Airflow containers at `/opt/airflow/data/raw/data.csv`.
- Revisit when: The instructor requires relational joins or a scaled multi-table demonstration.

## 2026-09-17 23:07 UTC+07:00 - Split cleaning outputs by downstream null policy

- Status: Accepted
- Context: RFM preparation requires complete rows, while anomaly analysis must retain incomplete records for later inspection.
- Decision: `scripts/pyspark_clean.py` writes full-row-deduplicated data to both outputs. For `data/curated/RFM.parquet`, it drops rows with any missing value, cancelled invoices whose `InvoiceNo` starts with `C`, and rows where `Quantity <= 0` or `UnitPrice <= 0`. It writes the less restrictive output to `data/audit/anomalies.parquet`.
- Reason: A single raw-data scan can prepare datasets with the different retention policies required by the two downstream analyses.
- Consequences: Missing values, cancelled invoices, and non-positive quantities or prices remain in the anomaly input. Spark treats each `.parquet` output path as a directory of Parquet part files and overwrites that directory on each run.
- Revisit when: The downstream RFM or anomaly jobs require a stricter schema, incremental writes, or additional business-rule filters.

## 2026-09-21 10:06 UTC+07:00 - Use stable business types for retail transactions

- Status: Accepted
- Context: Pandas inferred `CustomerID` as `float64` and left `InvoiceDate` as text, while Spark inferred types independently and persisted `InvoiceDate` as a string in both Parquet outputs.
- Decision: Treat `CustomerID` and `InvoiceNo` as string identifiers, `InvoiceDate` as a pandas datetime/Spark timestamp, `Quantity` as a 32-bit integer, and `UnitPrice` as a 64-bit floating-point value. Apply these types while reading the CSV instead of relying on inference.
- Reason: Identifier columns are not numeric measures, timestamps should support time operations directly, and an explicit schema prevents environment- or sample-dependent Parquet schemas.
- Consequences: Both Parquet datasets must be regenerated after this change. Missing customer IDs remain null and RFM filtering behavior remains unchanged.
- Revisit when: The source format changes, quantities exceed the 32-bit range, or financial calculations require a fixed-precision decimal price.

## 2026-09-21 16:52 UTC+07:00 - Exclude service charges from RFM transactions

- Status: Accepted
- Context: Positive, non-cancelled fee lines were still included in RFM input and could inflate monetary values or transaction counts. The raw CSV also uses numeric stock codes for packing and next-day carriage charges.
- Decision: Exclude RFM rows with `StockCode` `POST`, `M`, `DOT`, `BANK CHARGES`, `C2`, or `PADS`, and rows with exact `Description` `PACKING CHARGE` or `NEXT DAY CARRIAGE`, after trimming and case normalization. Keep these rows in the anomaly output.
- Reason: These are known non-product lines in the selected dataset. Exact matches protect products whose descriptions incidentally contain words such as "carriage".
- Consequences: The RFM Parquet output must be regenerated; downstream RFM totals exclude these line items.
- Revisit when: New input data contains other documented service codes or descriptions.


## 2026-09-23 15:30 UTC+07:00 - PySpark anomaly assessment scope

- Status: Accepted and implemented.
- Context: The EDA retains 135,037 rows without CustomerID, most of which remain eligible for product, transaction and time checks. Statistical flags are review candidates, and keyword interpretations require same-StockCode product-name context.
- Decision: Implement `scripts/pyspark_anomalies.py` in PySpark. Retain every source row in scope and all eight original columns; keep data-quality flags, business context and anomaly checks distinct. Evaluate retrospectively using rows with `InvoiceDate < run_date`; allow rows with missing dates only in applicable business checks. Do not impute CustomerID or automatically drop/repair/label fraud.
- Decision: V1 uses six explainable business checks and upper product-level IQR checks for Quantity, UnitPrice and line value. Start with multiplier 3 and minimum 30 eligible rows; require positive IQR; do not fallback to global thresholds. Record each check as flagged, not_flagged or not_applied with reason and evidence. Aggregate status is computed after all checks; an inapplicable check does not imply the transaction is unassessable.
- Decision: Write results to a date partition under `data/audit/anomaly_results`, overwriting only the requested run-date partition after path-safety and row-preservation checks.
- Reason: This keeps missing-ID transactions useful for transaction-level analysis while preserving traceability and avoiding unsupported conclusions.
- Consequences: IQR thresholds are retrospective review aids, not validated fraud thresholds or online detection. Implementation and acceptance evidence are in `.agent/plans/archive/2026-09-23-pyspark-anomalies.md`.
- Revisit when: There are validated anomaly labels, a need to score new transactions using past-only reference data, or domain-approved alternative handling for sparse/zero-IQR products.
