# Technical Decisions

Use `YYYY-MM-DD HH:mm UTC+07:00` in every decision heading.

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
