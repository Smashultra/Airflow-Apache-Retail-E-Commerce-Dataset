# Airflow PySpark Retail ETL

Group assignment implementing a reproducible daily retail ETL pipeline. Apache Airflow schedules and monitors the workflow, while PySpark performs data cleaning, RFM aggregation, and anomaly detection.

## Pipeline

```text
start_pipeline
      |
validate_raw_data
      |
submit_pyspark_etl
      |
      +---------------------+
      |                     |
compute_rfm_metrics   detect_anomalies
      |                     |
      +----------+----------+
                 |
        notify_completion
```

The intended data flow is:

```text
CSV in data/raw/
  -> cleaned partitioned Parquet in data/curated/
  -> RFM output in data/analytics/rfm_daily/
  -> anomaly audit output in data/audit/anomalies/
```

The DAG structure and task dependencies exist, but all tasks are currently placeholders. ETL business logic has not been implemented yet.

## Project structure

```text
.
├── AGENTS.md                    Shared rules for contributors and coding agents
├── CONTRIBUTING.md              Branch, commit, verification, and handoff workflow
├── README.md                    Project overview and quick start
├── .env.example                 Safe template for local environment variables
├── .gitignore                   Excludes secrets, datasets, logs, and generated output
├── Dockerfile                   Airflow image extended with Java and PySpark
├── docker-compose.yaml          Local Airflow and PostgreSQL environment
├── requirements.txt             Python and Airflow provider dependencies installed in Docker
│
├── .agent/                      Tool-neutral shared project state
│   ├── README.md                State-directory conventions
│   ├── HANDOFF.md               Latest verified status, risks, and next action
│   ├── TODO.md                  Work queue, ownership, branches, and dependencies
│   ├── DECISIONS.md             Durable technical decisions and rationale
│   ├── PLANS.md                 Execution-plan rules and template
│   └── plans/
│       ├── active/              Plans currently being executed
│       └── archive/             Completed or cancelled plans
│
├── .agents/skills/              Project-specific Codex skills, added only when needed
├── .codex/
│   ├── config.toml              Trusted project-scoped Codex configuration
│   └── agents/                  Optional named Codex agent definitions
├── .github/
│   └── PULL_REQUEST_TEMPLATE.md Required PR rationale and verification fields
│
├── dags/
│   └── ecommerce_etl_dag.py     Daily Airflow DAG and task dependencies
├── scripts/
│   ├── pyspark_clean.py         Raw-data cleaning and curated Parquet output
│   ├── pyspark_rfm.py           Per-customer RFM metrics
│   └── pyspark_anomalies.py     Transaction anomaly detection
├── tests/
│   ├── fixtures/
│   │   └── sample_transactions.csv
│   └── test_pyspark_jobs.py     Focused automated checks
├── data/
│   ├── raw/                     Local input CSV files
│   ├── curated/                 Generated cleaned Parquet data
│   ├── analytics/rfm_daily/     Generated daily RFM results
│   └── audit/anomalies/         Generated anomaly records
├── docs/
│   ├── REPORT.md                Conceptual and architectural report
│   ├── SETUP.md                 Detailed local setup instructions
│   └── images/                  Architecture and demonstration screenshots
└── logs/                        Generated Airflow logs; not committed
```

Placeholder `.gitkeep` files preserve required empty directories in Git.

## Docker environment

The development environment deliberately uses Spark local mode. This is sufficient for the assignment dataset and keeps local setup small; a separate Spark cluster can be added if distributed execution becomes a requirement.

| Component | Version or mode | Responsibility |
|---|---|---|
| Apache Airflow | 3.3.1 | Scheduling, retries, dependencies, and monitoring |
| LocalExecutor | Local process execution | Runs Airflow tasks without Celery or Redis |
| PostgreSQL | 16 | Airflow metadata database |
| OpenJDK | 17 | JVM required by Spark |
| PySpark | 4.2.0 | ETL and analytics compute engine |
| Spark provider | 6.3.2 | Supplies `SparkSubmitOperator` and Spark hooks |
| pytest | 8.4.2 | Focused automated checks |

Compose starts five services:

- `postgres`: metadata database.
- `airflow-init`: database migration and initial admin creation.
- `airflow-apiserver`: web interface and API on port `8080`.
- `airflow-scheduler`: schedules tasks and executes local Spark submissions.
- `airflow-dag-processor`: parses DAG definitions separately from the scheduler.

The same `dags`, `scripts`, `tests`, `data`, and `logs` paths are mounted into the relevant Airflow containers under `/opt/airflow/`.

## Dataset

The team selected Kaggle's [E-Commerce Data](https://www.kaggle.com/datasets/carrie1/ecommerce-data), a copy of the UCI Online Retail transactions dataset. It contains one approximately 45.6 MB file named `data.csv` with the eight assignment fields: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, and `Country`.

The Kaggle CLI is already declared in `requirements.txt`. With the recommended Docker workflow, do not install the project requirements on the host: `docker compose build` installs them inside the shared image.

Configure your [Kaggle API credentials](https://github.com/Kaggle/kaggle-api) at `$HOME/.kaggle/kaggle.json`. After building the image, run this command from the repository root:

```powershell
docker compose run --rm --no-deps -v "${HOME}/.kaggle:/home/airflow/.kaggle:ro" airflow-scheduler bash -c "kaggle datasets download -d carrie1/ecommerce-data -p /opt/airflow/data/raw --unzip"
```

Verify the expected file:

```powershell
Get-Item data/raw/data.csv
```

The downloaded file is intentionally ignored by Git. Inside the Airflow containers it is available at `/opt/airflow/data/raw/data.csv` through the shared `data` volume.

## Quick start

Requirements: Docker Desktop with at least 4 GB of memory available, local port `8080` free, and Kaggle API credentials configured at `$HOME/.kaggle/kaggle.json`.

No local `pip install` is required. Building the image is the dependency-installation step because the Dockerfile runs `pip install -r requirements.txt` inside the image.

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm --no-deps -v "${HOME}/.kaggle:/home/airflow/.kaggle:ro" airflow-scheduler bash -c "kaggle datasets download -d carrie1/ecommerce-data -p /opt/airflow/data/raw --unzip"
docker compose up airflow-init
docker compose up -d
```

Generate and configure a Fernet key before sharing or deploying the environment; see [`docs/SETUP.md`](docs/SETUP.md).

Open <http://localhost:8080> and sign in with the credentials configured in `.env`.

## Verification and common commands

```powershell
docker compose ps
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler spark-submit --version
docker compose exec airflow-scheduler pytest /opt/airflow/tests
docker compose down
```

The destructive cleanup command `docker compose down --volumes` is documented separately in [`docs/SETUP.md`](docs/SETUP.md).

## Data and generated files

The full dataset, `.env`, Airflow runtime configuration, logs, bytecode, curated Parquet files, and analytics output must not be committed. Only the small test fixture and `.gitkeep` directory placeholders belong in Git.

## Team workflow

Before editing, read [`AGENTS.md`](AGENTS.md), [`.agent/HANDOFF.md`](.agent/HANDOFF.md), [`.agent/TODO.md`](.agent/TODO.md), and the relevant active plan. Claim substantial work in the TODO table and follow [`CONTRIBUTING.md`](CONTRIBUTING.md) when creating commits and pull requests.

Git commits and pull requests are the audit trail for individual changes. `.agent` stores only current state, plans, ownership, important decisions, verification evidence, and handoff information.

## Current status

- Project and collaboration structure: complete on `initial-setup`.
- Docker configuration: statically validated; not built because Docker is unavailable in the current host shell.
- DAG topology: complete placeholder skeleton.
- Dataset selection and download location: complete.
- Cleaning, RFM, anomaly detection, notification, and data validation logic: pending.
- Report, screenshots, and presentation: pending.

See [`.agent/TODO.md`](.agent/TODO.md) for the live work queue.
