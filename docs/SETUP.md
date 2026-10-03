# Local Setup

## Requirements

- Docker Desktop with Docker Compose.
- About 8 GB of memory available to Docker for one Spark JVM at a time.
- About 20 GB of free disk space for building the image and generated data.
- Port `8080` available locally.

No host installation of Airflow, Java, Spark, or Python packages is required.

## Configure

From the repository root:

```powershell
Copy-Item .env.example .env
```

Do this only on first setup; keep an existing working `.env`. Change the local credentials in `.env`. A Fernet key can be generated with the Airflow image:

```powershell
docker run --rm apache/airflow:3.3.1 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the result into `AIRFLOW_FERNET_KEY`.

## Build and initialize

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open <http://localhost:8080> and use the credentials from `.env`.

## Verify

```powershell
docker compose ps
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler spark-submit --version
docker compose exec airflow-scheduler python -m pytest -q tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py
```

## Prepare and run the official DAG

Run commands from the repository directory, alongside `docker-compose.yaml`.
Download the dataset as described in the README first; `data/raw/data.csv` must
exist. Its dates are 2010-12-01 through 2011-12-09.

For an existing installation, apply the updated connections and Python paths:

```powershell
docker compose up -d
docker compose exec airflow-scheduler python scripts/prepare_daily_landing.py
docker compose exec airflow-scheduler airflow pools set retail_spark 1 "One local Spark JVM"
docker compose exec airflow-scheduler airflow dags list-import-errors
```

The preparation command preserves the original CP1252 CSV and creates verified
UTF-8 daily files under `data/raw/landing/online-retail-v1/`. Header-only files
represent days with no transactions. A committed source version is immutable;
repeating preparation with unchanged source is safe. Changed source requires a
new `--source-version` and the corresponding DAG parameter.

Open <http://localhost:8080>, select `ecommerce_etl_dag`, enable it, and choose
**Trigger DAG**. Set:

| Parameter | Demo value |
|---|---|
| business_date | `2011-12-09` |
| source_version | `online-retail-v1` |
| rfm_k | `2` |
| churn_days | `90` |

The exclusive cutoff is `2011-12-10`. Manual runs require an explicit business
date; do not use today's date for this historical dataset. The daily timetable
is bounded to the dataset period. Scheduled/backfill runs derive their day from
the data interval and reject a conflicting override. For a backfill, use the
Airflow 3 Backfill UI and select a small historical range first: every day
recomputes a cumulative snapshot. Pause the DAG after the demonstration.

The graph is `start_pipeline >> validate_raw_data >> submit_pyspark_etl`, then
two branches (`compute_rfm_metrics`, `detect_anomalies`) join at
`notify_completion`. The one-slot pool runs Spark branches sequentially on an
8 GB Docker allocation. Increase to two slots only after measuring enough RAM
for two 3 GB heaps plus JVM/Python/Airflow overhead.

Success publishes `data/manifests/published/2011-12-10.json`. Read its `stages`
output paths to locate the exact Parquet generations:

- ETL: `data/curated/run_date=2011-12-10/version=.../transactions/year=.../month=.../`.
- RFM: `data/analytics/rfm_daily/run_date=2011-12-10/version=.../customers/`.
- Audit: `data/audit/anomalies/run_date=2011-12-10/version=.../` containing row,
  order and flagged-order results.

Each task logs its stage summary; completion logs `PIPELINE_COMPLETE`.
Parquet directories contain multiple parts; read a dataset root with Spark,
not an arbitrary individual part. Legacy script commands and output paths
remain available independently of the DAG.

## Recovery and quality checks

A missing source manifest waits up to 30 minutes; a committed but invalid
manifest/file fails immediately. Spark retries once with backoff. Clear the
failed task and its downstream tasks after fixing a transient issue. If source,
business parameters or business code changed, create a new DagRun instead:
the saved run context freezes these inputs. Re-running ETL requires re-running
both branches so the completion step sees one consistent ETL generation.

Incomplete attempts remain unpublished and may be inspected. Do not manually
point consumers at them. Publication replaces one small JSON pointer only after
all stage manifests and `_SUCCESS` markers pass. A crash during publication can
leave `data/manifests/.publish-<date>.lock`; inspect active runs before manually
removing a stale lock. A 60-minute deadline logs a warning via a synchronous
callback; task/DAG timeouts separately terminate excessive runtime. It is not
an email notification or an external scheduler watchdog.

For development lint, install `requirements-dev.txt` in a development Python
environment and run `ruff check dags scripts tests`. The project-structure test
`tests/test_pyspark_jobs.py` runs on the host checkout, since Docker mounts only
runtime folders rather than the whole repository.

## Stop

```powershell
docker compose down
```

To remove the local PostgreSQL volume as well:

```powershell
docker compose down --volumes
```

The `--volumes` command deletes the local Airflow metadata database and should only be used intentionally.
