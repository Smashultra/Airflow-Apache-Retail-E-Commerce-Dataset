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
docker compose exec airflow-scheduler python -m pytest -q tests/test_retail_contracts.py tests/test_retail_models.py tests/test_retail_dag.py tests/test_retail_training.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py
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

### Bootstrap the first model before daily scoring

Daily runs load a pinned classifier bundle (or a legacy KMeans model); they never fit one.
For the current classifier workflow, exact temporal split and UI parameters, follow
[RFM classifier DAG handoff](RFM_SUPERVISED_VI.md). Training defaults to `model_kind=classifier`.
Bootstrap a committed ETL snapshot without requiring a scoring model:

```powershell
docker compose exec airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g scripts/retail_training.py --bootstrap-date 2011-11-30 --run-id bootstrap_rfm_20111201
```

This preserves the original data and writes a new curated generation plus
`data/manifests/etl/online-retail-v1/2011-12-01.json`. The final printed path is
the committed ETL manifest. Run this command when no other Spark job is active:
manual spark-submit does not acquire the Airflow pool.

In the UI, trigger `retail_rfm_training_dag` with:

| Parameter | First-model demo value |
|---|---|
| training_cutoff | `2011-12-01` |
| effective_from | `2011-12-02` |
| source_version | `online-retail-v1` |
| etl_manifest | `null` (resolve the committed ETL index above) |
| rfm_k | `2` (demo configuration; validate k on this training snapshot) |

Wait for all five tasks to succeed in training mode. `publish_model` logs `MODEL_ACTIVATED`;
`data/manifests/models/registry.json` lists activations and model versions.
Classifier artifacts are under `data/models/rfm/version=<id>/bundle/`, with
`student`, `teacher` and `supervised_info`. Legacy KMeans uses `pipeline/`.
Training uses only transactions before its cutoff. Model effectiveness dates
in 2011 are simulated for this historical dataset, not actual deployment dates.

### Run daily scoring

Open <http://localhost:8080>, select `ecommerce_etl_dag`, enable it, and choose
**Trigger DAG**. Set:

| Parameter | Demo value |
|---|---|
| business_date | `2011-12-09` |
| source_version | `online-retail-v1` |
| model_version | `null` (resolve by cutoff) or the eligible model ID |
| rfm_k | `null` (read k from model; an explicit value must match) |
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
Customer rows include model_version, feature_version and score_reference_mode.
Daily rank scores still depend on the current customer population; fixed
centroids do not freeze rule thresholds or campaign membership.
Parquet directories contain multiple parts; read a dataset root with Spark,
not an arbitrary individual part. Legacy script commands and output paths
remain available independently of the DAG.

## Recovery and quality checks

Training is monthly (`0 0 1 * *`, UTC) and uses the ETL cutoff at the interval
end. If that snapshot is not committed yet, training fails clearly: rerun after
ETL finishes. Daily continues on its previously activated eligible model.
Training does not wait for daily scoring/completion and does not trigger daily.
Both DAGs use the same one-slot `retail_spark` pool. Keep historical DAGs paused
outside explicit demos/backfills; training end_date is the END of 2011-11-01 UTC
(23:59:59.999999). Both daily DAGs include the whole final day 2011-12-09 UTC.
This accepts manual UI dates with a non-midnight time on that last day without
adding another scheduled day/month. Convert UI timezone to UTC when selecting dates.
When manually triggering, explicitly set a historical logical date within the
DAG's start/end bounds, not today's date. Example: training logical date
2011-11-01T00:00:00Z with training_cutoff=2011-12-01. The report-only run can
use 2011-10-31T00:00:00Z with the SAME cutoff and ETL manifest. Choose unused
historical logical dates if these already belong to existing runs; changing
run_id alone may not avoid a logical-date conflict. The manual cutoff comes
from Params, independently of logical date. For daily, use logical date
2011-12-09T00:00:00Z and business_date=2011-12-09. Explicit logical dates after
task end_date can yield runs without task instances; create a new correctly
dated run rather than reusing the empty run.

Daily freezes model URI/hash/activation in its context. Retry or clear cannot
silently switch models, even after a newer activation. Changing model/source/k
or business code requires a new run. Training pins its ETL generation too.
Missing/corrupt/incompatible artifacts fail closed; no automatic fit/fallback.
Dates before the first eligible model are rejected rather than scored with a
future model. Backfill a new date only with training_cutoff < daily cutoff and
effective_from <= daily cutoff; replay an existing run uses its frozen binding.

Model publication appends an activation to a single atomically replaced
registry JSON under an exclusive lock; artifacts stay immutable. A failed
training/publication leaves the serving registry unchanged. After a model is
published, retraining it requires a new run; clearing only publication is
idempotent. To score with an older eligible version, create a new daily run
with explicit model_version. This is an explicit per-run rollback, not an
automatic rewrite of activation history.

Inspect active writers before removing a stale
`data/manifests/models/.registry.lock` after a crash. Do not edit model artifacts
or hashes to bypass validation. Old daily contexts from before this code change
cannot be resumed because the business-code fingerprint changed; preserve their
outputs and create new runs.

A missing source manifest waits up to 30 minutes; a committed but invalid
manifest/file fails immediately. Spark retries once with backoff. Clear the
failed task and its downstream tasks after fixing a transient issue. If source,
business parameters or business code changed, create a new DagRun instead:
the saved run context freezes these inputs. Re-running ETL requires re-running
both branches so the completion step sees one consistent ETL generation.
After either branch publication marker exists, do not replace that branch's
ETL/output generation in the same run. Both RFM and Audit tasks reuse their
checked snapshots on retry; changed generations require a new DagRun.
Each branch emits only its own Asset. Notify validates both markers and emits
RETAIL_SNAPSHOT_ASSET plus curated transactions; Dashboard listens only to the
aggregate Asset. Use a new daily run after upgrading; old branch events cannot
trigger the new aggregate schedule. See [Asset scheduling](ASSET_SCHEDULING_VI.md).

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
