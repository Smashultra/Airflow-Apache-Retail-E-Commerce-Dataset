# Local Setup

## Requirements

- Docker Desktop with Docker Compose.
- At least 4 GB of memory available to Docker.
- Port `8080` available locally.

No host installation of Airflow, Java, Spark, or Python packages is required.

## Configure

From the repository root:

```powershell
Copy-Item .env.example .env
```

Change the local credentials in `.env`. A Fernet key can be generated with the Airflow image:

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
docker compose exec airflow-scheduler pytest /opt/airflow/tests
```

## Stop

```powershell
docker compose down
```

To remove the local PostgreSQL volume as well:

```powershell
docker compose down --volumes
```

The `--volumes` command deletes the local Airflow metadata database and should only be used intentionally.
