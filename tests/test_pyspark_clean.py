from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F

from scripts.pyspark_clean import (
    DEFAULT_ANOMALIES_OUTPUT_PATH,
    DEFAULT_RFM_OUTPUT_PATH,
    parse_args,
    prepare_datasets,
    read_transactions,
    run_cleaning_job,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-clean")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def transactions(spark):
    fixture_path = Path(__file__).parent / "fixtures" / "sample_transactions.csv"
    return read_transactions(spark, str(fixture_path))


def test_default_output_paths_follow_project_layout():
    args = parse_args([])

    assert args.rfm_output == DEFAULT_RFM_OUTPUT_PATH == "data/curated/RFM.parquet"
    assert (
        args.anomalies_output
        == DEFAULT_ANOMALIES_OUTPUT_PATH
        == "data/audit/anomalies.parquet"
    )


def test_prepare_datasets_applies_each_output_rule(transactions):
    rfm, anomalies = prepare_datasets(transactions)

    assert transactions.count() == 4
    assert anomalies.count() == 3
    assert rfm.count() == 2
    assert anomalies.filter(F.col("CustomerID").isNull()).count() == 1
    assert rfm.filter(F.col("CustomerID").isNull()).count() == 0
    assert rfm.filter(F.col("Quantity") < 0).count() == 1


def test_run_cleaning_job_writes_both_parquet_outputs(spark, tmp_path):
    fixture_path = Path(__file__).parent / "fixtures" / "sample_transactions.csv"
    rfm_output = tmp_path / "curated" / "RFM.parquet"
    anomalies_output = tmp_path / "audit" / "anomalies.parquet"

    run_cleaning_job(
        spark,
        str(fixture_path),
        str(rfm_output),
        str(anomalies_output),
    )

    assert rfm_output.is_dir()
    assert anomalies_output.is_dir()
    assert spark.read.parquet(str(rfm_output)).count() == 2
    assert spark.read.parquet(str(anomalies_output)).count() == 3
