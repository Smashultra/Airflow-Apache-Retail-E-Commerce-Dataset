from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from scripts.pyspark_clean import (
    DEFAULT_ANOMALIES_OUTPUT_PATH,
    DEFAULT_RFM_OUTPUT_PATH,
    TRANSACTION_SCHEMA,
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
        .config("spark.sql.session.timeZone", "UTC")
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


def test_read_transactions_uses_stable_business_types(transactions):
    expected_types = {
        "CustomerID": StringType,
        "InvoiceNo": StringType,
        "InvoiceDate": TimestampType,
        "Quantity": IntegerType,
        "UnitPrice": DoubleType,
    }

    assert transactions.schema == TRANSACTION_SCHEMA
    for column, expected_type in expected_types.items():
        assert isinstance(transactions.schema[column].dataType, expected_type)
    assert transactions.filter(F.col("InvoiceDate").isNull()).count() == 0
    assert transactions.filter(F.col("CustomerID").isNull()).count() == 2


def test_prepare_datasets_applies_each_output_rule(transactions):
    rfm, anomalies = prepare_datasets(transactions)

    invalid_customer_id = F.col("CustomerID").isNull() | F.isnan(
        F.col("CustomerID").cast("double")
    )

    assert transactions.count() == 8
    assert anomalies.count() == 7
    assert rfm.count() == 1
    assert anomalies.filter(invalid_customer_id).count() == 2
    assert rfm.filter(invalid_customer_id).count() == 0
    assert rfm.filter(F.upper(F.col("InvoiceNo")).startswith("C")).count() == 0
    assert rfm.filter(F.col("Quantity") <= 0).count() == 0
    assert rfm.filter(F.col("UnitPrice") <= 0).count() == 0


def test_prepare_datasets_excludes_service_lines_only_from_rfm(spark):
    rows = [
        ("10001", "12345", "Sold product", 1, None, 10.0, "12345", "UK"),
        ("10001", " POST ", "Postage", 1, None, 2.0, "12345", "UK"),
        ("10002", "m", "Manual", 1, None, 3.0, "12345", "UK"),
        ("10003", "DOT", "Dotcom Postage", 1, None, 4.0, "12345", "UK"),
        ("10004", "BANK CHARGES", "Bank Charges", 1, None, 5.0, "12345", "UK"),
        ("10005", "C2", "Carriage", 1, None, 6.0, "12345", "UK"),
        ("10006", "PADS", "Pads to match all cushions", 1, None, 7.0, "12345", "UK"),
        ("10007", "23574", " packing charge ", 1, None, 8.0, "12345", "UK"),
        ("10008", "23444", "Next Day Carriage", 1, None, 9.0, "12345", "UK"),
        ("10009", "23099", "French carriage lantern", 1, None, 10.0, "12345", "UK"),
    ]
    transactions = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
    # A timestamp is required by the existing complete-row RFM rule.
    transactions = transactions.withColumn(
        "InvoiceDate", F.to_timestamp(F.lit("2026-09-01 10:00:00"))
    )

    rfm, anomalies = prepare_datasets(transactions)

    assert {row.StockCode for row in rfm.select("StockCode").collect()} == {
        "12345",
        "23099",
    }
    assert anomalies.count() == len(rows)


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

    written_rfm = spark.read.parquet(str(rfm_output))
    written_anomalies = spark.read.parquet(str(anomalies_output))

    assert written_rfm.count() == 1
    assert written_anomalies.count() == 7
    assert written_rfm.schema == TRANSACTION_SCHEMA
    assert written_anomalies.schema == TRANSACTION_SCHEMA
