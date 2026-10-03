"""Business boundary checks for the official DAG's Spark extensions."""

import csv
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from pyspark_clean import TRANSACTION_SCHEMA
from retail_contracts import COLUMNS
from retail_pipeline import assess_orders, customer_labels, read_landing


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-pipeline-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def transaction(invoice, price=10.0, customer=None, quantity=1, stock="A"):
    return (
        invoice,
        stock,
        "Product",
        quantity,
        datetime(2011, 12, 9),
        price,
        customer,
        "UK",
    )


def test_csv_quotes_unicode_and_parse_quarantine(spark, tmp_path):
    path = tmp_path / "landing.csv"
    description = 'Caf\u00e9, "gift"\nsecond line'
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(COLUMNS)
        writer.writerows(
            [
                ["1", "A", description, 2, "12/9/2011 10:00", 3, "1", "UK"],
                ["2", "B", "B", "bad", "invalid", 1, "", "UK"],
            ]
        )
    typed, rejected = read_landing(spark, [str(path)])
    rows = {r.InvoiceNo: r for r in typed.collect()}
    assert rows["1"].Description == description
    assert rows["1"].InvoiceDate == datetime(2011, 12, 9, 10)
    assert rows["2"].Quantity is None
    assert rows["2"].CustomerID is None
    failures = rejected.collect()
    assert len(failures) == 1
    assert failures[0].raw_Quantity == "bad"


def test_order_total_missing_customer_and_invalid_line(spark):
    rows = [transaction(str(i), price=10.0 + i % 3) for i in range(40)]
    rows += [
        transaction("BIG", 500.0, stock="A"),
        transaction("BIG", 500.0, stock="B"),
        transaction("BAD", 2000.0),
        transaction("BAD", -1.0),
        transaction("C1", 9000.0),
    ]
    result = assess_orders(
        spark.createDataFrame(rows, TRANSACTION_SCHEMA), "2011-12-10"
    )
    by_id = {r.invoice_key: r for r in result.collect()}
    assert by_id["BIG"].status == "flagged"
    assert by_id["BIG"].order_total == 1000
    assert by_id["BIG"].customer_ids == 0
    assert by_id["BAD"].status == "not_assessable"
    assert by_id["BAD"].order_total is None
    assert by_id["C1"].reason == "cancelled_invoice"


def test_sparse_or_constant_reference_is_not_a_negative_finding(spark):
    frame = spark.createDataFrame([transaction("1")], TRANSACTION_SCHEMA)
    assert (
        assess_orders(frame, "2011-12-10").first().reason
        == "insufficient_reference"
    )
    frame = spark.createDataFrame(
        [transaction(str(i)) for i in range(30)], TRANSACTION_SCHEMA
    )
    assert (
        assess_orders(frame, "2011-12-10").first().reason
        == "zero_or_invalid_stddev"
    )


def test_churn_threshold_priority_and_insufficient_history(spark):
    frame = spark.createDataFrame(
        [("A", 89, 5, 5, 5), ("B", 90, 1, 5, 5), ("C", 91, 1, 1, 1)],
        "CustomerID string, Recency int, R_score int, "
        "F_score int, M_score int",
    )
    result = {
        r.CustomerID: r for r in customer_labels(frame, 90, 365).collect()
    }
    assert not result["A"].is_inactive
    assert result["B"].segment == "high_value_inactive"
    assert result["C"].segment == "inactive"
    assert (
        customer_labels(frame, 90, 30).first().churn_assessment_status
        == "insufficient_history"
    )
