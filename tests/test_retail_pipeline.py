"""Business boundary checks for the official DAG's Spark extensions."""

import csv
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from retail_contracts import COLUMNS
from retail_pipeline import customer_labels, read_landing


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
