"""Behavior checks for the retrospective anomaly audit job."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from scripts.pyspark_anomalies import (
    build_output,
    parse_args,
    validate_input_contract,
    validate_output,
    write_results,
)
from scripts.pyspark_clean import TRANSACTION_SCHEMA


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-anomalies")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def row(invoice, stock="100", description="Product", quantity=1, price=1.0,
        customer="1", when=datetime(2011, 12, 9), country="UK"):
    return (invoice, stock, description, quantity, when, price, customer, country)


def checks(record):
    return {check.rule: check for check in record.check_results}


def test_cli_rejects_bad_parameters_and_overlapping_paths():
    from scripts.pyspark_anomalies import validate_paths

    for extra in (["--run-date", "2011-02-30"], ["--iqr-multiplier", "nan"],
                  ["--iqr-multiplier", "0"], ["--min-samples", "1"]):
        arguments = ["--input", "a", "--output", "b", "--run-date", "2011-12-10"]
        if extra[0] == "--run-date":
            arguments[-1] = extra[1]
        else:
            arguments += extra
        with pytest.raises(SystemExit):
            parse_args(arguments)
    args = parse_args(["--input", "a", "--output", "b", "--run-date", "2011-12-10"])
    assert args.iqr_multiplier == 3.0 and args.min_samples == 30
    with pytest.raises(ValueError, match="overlap"):
        validate_paths("/tmp/data/input", "/tmp/data")
    with pytest.raises(ValueError, match="overlap"):
        validate_paths("/tmp/data", "/tmp/data/output")
    with pytest.raises(ValueError, match="local filesystem"):
        validate_paths("file:/tmp/data/input", "/tmp/data")


def test_contract_cutoff_quality_and_business_checks(spark):
    rows = [
        row("C1", quantity=1, description="cancel"),
        row("R1", quantity=-1, description="return"),
        row("P1", price=-2.0, description="negative price"),
        row("Z0", price=0.0, description="zero invoice 1"),
        row("Z0", price=0.0, description="zero invoice 2"),
        row("ZN", price=0.0, description="zero with null"),
        row("ZN", price=None, description="null price"),
        row("ZPN", price=0.0, description="zero with positive"),
        row("ZPN", price=2.0, description="positive with null"),
        row("ZPN", price=None, description="null with positive"),
        row("M", customer="1", description="first ID"),
        row("M", customer="2", description="second ID"),
        row("PART", customer="1", description="one ID"),
        row("PART", customer=None, description="missing ID"),
        row(None, customer=None, description=None, price=float("nan")),
        row("INF", price=float("inf"), description="infinite price"),
        row("OVERFLOW", quantity=2_147_483_647, price=1e308, description="overflow value"),
        row("NODATE", when=None, description="missing date"),
        row("TODAY", when=datetime(2011, 12, 10)),
        row("FUTURE", when=datetime(2012, 1, 1)),
    ]
    source = spark.createDataFrame(rows, TRANSACTION_SCHEMA)
    validate_input_contract(source)
    with pytest.raises(ValueError, match="UnitPrice"):
        validate_input_contract(source.withColumn("UnitPrice", source.UnitPrice.cast("string")))
    result = build_output(source, "2011-12-10", min_samples=30)
    validate_output(source, result, "2011-12-10")
    found = {r.Description: r for r in result.collect()}
    assert len(found) == len(rows) - 2
    assert checks(found["cancel"])["cancel_nonnegative_quantity"].status == "flagged"
    assert checks(found["return"])["negative_quantity_non_cancel"].status == "flagged"
    assert checks(found["negative price"])["negative_unit_price"].status == "flagged"
    assert checks(found["zero invoice 1"])["all_zero_price_invoice"].status == "flagged"
    assert checks(found["zero with null"])["all_zero_price_invoice"].status == "not_applied"
    assert checks(found["zero with null"])["zero_price_in_priced_invoice"].status == "not_applied"
    assert checks(found["zero with positive"])["zero_price_in_priced_invoice"].status == "flagged"
    assert checks(found["first ID"])["multiple_customer_ids"].status == "flagged"
    assert checks(found["one ID"])["multiple_customer_ids"].status == "not_applied"
    assert "missing_customer_id" in found["missing ID"].data_quality_flags
    assert "invalid_unit_price" in found["infinite price"].data_quality_flags
    assert found["infinite price"].line_value is None
    assert found["overflow value"].line_value is None
    assert "invalid_line_value" in found["overflow value"].data_quality_flags
    assert "invalid_unit_price" not in found["overflow value"].data_quality_flags
    assert found["missing date"].assessment_status == "not_flagged"
    assert checks(found["missing date"])["high_quantity"].reason == "missing_invoice_date"
    assert checks(found[None])["cancel_nonnegative_quantity"].reason == "missing_invoice_no"
    assert found[None].assessment_status == "not_assessable"
    assert all(len(r.check_results) == 9 for r in found.values())


def test_whitespace_only_identifiers_and_text_are_missing(spark):
    source = spark.createDataFrame([
        row("\t", stock="\t", description="\t \n", price=0.0,
            customer="\t", country="\t"),
        row("\t", stock="\n", description="\n", price=1.0,
            customer="\n", country="\n"),
    ], TRANSACTION_SCHEMA)
    records = build_output(source, "2011-12-10").collect()
    assert len(records) == 2
    for record in records:
        assert set(("missing_invoice_no", "missing_stock_code", "missing_description",
                    "missing_customer_id", "missing_country")).issubset(record.data_quality_flags)
        assert checks(record)["zero_price_in_priced_invoice"].reason == "missing_invoice_no"
        assert checks(record)["multiple_customer_ids"].reason == "missing_invoice_no"


def test_description_context_uses_two_distinct_invoices_and_same_stock(spark):
    rows = [
        row("1", "85084", "HOLLY TOP CHRISTMAS STOCKING"),
        row("2", "85084", "HOLLY TOP CHRISTMAS STOCKING"),
        row("3", "85084", "stock adjustment", price=0.0),
        row("4", "A1", "BROWN CHECK BAG"),
        row("5", "A1", "BROWN CHECK BAG"),
        row("6", "A1", "damaged", price=0.0),
        row("7", "B1", "damaged", price=0.0),
        row("8", "C1", "BROWN CHECK BAG", price=0.0),
        row("9", "D1", "MOULD HEART"),
        row("10", "D1", "MOULD HEART"),
        row("11", "D1", "OTHER CHECK"),
        row("12", "D1", "OTHER CHECK"),
        row("13", "E1", "SAMPLE"),
        row("13", "E1", "SAMPLE"),
        row("14", "E1", "damaged", price=0.0),
        row("15", "F1", "FUTURE CHECK", when=datetime(2011, 12, 11)),
        row("16", "F1", "FUTURE CHECK", when=datetime(2011, 12, 11)),
        row("17", "F1", "FUTURE CHECK", price=0.0),
    ]
    result = build_output(spark.createDataFrame(rows, TRANSACTION_SCHEMA), "2011-12-10", min_samples=30)
    found = {(r.InvoiceNo, r.StockCode): r for r in result.collect()}
    assert "stock_words" not in found["1", "85084"].context_flags
    assert "stock_words" in found["3", "85084"].context_flags
    assert "keyword_product_name" in found["4", "A1"].context_flags
    assert "stock_words" not in found["4", "A1"].context_flags
    assert "damage_words" in found["6", "A1"].context_flags
    assert "keyword_unresolved" in found["7", "B1"].context_flags
    assert "keyword_unresolved" in found["8", "C1"].context_flags
    assert "keyword_product_name" in found["9", "D1"].context_flags
    assert "keyword_product_name" in found["11", "D1"].context_flags
    assert "keyword_unresolved" in found["14", "E1"].context_flags
    assert "keyword_unresolved" in found["17", "F1"].context_flags
    assert ("15", "F1") not in found
    assert result.count() == len(rows) - 2


def test_exact_iqr_boundaries_and_exclusions(spark):
    rows = []
    for stock, values in (("101", [1, 2, 3, 4, 20]), ("102", [1, 2, 3, 4, 10])):
        for index, quantity in enumerate(values):
            rows.append(row(f"{stock}{index}", stock, f"{stock}-{index}", quantity,
                            customer=None if quantity == 20 else "1"))
    for index, price in enumerate((1.0, 2.0, 3.0, 4.0, 20.0)):
        rows.append(row(f"103{index}", "103", f"103-{index}", price=price))
    rows += [row("CCANCEL", "101", "cancelled", 100),
             row("SERVICE", "101", "PACKING CHARGE", 100),
             row("NULLDATE", "101", "no date", 100, when=None)]
    source = spark.createDataFrame(rows, TRANSACTION_SCHEMA)
    result = build_output(source, "2011-12-10", min_samples=5)
    found = {r.Description: r for r in result.collect()}
    high = checks(found["101-4"])["high_quantity"]
    assert (high.status, high.reference_count, high.q1, high.q3, high.upper_bound) == (
        "flagged", 5, 2.0, 4.0, 10.0)
    assert checks(found["102-4"])["high_quantity"].status == "not_flagged"
    assert checks(found["101-4"])["high_line_value"].status == "flagged"
    assert checks(found["101-4"])["high_unit_price"].reason == "zero_iqr"
    assert checks(found["103-4"])["high_unit_price"].status == "flagged"
    assert checks(found["no date"])["high_quantity"].reason == "missing_invoice_date"
    assert checks(found["PACKING CHARGE"])["high_quantity"].reason == "service_or_special_code"
    assert checks(found["101-4"])["high_quantity"].observed_value == 20.0
    assert "missing_customer_id" in found["101-4"].data_quality_flags
    sparse = build_output(source, "2011-12-10", min_samples=30)
    assert checks(next(r for r in sparse.collect() if r.Description == "101-4"))["high_quantity"].reason == "insufficient_product_samples"


def test_exact_interpolation_minimum_30_and_duplicate_rows(spark):
    rows = [row(str(index), "301", "sample", quantity=index) for index in range(1, 30)]
    rows.append(row("30", "301", "sample", quantity=100))
    rows.extend([row("DUP", "302", "same"), row("DUP", "302", "same")])
    source = spark.createDataFrame(rows, TRANSACTION_SCHEMA)
    result = build_output(source, "2011-12-10")
    validate_output(source, result, "2011-12-10")
    assert result.count() == len(rows)
    assert result.filter(result.InvoiceNo == "DUP").count() == 2
    high = checks(next(r for r in result.collect() if r.InvoiceNo == "30"))["high_quantity"]
    assert high.reference_count == 30
    assert high.q1 == 8.25 and high.q3 == 22.75
    assert high.upper_bound == 66.25 and high.status == "flagged"
    reduced = build_output(spark.createDataFrame(rows[:-3], TRANSACTION_SCHEMA), "2011-12-10")
    assert checks(next(r for r in reduced.collect() if r.InvoiceNo == "29"))["high_quantity"].reason == "insufficient_product_samples"


def test_empty_and_partition_round_trip(spark, tmp_path):
    from scripts.pyspark_anomalies import validate_paths

    empty = spark.createDataFrame([], TRANSACTION_SCHEMA)
    result = build_output(empty, "2011-12-10")
    validate_output(empty, result, "2011-12-10")
    output_root = str(tmp_path / "results")
    validate_paths(str(tmp_path / "input"), output_root)
    first = write_results(result, output_root, "2011-12-10")
    assert spark.read.parquet(first).count() == 0
    source = spark.createDataFrame([row("X")], TRANSACTION_SCHEMA)
    previous = build_output(source, "2011-12-09")
    previous_path = write_results(previous, output_root, "2011-12-09")
    assert spark.read.parquet(previous_path).count() == 0
    replacement = build_output(source, "2011-12-10")
    write_results(replacement, output_root, "2011-12-10")
    assert spark.read.parquet(first).count() == 1
    assert spark.read.parquet(previous_path).count() == 0
    assert spark.read.parquet(output_root).count() == 1
