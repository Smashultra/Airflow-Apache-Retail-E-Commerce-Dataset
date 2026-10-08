"""Behavior checks for the retrospective anomaly audit job."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from scripts.pyspark_anomalies import (
    DATA_RULES,
    RULES,
    add_base_columns,
    add_data_checks,
    add_deviation_checks,
    add_operational_checks,
    add_invoice_summary,
    add_record_context,
    assess_orders,
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

    for extra in (["--run-date", "2011-02-30"], ["--z-threshold", "nan"],
                  ["--z-threshold", "0"], ["--min-samples", "1"]):
        arguments = ["--input", "a", "--output", "b", "--run-date", "2011-12-10"]
        if extra[0] == "--run-date":
            arguments[-1] = extra[1]
        else:
            arguments += extra
        with pytest.raises(SystemExit):
            parse_args(arguments)
    args = parse_args(["--input", "a", "--output", "b", "--run-date", "2011-12-10"])
    assert args.z_threshold == 3.5 and args.min_samples == 30
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
    assert checks(found["cancel"])["cancel_sign_conflict"].status == "flagged"
    assert checks(found["return"])["cancel_sign_conflict"].status == "flagged"
    assert checks(found["negative price"])["negative_price_outside_adjustment"].status == "flagged"
    assert checks(found["zero invoice 1"])["all_zero_price_invoice"].status == "flagged"
    assert checks(found["zero with positive"])["zero_price_sale_line"].status == "flagged"
    assert checks(found["null price"])["invalid_numeric_value"].status == "flagged"
    assert checks(found["first ID"])["invoice_multiple_customers"].status == "flagged"
    assert checks(found["one ID"])["invoice_multiple_customers"].status == "not_applied"
    assert "missing_customer_id" in found["missing ID"].data_quality_flags
    assert found["missing ID"].data_anomaly_flags == []
    assert "invalid_unit_price" in found["infinite price"].data_quality_flags
    assert found["infinite price"].line_value is None
    assert found["overflow value"].line_value is None
    assert "invalid_line_value" in found["overflow value"].data_quality_flags
    assert "invalid_unit_price" not in found["overflow value"].data_quality_flags
    assert checks(found["overflow value"])["invalid_numeric_value"].status == "flagged"
    assert checks(found["missing date"])["missing_required_field"].status == "flagged"
    assert checks(found["missing date"])["quantity_deviation"].reason == "missing_invoice_date"
    assert checks(found[None])["cancel_sign_conflict"].reason == "missing_invoice_no"
    assert found[None].data_anomaly_flags == [
        "missing_required_field", "missing_description", "invalid_numeric_value"]
    assert found[None].max_severity == "high"
    assert all(len(r.check_results) == len(RULES) == 14 for r in found.values())


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
        assert checks(record)["cancel_sign_conflict"].reason == "missing_invoice_no"
        assert checks(record)["invoice_multiple_customers"].reason == "missing_invoice_no"


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


def test_output_schema_rule_order_and_summary(spark):
    rows = [row(f"N{i}", "50E", "steady", 10, 2.0, "Q") for i in range(6)]
    rows += [row("N6", "50E", "spike", 100, 2.0, "Q"),
             row("DUP", "50E", "duplicate", 10, 2.0, "Q"), row("DUP", "50E", "duplicate", 10, 2.0, "Q"),
             row("N7", "50E", None, 10, 2.0, "Q")]
    source = spark.createDataFrame(rows, TRANSACTION_SCHEMA)
    result = build_output(source, "2011-12-10")
    validate_output(source, result, "2011-12-10")
    found = {r.Description: r for r in result.collect()}

    assert result.count() == len(rows)
    assert all([c.rule for c in r.check_results] == list(RULES) for r in found.values())
    spike = found["spike"]
    assert spike.business_anomaly_flags == ["quantity_deviation"]
    assert spike.anomaly_flags == spike.data_anomaly_flags + spike.business_anomaly_flags
    assert spike.max_severity == "medium" and spike.assessment_status == "flagged"
    assert (spike.record_type, spike.channel, spike.has_dotcom_postage) == ("sale", "identified", False)
    assert found[None].data_anomaly_flags == ["missing_description"] and found[None].max_severity == "low"
    assert found["steady"].assessment_status == "not_flagged" and found["steady"].max_severity is None
    assert found["duplicate"].z_threshold == 3.5


def test_empty_and_partition_round_trip(spark, tmp_path):
    from scripts.pyspark_anomalies import validate_paths

    empty = spark.createDataFrame([], TRANSACTION_SCHEMA)
    result = build_output(empty, "2011-12-10")
    validate_output(empty, result, "2011-12-10")
    assert {"record_type", "channel", "max_severity", "data_anomaly_flags"}.issubset(result.columns)
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


def classify(spark, rows):
    source = spark.createDataFrame(rows, TRANSACTION_SCHEMA)
    return add_record_context(add_invoice_summary(add_base_columns(source)))


def test_record_type_priority_and_channels(spark):
    rows = [
        row("A1", "B", "Adjust bad debt", 1, 11062.06, None),
        row("C10", "100", "returned", -2, 1.0, "7"),
        row("C11", "100", "returned retail", -1, 1.0, None),
        row("500", "POST", "POSTAGE", 1, 18.0, "7"),
        row("500", "100", "sold", 3, 2.0, "7"),
        row("500", "200", "free gift", 2, 0.0, "7"),
        row("501", "DOT", "DOTCOM POSTAGE", 1, 5.0, None),
        row("501", "100", "web sale", 1, 4.0, None),
        row("501", "300", "web free", 1, 0.0, None),
        row("502", "100", "lost", -5, 0.0, None),
        row("503", "100", "all zero", 1, 0.0, "8"),
        row("504", "100", "other sale", 2, 3.0, None),
        row(None, "100", "no invoice", 1, 1.0, "9"),
        row("505", "100", "null quantity", None, 1.0, "9"),
    ]
    found = {r.Description: (r.record_type, r.channel, r.has_dotcom_postage)
             for r in classify(spark, rows).collect()}
    assert found == {
        "Adjust bad debt": ("accounting_adjustment", "internal", False),
        "returned": ("cancellation", "identified", False),
        "returned retail": ("cancellation", "retail_other", False),
        "POSTAGE": ("fee_service", "identified", False),
        "sold": ("sale", "identified", False),
        "free gift": ("zero_price_line", "identified", False),
        "DOTCOM POSTAGE": ("fee_service", "retail_web", True),
        "web sale": ("sale", "retail_web", True),
        "web free": ("zero_price_line", "retail_web", True),
        "lost": ("inventory_adjustment", "internal", False),
        "all zero": ("zero_price_line", "identified", False),
        "other sale": ("sale", "retail_other", False),
        "no invoice": ("sale", "unknown", False),
        "null quantity": ("unclassified", "identified", False),
    }


def data_checks(spark, rows):
    frame = add_data_checks(classify(spark, rows))
    return {r.Description: {c.rule: c for c in r["__data_checks"]} for r in frame.collect()}


def test_data_rules_dimensions_and_severity(spark):
    rows = [
        row("900", "100", "clean", 2, 1.0, "5"),
        row("901", "100", "clean retail", 2, 1.0, None),
        row("902", "100", "no country", 1, 1.0, "5", country=None),
        row("903", "100", None, 1, 1.0, "5"),
        row("904", "100", "infinite", 1, float("inf"), "5"),
        row("C905", "100", "bad cancel", 1, 1.0, "5"),
        row("906", "100", "negative", 1, -2.0, "5"),
        row("A907", "B", "Adjust bad debt", 1, -11062.06, None),
        row("908", "100", "first id", 1, 1.0, "5"),
        row("908", "100", "second id", 1, 1.0, "6"),
        row("909", "100", "uk", 1, 1.0, "5", country="UK"),
        row("909", "100", "france", 1, 1.0, "5", country="France"),
        row("910", "100", "day one", 1, 1.0, "5", when=datetime(2011, 12, 8)),
        row("910", "100", "day two", 1, 1.0, "5", when=datetime(2011, 12, 9)),
    ]
    found = data_checks(spark, rows)

    def flagged(description):
        return {rule: c.severity for rule, c in found[description].items() if c.status == "flagged"}

    assert all(list(checks_) == list(DATA_RULES) for checks_ in found.values())
    assert all(c.tier == "data" for checks_ in found.values() for c in checks_.values())
    assert flagged("clean") == {}
    assert flagged("clean retail") == {}
    assert all(found["clean retail"][rule].status == "not_flagged" for rule in DATA_RULES[:5])
    assert flagged("no country") == {"missing_required_field": "high"}
    assert flagged(None) == {"missing_description": "low"}
    assert flagged("infinite") == {"invalid_numeric_value": "high"}
    assert found["infinite"]["negative_price_outside_adjustment"].reason == "invalid_numeric_value"
    assert flagged("bad cancel") == {"cancel_sign_conflict": "medium"}
    assert flagged("negative") == {"negative_price_outside_adjustment": "high"}
    assert flagged("Adjust bad debt") == {}
    assert flagged("first id") == {"invoice_multiple_customers": "medium"}
    assert flagged("uk") == {"invoice_inconsistent_header": "medium"}
    assert flagged("day two") == {"invoice_inconsistent_header": "medium"}
    assert found["clean"]["cancel_sign_conflict"].severity is None


def deviation_checks(spark, rows):
    frame = add_deviation_checks(classify(spark, rows), z_threshold=3.5, min_samples=30)
    return [(r.InvoiceNo, r.CustomerID, r.StockCode, r.Quantity, r.UnitPrice,
             {c.rule: c for c in r["__deviation_checks"]}) for r in frame.collect()]


def test_quantity_hierarchy_levels(spark):
    rows = [row(f"S{i}", "10X", "item", 10, 2.0, "A") for i in range(6)]
    rows += [row("S6", "10X", "item", 40, 2.0, "A"), row("S7", "10X", "item", 25, 2.0, "A")]
    rows += [row(f"B{i}", "10X", "item", 10, 2.0, "B") for i in range(2)]
    rows += [row(f"BY{i}", "10Y", "other", 5, 1.0, "B") for i in range(25)]
    rows += [row("K0", "10X", "item", 10, 2.0, "C")]
    rows += [row(f"D{i}", "10X", "item", 10, 2.0, "D") for i in range(25)]
    rows += [row(f"R{i}", "10X", "item", 1, 4.0, None) for i in range(30)]
    found = deviation_checks(spark, rows)
    quantity = {(inv, qty): c["quantity_deviation"] for inv, _, _, qty, _, c in found}

    spike = quantity[("S6", 40)]
    assert (spike.status, spike.context_level, spike.fold_change) == ("flagged", "customer_stock", 4.0)
    assert spike.robust_z is None and spike.reference_median == 10.0
    assert spike.value_at_risk == 60.0 and spike.severity == "low"
    assert quantity[("S7", 25)].status == "not_flagged"
    assert quantity[("B0", 10)].context_level == "customer"
    assert quantity[("K0", 10)].context_level == "channel_stock"
    assert quantity[("R0", 1)].context_level == "channel_stock"
    assert quantity[("R0", 1)].status == "not_flagged"


def test_price_deviation_zero_mad_and_direction(spark):
    rows = [row(f"P{i}", "10Z", "priced", 1, 1.0, "P") for i in range(5)]
    rows += [row("P5", "10Z", "priced", 1, 1.4, "P"), row("P6", "10Z", "priced", 1, 1.2, "P"),
             row("P7", "10Z", "priced", 1, 0.6, "P")]
    price = {inv: c["price_deviation"] for inv, _, _, _, _, c in deviation_checks(spark, rows)}

    assert price["P5"].status == "flagged" and price["P5"].robust_z is None
    assert price["P5"].fold_change == pytest.approx(1.4)
    assert price["P5"].context_level == "customer_stock"
    assert price["P6"].status == "not_flagged"
    assert price["P7"].status == "flagged" and price["P7"].fold_change == pytest.approx(0.6)
    assert price["P0"].status == "not_flagged"


def test_identified_never_uses_retail_baseline(spark):
    rows = [row(f"R{i}", "10W", "retail only", 1, 4.0, None) for i in range(30)]
    rows += [row("E0", "10W", "retail only", 50, 2.0, "E")]
    found = {inv: c for inv, _, _, _, _, c in deviation_checks(spark, rows)}

    assert found["E0"]["quantity_deviation"].reason == "insufficient_history"
    assert found["E0"]["price_deviation"].reason == "insufficient_history"
    assert found["R0"]["quantity_deviation"].context_level == "channel_stock"


def operational_checks(spark, rows):
    frame = add_operational_checks(classify(spark, rows))
    return {r.Description: {c.rule: c for c in r["__operational_checks"]} for r in frame.collect()}


def test_operational_rules_value_at_risk(spark):
    rows = [row(f"60{i}", "20A", "priced sale", 1, 2.0, "5") for i in range(3)]
    rows += [
        row("I1", "20A", "lost", -20, 0.0, None),
        row("I2", "99Z", "found", 5, 0.0, None),
        row("A9", "B", "Adjust bad debt", 1, -11062.06, None),
        row("700", "20A", "sold", 1, 2.0, "5"),
        row("700", "20A", "gift", 2, 0.0, "5"),
        row("701", "20A", "all zero", 3, 0.0, "6"),
    ]
    found = operational_checks(spark, rows)

    lost = found["lost"]["inventory_adjustment"]
    assert (lost.status, lost.value_at_risk, lost.severity) == ("flagged", 40.0, "low")
    found_row = found["found"]["inventory_adjustment"]
    assert (found_row.status, found_row.value_at_risk, found_row.severity) == ("flagged", None, "low")
    debt = found["Adjust bad debt"]["manual_accounting_entry"]
    assert (debt.status, debt.value_at_risk, debt.severity) == ("flagged", 11062.06, "high")
    gift = found["gift"]
    assert gift["zero_price_sale_line"].status == "flagged" and gift["zero_price_sale_line"].value_at_risk == 4.0
    assert gift["all_zero_price_invoice"].status == "not_flagged"
    zero = found["all zero"]
    assert zero["all_zero_price_invoice"].status == "flagged" and zero["all_zero_price_invoice"].value_at_risk == 6.0
    assert zero["zero_price_sale_line"].status == "not_flagged"
    assert found["priced sale"]["inventory_adjustment"].reason == "outside_scope"
    assert all(c.tier == "business" for checks_ in found.values() for c in checks_.values())


def test_unmatched_return_collective(spark):
    early = datetime(2011, 1, 1)
    rows = [
        row("790", "40C", "first day", 1, 1.0, "U9", when=early),
        row("800", "40C", "bought five", 5, 1.0, "U1"),
        row("C801", "40C", "return five", -5, 1.0, "U1"),
        row("C807", "40C", "return three", -3, 1.0, "U1"),
        row("802", "40C", "bought ten", 10, 1.0, "U2"),
        row("C802", "40C", "return part", -3, 1.0, "U2"),
        row("C803", "40C", "return only", -2, 1.0, "U3"),
        row("C804", "40C", "return no id", -2, 1.0, None),
        row("C805", "POST", "postage refund", -1, 5.0, "U1"),
        row("C806", "40C", "early return", -2, 1.0, "U3", when=datetime(2011, 1, 5)),
    ]
    found = {d: c["unmatched_return"] for d, c in operational_checks(spark, rows).items()}

    assert found["return five"].status == "flagged" and found["return three"].status == "flagged"
    assert found["return five"].observed_value == 8.0 and found["return five"].value_at_risk == 5.0
    assert found["return part"].status == "not_flagged"
    assert found["return only"].status == "flagged"
    assert found["return no id"].reason == "no_customer_history"
    assert found["postage refund"].reason == "service_or_special_code"
    assert found["early return"].reason == "history_window_start"
    assert found["bought five"].reason == "outside_scope"


def test_order_value_customer_then_channel_baseline(spark):
    rows = [row(f"K1{i}", "60G", "order", 50, 2.0, "K1") for i in range(6)]
    rows += [row("K16", "60G", "order", 225, 2.0, "K1")]
    rows += [row(f"F{i}", "60G", "order", 50, 2.0, f"F{i}") for i in range(25)]
    rows += [row("K20", "60G", "order", 60, 2.0, "K2")]
    rows += [row(f"W{i}", "60G", "web", 25, 2.0, None) for i in range(30)]
    rows += [row("W99", "DOT", "DOTCOM POSTAGE", 1, 10.0, None), row("W99", "60G", "web", 195, 2.0, None)]
    rows += [row("W98", "60G", "web", 50, 2.0, None)]
    rows += [row("BAD", "60G", "bad", 1, 20.0, "K3"), row("BAD", "60G", "bad", 1, -1.0, "K3"),
             row("C1", "60G", "cancel", -1, 90.0, "K3")]
    orders = {r.invoice_key: r for r in assess_orders(
        spark.createDataFrame(rows, TRANSACTION_SCHEMA), "2011-12-10").collect()}

    spike = orders["K16"]
    assert (spike.status, spike.context_level, spike.channel) == ("flagged", "customer", "identified")
    assert spike.order_total == 450.0 and spike.fold_change == 4.5
    assert spike.value_at_risk == 350.0 and spike.severity == "medium"
    assert orders["K20"].context_level == "channel" and orders["K20"].status == "not_flagged"
    web = orders["W99"]
    assert (web.channel, web.context_level, web.status) == ("retail_web", "channel", "flagged")
    assert web.reference_median == 50.0
    assert orders["W98"].status == "not_flagged" and orders["W98"].channel == "retail_other"
    assert orders["BAD"].reason == "invalid_invoice_lines" and orders["BAD"].order_total is None
    assert orders["BAD"].status == "not_assessable"
    assert orders["C1"].reason == "cancelled_invoice"
    assert orders["K10"].severity is None


def test_assess_orders_empty_and_sparse(spark):
    empty = assess_orders(spark.createDataFrame([], TRANSACTION_SCHEMA), "2011-12-10")
    assert empty.count() == 0 and "status" in empty.columns
    single = assess_orders(spark.createDataFrame([row("1", "60G")], TRANSACTION_SCHEMA), "2011-12-10")
    assert single.first().reason == "insufficient_history"
