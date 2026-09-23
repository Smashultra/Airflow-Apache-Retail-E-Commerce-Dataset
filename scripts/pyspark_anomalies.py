"""Retrospective, row-preserving anomaly checks for retail transactions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import date
import logging
import math
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

if __package__:
    from .pyspark_clean import RFM_EXCLUDED_DESCRIPTIONS, RFM_EXCLUDED_STOCK_CODES, TRANSACTION_SCHEMA
else:  # spark-submit executes the file from scripts/ without a package.
    from pyspark_clean import RFM_EXCLUDED_DESCRIPTIONS, RFM_EXCLUDED_STOCK_CODES, TRANSACTION_SCHEMA


logger = logging.getLogger("retail_anomalies")
REFERENCE_MODE = "retrospective_before_run_date"
BUSINESS_RULES = (
    "cancel_nonnegative_quantity", "negative_quantity_non_cancel",
    "negative_unit_price", "zero_price_in_priced_invoice",
    "all_zero_price_invoice", "multiple_customer_ids",
)
IQR_RULES = ("high_quantity", "high_unit_price", "high_line_value")
RULES = BUSINESS_RULES + IQR_RULES
# Match the corrected full-word expressions in EDA_Anomalies.ipynb.
KEYWORD_PATTERNS = (
    ("stock_words", r"\b(?:CHECK(?:ED|ING|S)?|ADJUST(?:ED|MENT|MENTS|ING)?|COUNTED|STOCK|HISTORIC|FOUND|TEST(?:ING|ED|S)?)\b"),
    ("damage_words", r"\b(?:DAMAG(?:E|ED|ES|ING)|DAGAM(?:E|ED)|FAULT(?:Y|S)?|MOULD(?:Y)?|WET|RUST(?:Y)?|SMASH(?:ED)?|CRUSH(?:ED)?|CRACK(?:ED)?|BROKEN|BREAKAGE|UNSALEABLE|THROWN)\b"),
    ("loss_words", r"\b(?:MISSING|LOST|MIA|NOT RCVD|CAN['’]?T FIND)\b"),
    ("coding_words", r"\b(?:WRONG(?:LY)?|INCORRECT(?:LY)?|BARCODE|MIXED UP|MARKED|MRKED|CODED)\b"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Anomaly-input Parquet directory")
    parser.add_argument("--output", required=True, help="Result root directory")
    parser.add_argument("--run-date", required=True, help="Exclusive YYYY-MM-DD cutoff")
    parser.add_argument("--iqr-multiplier", type=float, default=3.0)
    parser.add_argument("--min-samples", type=int, default=30)
    args = parser.parse_args(argv)
    try:
        if date.fromisoformat(args.run_date).isoformat() != args.run_date:
            raise ValueError
    except ValueError:
        parser.error("--run-date must be a valid YYYY-MM-DD date")
    if not math.isfinite(args.iqr_multiplier) or args.iqr_multiplier <= 0:
        parser.error("--iqr-multiplier must be finite and positive")
    if args.min_samples < 2:
        parser.error("--min-samples must be at least 2")
    return args


def validate_paths(input_path: str, output_root: str) -> None:
    """Reject local path overlap before any partition can be replaced."""
    for raw_path in (input_path, output_root):
        local_path = Path(raw_path)
        if ":" in raw_path and not (local_path.drive and local_path.is_absolute()):
            raise ValueError("Only local filesystem paths are supported for safe partition writes")
    source, target = Path(input_path).resolve(), Path(output_root).resolve()
    if source == target or source in target.parents or target in source.parents:
        raise ValueError("Input and output paths overlap")


def validate_input_contract(transactions: DataFrame) -> None:
    missing = set(TRANSACTION_SCHEMA.fieldNames()) - set(transactions.columns)
    if missing:
        raise ValueError(f"Missing transaction columns: {sorted(missing)}")
    for field in TRANSACTION_SCHEMA:
        actual = transactions.schema[field.name].dataType
        if actual != field.dataType:
            raise ValueError(
                f"{field.name} has type {actual.simpleString()}; expected {field.dataType.simpleString()}"
            )


def select_history(transactions: DataFrame, run_date: str) -> DataFrame:
    cutoff = F.lit(run_date).cast("timestamp")
    return transactions.filter(
        F.col("InvoiceDate").isNull() | (F.col("InvoiceDate") < cutoff)
    )


def _finite(value: Column) -> Column:
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))


def _normalized(value: Column, collapse_spaces: bool = False) -> Column:
    normalized = F.regexp_replace(F.upper(value), r"(?U)^\s+|\s+$", "")
    if collapse_spaces:
        normalized = F.regexp_replace(normalized, r"(?U)\s+", " ")
    return F.when(normalized != "", normalized)


def _flags(items: Sequence[tuple[str, Column]]) -> Column:
    return F.filter(
        F.array(*[F.when(condition, F.lit(name)) for name, condition in items]),
        lambda value: value.isNotNull(),
    )


def _check(rule: str, reason: Column, flagged: Column,
           observed: Column | None = None, reference_count: Column | None = None,
           q1: Column | None = None, q3: Column | None = None,
           upper_bound: Column | None = None) -> Column:
    null_double, null_long = F.lit(None).cast("double"), F.lit(None).cast("long")
    observed = observed.cast("double") if observed is not None else null_double
    observed = F.when(_finite(observed), observed).otherwise(null_double)
    return F.struct(
        F.lit(rule).alias("rule"),
        F.when(reason.isNotNull(), "not_applied")
        .when(flagged, "flagged").otherwise("not_flagged").alias("status"),
        reason.cast("string").alias("reason"),
        observed.alias("observed_value"),
        (reference_count.cast("long") if reference_count is not None else null_long).alias("reference_count"),
        (q1.cast("double") if q1 is not None else null_double).alias("q1"),
        (q3.cast("double") if q3 is not None else null_double).alias("q3"),
        (upper_bound.cast("double") if upper_bound is not None else null_double).alias("upper_bound"),
    )


def add_base_columns(history: DataFrame) -> DataFrame:
    result = history.select(*TRANSACTION_SCHEMA.fieldNames())
    for original, internal, collapse in (
        ("InvoiceNo", "__invoice", False), ("StockCode", "__stock", False),
        ("CustomerID", "__customer", False), ("Description", "__description", True),
    ):
        result = result.withColumn(internal, _normalized(F.col(original), collapse))
    result = result.withColumn("__valid_price", _finite(F.col("UnitPrice")))
    raw_line = F.col("Quantity").cast("double") * F.col("UnitPrice")
    result = result.withColumn(
        "line_value",
        F.when(F.col("Quantity").isNotNull() & F.col("__valid_price") & _finite(raw_line), raw_line),
    )
    result = (
        result.withColumn("__cancel", F.coalesce(F.col("__invoice").startswith("C"), F.lit(False)))
        .withColumn("__service", F.coalesce(
            F.col("__stock").isin(*RFM_EXCLUDED_STOCK_CODES)
            | F.col("__description").isin(*RFM_EXCLUDED_DESCRIPTIONS), F.lit(False)))
        .withColumn("__special", F.coalesce(F.col("__stock").rlike(r"^[A-Z ]+$"), F.lit(False)))
    )
    result = result.withColumn(
        "__sale_shape",
        F.col("__invoice").isNotNull() & ~F.col("__cancel")
        & (F.col("Quantity") > 0) & (F.col("UnitPrice") > 0)
        & F.col("line_value").isNotNull() & (F.col("line_value") > 0),
    )
    return result.withColumn("data_quality_flags", _flags((
        ("missing_customer_id", F.col("__customer").isNull()),
        ("missing_invoice_no", F.col("__invoice").isNull()),
        ("missing_stock_code", F.col("__stock").isNull()),
        ("missing_description", F.col("__description").isNull()),
        ("missing_country", _normalized(F.col("Country")).isNull()),
        ("missing_invoice_date", F.col("InvoiceDate").isNull()),
        ("invalid_quantity", F.col("Quantity").isNull()),
        ("invalid_unit_price", ~F.col("__valid_price")),
        ("invalid_line_value", F.col("line_value").isNull()),
    )))


def add_description_context(base: DataFrame) -> DataFrame:
    """Use same-stock names seen on two distinct positive sales invoices."""
    names = (
        base.filter(F.col("InvoiceDate").isNotNull() & F.col("__stock").isNotNull()
                    & F.col("__description").isNotNull() & F.col("__sale_shape")
                    & ~F.col("__service") & ~F.col("__special"))
        .groupBy("__stock", "__description")
        .agg(F.countDistinct("__invoice").alias("__invoice_count"))
        .filter(F.col("__invoice_count") >= 2)
        .groupBy("__stock")
        .agg(F.collect_set("__description").alias("__reference_names"))
    )
    result = base.join(names, "__stock", "left")
    matches = [(name, F.coalesce(F.col("__description").rlike(pattern), F.lit(False)))
               for name, pattern in KEYWORD_PATTERNS]
    candidate = matches[0][1]
    for _, match in matches[1:]:
        candidate = candidate | match
    known = F.coalesce(F.array_contains(F.col("__reference_names"), F.col("__description")), F.lit(False))
    has_reference = F.col("__reference_names").isNotNull()
    return result.withColumn("context_flags", _flags((
        ("cancelled_invoice", F.col("__cancel")),
        ("service_line", F.col("__service")),
        ("special_stock_code", F.col("__special")),
        ("zero_negative", (F.col("UnitPrice") == 0) & (F.col("Quantity") < 0)),
        ("zero_positive", (F.col("UnitPrice") == 0) & (F.col("Quantity") > 0)),
        ("sale_shape", F.col("__sale_shape")),
        ("keyword_product_name", candidate & known),
        ("keyword_unresolved", candidate & ~has_reference),
        *[(name, match & has_reference & ~known) for name, match in matches],
    )))


def add_invoice_checks(context: DataFrame) -> DataFrame:
    valid_price = F.col("__valid_price")
    invoice_summary = (
        context.filter(F.col("__invoice").isNotNull()).groupBy("__invoice")
        .agg(
            F.count("*").alias("__invoice_lines"),
            F.sum(F.when(valid_price, 1).otherwise(0)).alias("__priced_lines"),
            F.sum(F.when(valid_price & (F.col("UnitPrice") > 0), 1).otherwise(0)).alias("__positive_price_lines"),
            F.sum(F.when(valid_price & (F.col("UnitPrice") == 0), 1).otherwise(0)).alias("__zero_price_lines"),
            F.countDistinct("__customer").alias("__customer_count"),
            F.count("__customer").alias("__identified_lines"),
        )
    )
    result = context.join(invoice_summary, "__invoice", "left")
    missing_invoice = F.col("__invoice").isNull()
    valid_quantity = F.col("Quantity").isNotNull()
    all_prices = F.col("__priced_lines") == F.col("__invoice_lines")
    any_positive = F.col("__positive_price_lines") > 0
    multiple_ids = F.col("__customer_count") >= 2
    all_ids = F.col("__identified_lines") == F.col("__invoice_lines")
    checks = (
        _check(BUSINESS_RULES[0],
               F.when(missing_invoice, "missing_invoice_no").when(~valid_quantity, "invalid_numeric_value"),
               F.col("__cancel") & (F.col("Quantity") >= 0), F.col("Quantity")),
        _check(BUSINESS_RULES[1],
               F.when(missing_invoice, "missing_invoice_no").when(~valid_quantity, "invalid_numeric_value"),
               ~F.col("__cancel") & (F.col("Quantity") < 0), F.col("Quantity")),
        _check(BUSINESS_RULES[2],
               F.when(~valid_price, "invalid_numeric_value"), F.col("UnitPrice") < 0, F.col("UnitPrice")),
        _check(BUSINESS_RULES[3],
               F.when(missing_invoice, "missing_invoice_no")
               .when(~valid_price, "invalid_numeric_value")
               .when(~any_positive & ~all_prices, "incomplete_invoice_prices"),
               (F.col("UnitPrice") == 0) & any_positive,
               F.col("UnitPrice"), F.col("__invoice_lines")),
        _check(BUSINESS_RULES[4],
               F.when(missing_invoice, "missing_invoice_no")
               .when(~all_prices, "incomplete_invoice_prices"),
               F.col("__zero_price_lines") == F.col("__invoice_lines"),
               F.col("UnitPrice"), F.col("__invoice_lines")),
        _check(BUSINESS_RULES[5],
               F.when(missing_invoice, "missing_invoice_no")
               .when(~multiple_ids & ~all_ids, "incomplete_customer_ids"),
               multiple_ids, F.col("__customer_count"), F.col("__invoice_lines")),
    )
    return result.withColumn("__business_checks", F.array(*checks))


def add_product_iqr_checks(invoice_rows: DataFrame, multiplier: float, min_samples: int) -> DataFrame:
    eligible = (
        F.col("InvoiceDate").isNotNull() & F.col("__invoice").isNotNull()
        & F.col("__stock").isNotNull() & ~F.col("__cancel")
        & (F.col("Quantity") > 0) & F.col("__valid_price")
        & (F.col("UnitPrice") > 0) & (F.col("line_value") > 0)
        & ~F.col("__service") & ~F.col("__special")
    )
    product_stats = (
        invoice_rows.filter(eligible).groupBy("__stock")
        .agg(
            F.count("*").alias("__reference_count"),
            F.expr("percentile(Quantity, array(0.25, 0.75))").alias("__quantity_quartiles"),
            F.expr("percentile(UnitPrice, array(0.25, 0.75))").alias("__price_quartiles"),
            F.expr("percentile(line_value, array(0.25, 0.75))").alias("__value_quartiles"),
        )
    )
    result = invoice_rows.join(product_stats, "__stock", "left")
    numeric_valid = F.col("Quantity").isNotNull() & F.col("__valid_price") & F.col("line_value").isNotNull()
    base_reason = (
        F.when(F.col("InvoiceDate").isNull(), "missing_invoice_date")
        .when(F.col("__invoice").isNull(), "missing_invoice_no")
        .when(F.col("__stock").isNull(), "missing_stock_code")
        .when(~numeric_valid, "invalid_numeric_value")
        .when(F.col("__cancel") | (F.col("Quantity") <= 0)
              | (F.col("UnitPrice") <= 0) | (F.col("line_value") <= 0), "outside_sales_scope")
        .when(F.col("__service") | F.col("__special"), "service_or_special_code")
        .when(F.coalesce(F.col("__reference_count"), F.lit(0)) < min_samples,
              "insufficient_product_samples")
    )
    checks = []
    for rule, observed_name, quartiles_name in zip(
        IQR_RULES, ("Quantity", "UnitPrice", "line_value"),
        ("__quantity_quartiles", "__price_quartiles", "__value_quartiles"),
    ):
        observed = F.col(observed_name).cast("double")
        q1, q3 = F.col(quartiles_name)[0], F.col(quartiles_name)[1]
        spread = q3 - q1
        upper = q3 + F.lit(multiplier) * spread
        reason = base_reason.when(spread <= 0, "zero_iqr").when(~_finite(upper), "nonfinite_threshold")
        checks.append(_check(rule, reason, observed > upper, observed,
                             F.col("__reference_count"), q1, q3, upper))
    return result.withColumn("__iqr_checks", F.array(*checks))


def build_output(transactions: DataFrame, run_date: str,
                 iqr_multiplier: float = 3.0, min_samples: int = 30) -> DataFrame:
    """Evaluate history with its own product reference population."""
    validate_input_contract(transactions)
    history = select_history(transactions, run_date)
    base = add_base_columns(history)
    context = add_description_context(base)
    invoice_rows = add_invoice_checks(context)
    scored = add_product_iqr_checks(invoice_rows, iqr_multiplier, min_samples)
    scored = scored.withColumn("check_results", F.concat(F.col("__business_checks"), F.col("__iqr_checks")))
    scored = scored.withColumn(
        "anomaly_flags",
        F.transform(F.filter("check_results", lambda check: check.status == "flagged"),
                    lambda check: check.rule),
    )
    scored = scored.withColumn(
        "assessment_status",
        F.when(F.size("anomaly_flags") > 0, "flagged")
        .when(F.exists("check_results", lambda check: check.status == "not_flagged"), "not_flagged")
        .otherwise("not_assessable"),
    )
    return scored.select(
        *TRANSACTION_SCHEMA.fieldNames(),
        F.lit(run_date).cast("date").alias("run_date"),
        F.lit(REFERENCE_MODE).alias("reference_mode"),
        F.lit(iqr_multiplier).cast("double").alias("iqr_multiplier"),
        F.lit(min_samples).cast("int").alias("min_samples"),
        "line_value", "data_quality_flags", "context_flags",
        "check_results", "anomaly_flags", "assessment_status",
    )


def validate_output(transactions: DataFrame, result: DataFrame, run_date: str) -> None:
    """Reject lost/duplicated source rows and malformed checks before writing."""
    history = select_history(transactions, run_date).select(*TRANSACTION_SCHEMA.fieldNames())
    original = result.select(*TRANSACTION_SCHEMA.fieldNames())
    if history.count() != result.count() or not history.exceptAll(original).isEmpty() \
            or not original.exceptAll(history).isEmpty():
        raise ValueError("Output does not preserve the input row multiset before cutoff")
    expected_rules = F.array(*[F.lit(rule) for rule in RULES])
    invalid = (
        F.col("run_date").isNull() | (F.col("run_date") != F.lit(run_date).cast("date"))
        | (F.transform("check_results", lambda check: check.rule) != expected_rules)
        | F.exists("check_results", lambda check: ~check.status.isin("flagged", "not_flagged", "not_applied"))
        | ~F.col("assessment_status").isin("flagged", "not_flagged", "not_assessable")
    )
    if not result.filter(invalid).isEmpty():
        raise ValueError("Output contains invalid run date, status, or check order")


def write_results(result: DataFrame, output_root: str, run_date: str) -> str:
    """Replace only the requested local date partition."""
    output_path = str(Path(output_root).resolve() / f"run_date={run_date}")
    result.write.mode("overwrite").parquet(output_path)
    return output_path


def _log_results(result: DataFrame) -> None:
    for record in result.groupBy("assessment_status").count().collect():
        logger.info("assessment_status=%s rows=%s", record.assessment_status, record["count"])
    invoice = _normalized(F.col("InvoiceNo"))
    exploded = result.select(invoice.alias("invoice"), F.explode("check_results").alias("check"))
    for record in (
        exploded.filter(F.col("check.status") == "flagged")
        .groupBy("check.rule").agg(F.count("*").alias("rows"), F.countDistinct("invoice").alias("invoices"))
        .collect()
    ):
        logger.info("flagged rule=%s rows=%s invoices=%s", record["rule"], record.rows, record.invoices)
    for record in (
        exploded.filter(F.col("check.status") == "not_applied")
        .groupBy("check.rule", "check.reason").count().collect()
    ):
        logger.info("not_applied rule=%s reason=%s rows=%s",
                    record["rule"], record["reason"], record["count"])


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    validate_paths(args.input, args.output)
    spark = (SparkSession.builder.appName("retail-pyspark-anomalies")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true").getOrCreate())
    result = None
    try:
        transactions = spark.read.parquet(args.input)
        validate_input_contract(transactions)
        history = select_history(transactions, args.run_date)
        source_count, history_count = transactions.count(), history.count()
        logger.info("input_rows=%s outside_cutoff_rows=%s missing_date_rows=%s",
                    source_count, source_count - history_count,
                    history.filter(F.col("InvoiceDate").isNull()).count())
        result = build_output(transactions, args.run_date, args.iqr_multiplier,
                              args.min_samples).cache()
        result.count()
        validate_output(transactions, result, args.run_date)
        _log_results(result)
        output_path = write_results(result, args.output, args.run_date)
        logger.info("anomaly_rows=%s output=%s", history_count, output_path)
    finally:
        try:
            if result is not None:
                result.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    main()
