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
# Tier 1 data rules, each tied to an ISO/IEC 25012 / DAMA quality dimension.
DATA_RULES = (
    "missing_required_field", "missing_description", "invalid_numeric_value",
    "cancel_sign_conflict", "negative_price_outside_adjustment",
    "invoice_multiple_customers", "invoice_inconsistent_header",
)
# Robust contextual thresholds: Iglewicz-Hoaglin modified z-score on log values,
# combined with materiality limits fixed before looking at results (ISA 520).
Z_THRESHOLD = 3.5
MAD_CONSTANT = 0.6745
QUANTITY_MIN_FOLD = 3.0
PRICE_MIN_RELATIVE = 0.30
ORDER_MIN_FOLD = 3.0
MIN_CUSTOMER_STOCK_LINES = 5
MIN_CUSTOMER_LINES = 20
MIN_CUSTOMER_INVOICES = 5
# Tier 2 business rules (Chandola et al. 2009: point, contextual, collective).
BUSINESS_RULES = (
    "quantity_deviation", "price_deviation", "zero_price_sale_line",
    "all_zero_price_invoice", "inventory_adjustment", "manual_accounting_entry",
    "unmatched_return",
)
RETURN_WINDOW_DAYS = 30
SEVERITY_HIGH_GBP = 1000.0
SEVERITY_MEDIUM_GBP = 100.0
RULES = DATA_RULES + BUSINESS_RULES
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
    parser.add_argument("--z-threshold", type=float, default=Z_THRESHOLD,
                        help="Modified z-score limit for contextual checks")
    parser.add_argument("--min-samples", type=int, default=30)
    args = parser.parse_args(argv)
    try:
        if date.fromisoformat(args.run_date).isoformat() != args.run_date:
            raise ValueError
    except ValueError:
        parser.error("--run-date must be a valid YYYY-MM-DD date")
    if not math.isfinite(args.z_threshold) or args.z_threshold <= 0:
        parser.error("--z-threshold must be finite and positive")
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


def _severity_from_value(value_at_risk: Column) -> Column:
    return (F.when(value_at_risk >= SEVERITY_HIGH_GBP, "high")
            .when(value_at_risk >= SEVERITY_MEDIUM_GBP, "medium").otherwise("low"))


def _check(tier: str, rule: str, reason: Column, flagged: Column, *,
           observed: Column | None = None, context_level: Column | None = None,
           reference_count: Column | None = None, reference_median: Column | None = None,
           reference_mad: Column | None = None, robust_z: Column | None = None,
           fold_change: Column | None = None, value_at_risk: Column | None = None,
           severity: Column | None = None) -> Column:
    """One rule outcome; severity comes from value_at_risk unless given."""
    def number(value: Column | None) -> Column:
        value = value.cast("double") if value is not None else F.lit(None).cast("double")
        return F.when(_finite(value), value)

    status = (F.when(reason.isNotNull(), "not_applied")
              .when(F.coalesce(flagged, F.lit(False)), "flagged").otherwise("not_flagged"))
    value = number(value_at_risk)
    return F.struct(
        F.lit(tier).alias("tier"),
        F.lit(rule).alias("rule"),
        status.alias("status"),
        reason.cast("string").alias("reason"),
        (context_level if context_level is not None else F.lit(None)).cast("string").alias("context_level"),
        number(observed).alias("observed_value"),
        (reference_count.cast("long") if reference_count is not None
         else F.lit(None).cast("long")).alias("reference_count"),
        number(reference_median).alias("reference_median"),
        number(reference_mad).alias("reference_mad"),
        number(robust_z).alias("robust_z"),
        number(fold_change).alias("fold_change"),
        value.alias("value_at_risk"),
        F.when(status == "flagged", severity if severity is not None else _severity_from_value(value))
        .cast("string").alias("severity"),
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


def add_invoice_summary(frame: DataFrame) -> DataFrame:
    """Attach per-invoice counts used by record typing, channels and checks."""
    valid_price = F.col("__valid_price")
    result = frame.withColumn("__accounting", F.coalesce(
        F.col("__invoice").startswith("A") | (F.col("__stock") == "B")
        | (F.col("__description") == "ADJUST BAD DEBT"), F.lit(False)))
    summary = (
        result.filter(F.col("__invoice").isNotNull()).groupBy("__invoice")
        .agg(
            F.count("*").alias("__invoice_lines"),
            F.sum(F.when(valid_price, 1).otherwise(0)).alias("__priced_lines"),
            F.sum(F.when(valid_price & (F.col("UnitPrice") > 0), 1).otherwise(0)).alias("__positive_price_lines"),
            F.sum(F.when(valid_price & (F.col("UnitPrice") == 0), 1).otherwise(0)).alias("__zero_price_lines"),
            F.countDistinct("__customer").alias("__customer_count"),
            F.count("__customer").alias("__identified_lines"),
            F.countDistinct(_normalized(F.col("Country"))).alias("__country_count"),
            F.countDistinct(F.to_date("InvoiceDate")).alias("__date_count"),
            F.max(F.coalesce(F.col("__stock") == "DOT", F.lit(False))).alias("has_dotcom_postage"),
            F.max(F.coalesce(valid_price & (F.col("UnitPrice") > 0) & ~F.col("__accounting"),
                             F.lit(False))).alias("__retail_priced"),
        )
    )
    return (result.join(summary, "__invoice", "left")
            .withColumn("has_dotcom_postage", F.coalesce("has_dotcom_postage", F.lit(False))))


def add_record_context(frame: DataFrame) -> DataFrame:
    """Classify each row (record_type) and its invoice (channel)."""
    price, quantity = F.col("UnitPrice"), F.col("Quantity")
    record_type = (
        F.when(F.col("__accounting"), "accounting_adjustment")
        .when(F.col("__cancel"), "cancellation")
        .when(F.col("__service") | F.col("__special"), "fee_service")
        .when((price == 0) & F.col("__customer").isNull()
              & (F.coalesce(F.col("__positive_price_lines"), F.lit(0)) == 0), "inventory_adjustment")
        .when((quantity > 0) & (price > 0) & (F.col("line_value") > 0), "sale")
        .when((price == 0) & (quantity > 0), "zero_price_line")
        .otherwise("unclassified")
    )
    channel = (
        F.when(F.col("__invoice").isNull(), "unknown")
        .when(F.col("__identified_lines") > 0, "identified")
        .when(F.col("has_dotcom_postage"), "retail_web")
        .when(F.col("__retail_priced"), "retail_other")
        .otherwise("internal")
    )
    return (
        frame.withColumn("record_type", record_type).withColumn("channel", channel)
        .withColumn("__baseline_group",
                    F.when(F.col("channel") == "identified", "identified")
                    .when(F.col("channel").isin("retail_web", "retail_other"), "retail"))
    )


def add_data_checks(frame: DataFrame) -> DataFrame:
    """Tier 1: completeness, validity and consistency of the recorded data."""
    no_reason = F.lit(None).cast("string")
    missing_invoice = F.col("__invoice").isNull()
    multiple_ids = F.col("__customer_count") >= 2
    all_ids = F.col("__identified_lines") == F.col("__invoice_lines")
    high, medium, low = F.lit("high"), F.lit("medium"), F.lit("low")
    checks = (
        _check("data", DATA_RULES[0], no_reason,
               missing_invoice | F.col("__stock").isNull() | F.col("InvoiceDate").isNull()
               | F.col("Quantity").isNull() | F.col("UnitPrice").isNull()
               | _normalized(F.col("Country")).isNull(), severity=high),
        _check("data", DATA_RULES[1], no_reason, F.col("__description").isNull(), severity=low),
        _check("data", DATA_RULES[2], no_reason,
               F.col("Quantity").isNull() | ~F.col("__valid_price") | F.col("line_value").isNull(),
               severity=high),
        _check("data", DATA_RULES[3],
               F.when(missing_invoice, "missing_invoice_no")
               .when(F.col("Quantity").isNull(), "invalid_numeric_value"),
               (F.col("__cancel") & (F.col("Quantity") >= 0))
               | (~F.col("__cancel") & (F.col("Quantity") < 0)
                  & ~F.col("record_type").isin("inventory_adjustment", "accounting_adjustment")),
               observed=F.col("Quantity"), severity=medium),
        _check("data", DATA_RULES[4], F.when(~F.col("__valid_price"), "invalid_numeric_value"),
               (F.col("UnitPrice") < 0) & (F.col("record_type") != "accounting_adjustment"),
               observed=F.col("UnitPrice"), severity=high),
        _check("data", DATA_RULES[5],
               F.when(missing_invoice, "missing_invoice_no")
               .when(~multiple_ids & ~all_ids, "incomplete_customer_ids"),
               multiple_ids, observed=F.col("__customer_count"),
               reference_count=F.col("__invoice_lines"), severity=medium),
        _check("data", DATA_RULES[6], F.when(missing_invoice, "missing_invoice_no"),
               (F.col("__country_count") > 1) | (F.col("__date_count") > 1),
               reference_count=F.col("__invoice_lines"), severity=medium),
    )
    return frame.withColumn("__data_checks", F.array(*checks))


def robust_baseline(frame: DataFrame, keys: list[str], log_value: Column, prefix: str) -> DataFrame:
    """Exact median and median absolute deviation of log_value per key group."""
    base = frame.select(*keys, log_value.alias("__value")).filter(
        F.col("__value").isNotNull() & ~F.isnan("__value"))
    for key in keys:
        base = base.filter(F.col(key).isNotNull())
    centre = base.groupBy(*keys).agg(
        F.count("*").alias(f"{prefix}_n"),
        F.percentile("__value", F.lit(0.5)).alias(f"{prefix}_med"),
    )
    spread = base.join(centre, keys).groupBy(*keys).agg(
        F.percentile(F.abs(F.col("__value") - F.col(f"{prefix}_med")), F.lit(0.5)).alias(f"{prefix}_mad"))
    return centre.join(spread, keys)


def _first(choices: Sequence[tuple[Column, Column]]) -> Column:
    result = None
    for condition, value in choices:
        result = F.when(condition, value) if result is None else result.when(condition, value)
    return result


def _deviation(x: Column, median: Column, mad: Column, z_threshold: float,
               two_sided: bool) -> tuple[Column, Column, Column]:
    """Return robust z (null when MAD = 0), statistical exceedance and fold change."""
    z = F.when(mad > 0, F.lit(MAD_CONSTANT) * (x - median) / mad)
    if two_sided:
        exceeds = F.when(mad > 0, F.abs(z) > z_threshold).otherwise(x != median)
    else:
        exceeds = F.when(mad > 0, z > z_threshold).otherwise(x > median)
    return z, exceeds, F.round(F.exp(x - median), 6)


def add_deviation_checks(frame: DataFrame, z_threshold: float, min_samples: int) -> DataFrame:
    """Contextual quantity/price checks with customer -> channel fallbacks."""
    log_quantity, log_price = F.log(F.col("Quantity").cast("double")), F.log(F.col("UnitPrice"))
    sales = frame.filter(
        (F.col("record_type") == "sale") & F.col("InvoiceDate").isNotNull()
        & F.col("__baseline_group").isNotNull() & F.col("__stock").isNotNull())
    identified = sales.filter((F.col("__baseline_group") == "identified") & F.col("__customer").isNotNull())
    pair, channel = ["__customer", "__stock"], ["__baseline_group", "__stock"]
    quantity_channel = robust_baseline(sales, channel, log_quantity, "__q3")
    stock_median = quantity_channel.filter(F.col("__baseline_group") == "identified").select(
        "__stock", F.col("__q3_med").alias("__stock_qmed"))
    quantity_customer = robust_baseline(
        identified.join(stock_median, "__stock"), ["__customer"],
        log_quantity - F.col("__stock_qmed"), "__q2")
    result = (
        frame.join(robust_baseline(identified, pair, log_quantity, "__q1"), pair, "left")
        .join(quantity_channel, channel, "left")
        .join(stock_median, "__stock", "left")
        .join(quantity_customer, "__customer", "left")
        .join(robust_baseline(identified, pair, log_price, "__p1"), pair, "left")
        .join(robust_baseline(sales, channel, log_price, "__p3"), channel, "left")
    )
    identified_row = F.col("__baseline_group") == "identified"
    base_reason = (
        F.when(F.col("record_type") != "sale", "outside_scope")
        .when(F.col("InvoiceDate").isNull(), "missing_invoice_date")
        .when(F.col("__baseline_group").isNull(), "missing_invoice_no")
    )

    def levels(options: Sequence[tuple[Column, str, Column, str, Column]]):
        pick = lambda part: _first([(use, part(option)) for option in options for use in [option[0]]])
        level = pick(lambda o: F.lit(o[1]))
        x = pick(lambda o: o[2])
        median = pick(lambda o: F.col(f"{o[3]}_med"))
        mad = pick(lambda o: F.col(f"{o[3]}_mad"))
        count = pick(lambda o: F.col(f"{o[3]}_n"))
        offset = pick(lambda o: o[4])
        return level, x, median, mad, count, offset

    zero = F.lit(0.0)
    q_level, q_x, q_med, q_mad, q_n, q_offset = levels((
        (identified_row & (F.col("__q1_n") >= MIN_CUSTOMER_STOCK_LINES), "customer_stock", log_quantity, "__q1", zero),
        (identified_row & (F.col("__q2_n") >= MIN_CUSTOMER_LINES) & F.col("__stock_qmed").isNotNull(),
         "customer", log_quantity - F.col("__stock_qmed"), "__q2", F.col("__stock_qmed")),
        (F.col("__q3_n") >= min_samples, "channel_stock", log_quantity, "__q3", zero),
    ))
    q_z, q_exceeds, q_fold = _deviation(q_x, q_med, q_mad, z_threshold, two_sided=False)
    q_expected = F.round(F.exp(q_med + q_offset), 6)
    quantity_check = _check(
        "business", "quantity_deviation",
        base_reason.when(q_level.isNull(), "insufficient_history"),
        q_exceeds & (q_fold >= QUANTITY_MIN_FOLD),
        observed=F.col("Quantity"), context_level=q_level, reference_count=q_n,
        reference_median=q_expected, reference_mad=q_mad, robust_z=q_z, fold_change=q_fold,
        value_at_risk=F.round((F.col("Quantity") - q_expected) * F.col("UnitPrice"), 2),
    )
    p_level, p_x, p_med, p_mad, p_n, _ = levels((
        (identified_row & (F.col("__p1_n") >= MIN_CUSTOMER_STOCK_LINES), "customer_stock", log_price, "__p1", zero),
        (F.col("__p3_n") >= min_samples, "channel_stock", log_price, "__p3", zero),
    ))
    p_z, p_exceeds, p_fold = _deviation(p_x, p_med, p_mad, z_threshold, two_sided=True)
    p_expected = F.round(F.exp(p_med), 6)
    price_check = _check(
        "business", "price_deviation",
        base_reason.when(p_level.isNull(), "insufficient_history"),
        p_exceeds & (F.abs(p_fold - 1) >= PRICE_MIN_RELATIVE),
        observed=F.col("UnitPrice"), context_level=p_level, reference_count=p_n,
        reference_median=p_expected, reference_mad=p_mad, robust_z=p_z, fold_change=p_fold,
        value_at_risk=F.round(F.abs(F.col("UnitPrice") - p_expected) * F.col("Quantity"), 2),
    )
    return result.withColumn("__deviation_checks", F.array(quantity_check, price_check))


def add_operational_checks(frame: DataFrame) -> DataFrame:
    """Free goods, stock counts, accounting entries and returns beyond purchases."""
    dated_sales = frame.filter((F.col("record_type") == "sale") & F.col("InvoiceDate").isNotNull())
    stock_price = dated_sales.groupBy("__stock").agg(
        F.percentile("UnitPrice", F.lit(0.5)).alias("__stock_price"))
    pair = ["__customer", "__stock"]
    purchased = dated_sales.filter(F.col("__customer").isNotNull()).groupBy(*pair).agg(
        F.sum("Quantity").alias("__purchased"))
    returned = frame.filter((F.col("record_type") == "cancellation") & F.col("__customer").isNotNull()).groupBy(
        *pair).agg(F.sum(F.abs("Quantity")).alias("__returned"))
    history_start = frame.agg(F.min("InvoiceDate")).first()[0]
    result = (frame.join(stock_price, "__stock", "left").join(purchased, pair, "left")
              .join(returned, pair, "left"))
    record_type = F.col("record_type")
    estimated = lambda quantity: F.round(F.abs(quantity) * F.col("__stock_price"), 2)

    def scope(kind: str) -> Column:
        return F.when(record_type != kind, "outside_scope")

    zero_reason = scope("zero_price_line").when(F.col("__invoice").isNull(), "missing_invoice_no")
    window_start = (F.lit(history_start).cast("timestamp") + F.expr(f"INTERVAL {RETURN_WINDOW_DAYS} DAYS")
                    if history_start is not None else F.lit(None).cast("timestamp"))
    checks = (
        _check("business", "zero_price_sale_line", zero_reason, F.col("__positive_price_lines") > 0,
               observed=F.col("Quantity"), value_at_risk=estimated(F.col("Quantity"))),
        _check("business", "all_zero_price_invoice", zero_reason, F.col("__positive_price_lines") == 0,
               observed=F.col("Quantity"), reference_count=F.col("__invoice_lines"),
               value_at_risk=estimated(F.col("Quantity"))),
        _check("business", "inventory_adjustment", scope("inventory_adjustment"), F.lit(True),
               observed=F.col("Quantity"), value_at_risk=estimated(F.col("Quantity"))),
        _check("business", "manual_accounting_entry", scope("accounting_adjustment"), F.lit(True),
               observed=F.col("line_value"), value_at_risk=F.round(F.abs("line_value"), 2),
               severity=F.lit("high")),
        _check("business", "unmatched_return",
               scope("cancellation")
               .when(F.col("__customer").isNull(), "no_customer_history")
               .when(F.col("__service") | F.col("__special"), "service_or_special_code")
               .when(F.col("InvoiceDate").isNull(), "missing_invoice_date")
               .when(F.col("InvoiceDate") < window_start, "history_window_start"),
               F.col("__returned") > F.coalesce(F.col("__purchased"), F.lit(0)),
               observed=F.col("__returned"), reference_count=F.coalesce(F.col("__purchased"), F.lit(0)),
               value_at_risk=F.round(F.abs("line_value"), 2)),
    )
    return result.withColumn("__operational_checks", F.array(*checks))


def _rules_with(checks: str, tier: str) -> Column:
    return F.transform(
        F.filter(checks, lambda check: (check.tier == tier) & (check.status == "flagged")),
        lambda check: check.rule)


def build_output(transactions: DataFrame, run_date: str,
                 z_threshold: float = Z_THRESHOLD, min_samples: int = 30) -> DataFrame:
    """Classify rows, then evaluate tier-1 data and tier-2 business rules."""
    validate_input_contract(transactions)
    history = select_history(transactions, run_date)
    # Checkpoints cut the lineage: each baseline otherwise re-plans every upstream join.
    typed = add_data_checks(add_record_context(add_invoice_summary(
        add_description_context(add_base_columns(history))))).localCheckpoint()
    deviation = add_deviation_checks(typed, z_threshold, min_samples).localCheckpoint()
    scored = add_operational_checks(deviation)
    scored = (
        scored.withColumn("check_results", F.concat(
            "__data_checks", "__deviation_checks", "__operational_checks"))
        .withColumn("data_anomaly_flags", _rules_with("check_results", "data"))
        .withColumn("business_anomaly_flags", _rules_with("check_results", "business"))
        .withColumn("anomaly_flags", F.concat("data_anomaly_flags", "business_anomaly_flags"))
    )
    severities = F.transform(F.filter("check_results", lambda check: check.status == "flagged"),
                             lambda check: check.severity)
    scored = scored.withColumn(
        "max_severity",
        F.when(F.array_contains(severities, "high"), "high")
        .when(F.array_contains(severities, "medium"), "medium")
        .when(F.array_contains(severities, "low"), "low"),
    ).withColumn(
        "assessment_status",
        F.when(F.size("anomaly_flags") > 0, "flagged")
        .when(F.exists("check_results", lambda check: check.status == "not_flagged"), "not_flagged")
        .otherwise("not_assessable"),
    )
    return scored.select(
        *TRANSACTION_SCHEMA.fieldNames(),
        F.lit(run_date).cast("date").alias("run_date"),
        F.lit(REFERENCE_MODE).alias("reference_mode"),
        F.lit(z_threshold).cast("double").alias("z_threshold"),
        F.lit(min_samples).cast("int").alias("min_samples"),
        "line_value", "record_type", "channel", "has_dotcom_postage",
        "data_quality_flags", "context_flags", "check_results",
        "data_anomaly_flags", "business_anomaly_flags", "anomaly_flags",
        "max_severity", "assessment_status",
    )


def validate_output(transactions: DataFrame, result: DataFrame, run_date: str) -> None:
    """Reject lost/duplicated source rows and malformed checks before writing."""
    history = select_history(transactions, run_date).select(*TRANSACTION_SCHEMA.fieldNames())
    original = result.select(*TRANSACTION_SCHEMA.fieldNames())
    if history.count() != result.count() or not history.exceptAll(original).isEmpty()             or not original.exceptAll(history).isEmpty():
        raise ValueError("Output does not preserve the input row multiset before cutoff")
    expected_rules = F.array(*[F.lit(rule) for rule in RULES])
    invalid = (
        F.col("run_date").isNull() | (F.col("run_date") != F.lit(run_date).cast("date"))
        | (F.transform("check_results", lambda check: check.rule) != expected_rules)
        | F.exists("check_results", lambda check: ~check.status.isin("flagged", "not_flagged", "not_applied")
                   | ~check.tier.isin("data", "business")
                   | ((check.status == "flagged") != check.severity.isNotNull()))
        | ~F.col("assessment_status").isin("flagged", "not_flagged", "not_assessable")
        | F.col("record_type").isNull() | F.col("channel").isNull()
    )
    if not result.filter(invalid).isEmpty():
        raise ValueError("Output contains invalid run date, status, severity, or check order")


def write_results(result: DataFrame, output_root: str, run_date: str) -> str:
    """Replace only the requested local date partition."""
    output_path = str(Path(output_root).resolve() / f"run_date={run_date}")
    result.write.mode("overwrite").parquet(output_path)
    return output_path


def _log_results(result: DataFrame) -> None:
    for column in ("assessment_status", "max_severity", "channel", "record_type"):
        for record in result.groupBy(column).count().collect():
            logger.info("%s=%s rows=%s", column, record[column], record["count"])
    invoice = _normalized(F.col("InvoiceNo"))
    exploded = result.select(invoice.alias("invoice"), F.explode("check_results").alias("check"))
    for record in (
        exploded.filter(F.col("check.status") == "flagged")
        .groupBy("check.tier", "check.rule").agg(
            F.count("*").alias("rows"), F.countDistinct("invoice").alias("invoices"),
            F.round(F.sum("check.value_at_risk"), 2).alias("value_at_risk"))
        .collect()
    ):
        logger.info("flagged tier=%s rule=%s rows=%s invoices=%s value_at_risk=%s",
                    record["tier"], record["rule"], record.rows, record.invoices, record.value_at_risk)
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
        result = build_output(transactions, args.run_date, args.z_threshold,
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
    import sys

    if "--pipeline-context" in sys.argv:
        from retail_pipeline import pipeline_main

        pipeline_main("audit")
    else:
        main()
