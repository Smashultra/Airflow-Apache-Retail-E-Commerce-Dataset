"""Spark execution contracts for the official DAG and legacy jobs."""

import argparse
import logging
import math
import time
from datetime import date
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

from retail_contracts import (
    COLUMNS,
    atomic_json,
    data_root,
    days,
    json_hash,
    load_context,
    load_stage,
    safe_path,
    stage_file,
    validate_landing,
)

LOGGER = logging.getLogger("retail_pipeline")


def finite(column):
    return (
        column.isNotNull()
        & ~F.isnan(column)
        & (F.abs(column) != F.lit(float("inf")))
    )


def read_landing(spark, paths):
    """Parse UTF-8 landing files and preserve failed raw values."""
    raw = (
        spark.read.schema(",".join(f"{c} STRING" for c in COLUMNS))
        .option("header", True)
        .option("mode", "FAILFAST")
        .option("multiLine", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(paths)
    )
    expressions = []
    for column in COLUMNS:
        value = F.when(F.col(column).isin("", "NaN"), F.lit(None)).otherwise(
            F.col(column)
        )
        if column == "InvoiceDate":
            value = F.try_to_timestamp(value, F.lit("M/d/yyyy H:mm"))
        elif column == "Quantity":
            value = F.expr("try_cast(Quantity AS INT)")
        elif column == "UnitPrice":
            value = F.expr("try_cast(UnitPrice AS DOUBLE)")
        expressions.append(value.alias(column))
    typed = raw.select(*expressions)
    parsed = raw.select(
        *[F.col(c).alias(f"raw_{c}") for c in COLUMNS], *expressions
    )
    bad = F.lit(False)
    for name in ("InvoiceDate", "Quantity", "UnitPrice"):
        bad = bad | (
            F.col(f"raw_{name}").isNotNull()
            & ~F.col(f"raw_{name}").isin("", "NaN")
            & F.col(name).isNull()
        )
    return typed, parsed.filter(bad)


def customer_labels(frame, churn_days, history_days):
    """Label observed inactivity, not a predicted churn probability."""
    enough = history_days >= churn_days
    result = (
        frame.withColumn("is_high_value", F.col("M_score") >= 4)
        .withColumn("is_inactive", F.col("Recency") >= churn_days)
        .withColumn("observed_history_days", F.lit(history_days))
        .withColumn("churn_threshold_days", F.lit(churn_days))
        .withColumn("segment_rule_version", F.lit("rfm-rule-v1"))
        .withColumn(
            "churn_assessment_status",
            F.when(F.lit(not enough), "insufficient_history")
            .when(F.col("is_inactive"), "inactivity_proxy")
            .otherwise("not_flagged"),
        )
    )
    return result.withColumn(
        "segment",
        F.when(
            F.col("is_high_value") & F.col("is_inactive"),
            "high_value_inactive",
        )
        .when(
            F.col("is_high_value")
            & (F.col("F_score") >= 4)
            & (F.col("R_score") >= 4),
            "high_value_active",
        )
        .when(F.col("is_inactive"), "inactive")
        .when(
            (F.col("R_score") >= 4) & (F.col("F_score") <= 2),
            "recent_low_frequency",
        )
        .otherwise("regular"),
    )


def assess_orders(transactions, cutoff, min_orders=30, multiplier=3.0):
    """Assess retrospective invoice totals, rejecting invalid lines."""
    valid = (
        (F.col("Quantity") > 0)
        & finite(F.col("UnitPrice"))
        & (F.col("UnitPrice") > 0)
        & F.col("InvoiceDate").isNotNull()
        & (F.col("InvoiceDate") < F.lit(cutoff).cast("timestamp"))
        & F.col("Country").isNotNull()
        & (F.trim("Country") != "")
    )
    line_total = F.col("Quantity").cast("decimal(18,4)") * F.col(
        "UnitPrice"
    ).cast("decimal(18,4)")
    source = (
        transactions.withColumn("invoice_key", F.upper(F.trim("InvoiceNo")))
        .filter(
            F.col("invoice_key").isNotNull() & (F.col("invoice_key") != "")
        )
        .withColumn("valid_line", F.coalesce(valid, F.lit(False)))
        .withColumn("line_amount", F.when(F.col("valid_line"), line_total))
        .withColumn(
            "customer_key",
            F.when(F.trim("CustomerID") != "", F.trim("CustomerID")),
        )
    )
    orders = source.groupBy("invoice_key").agg(
        F.count("*").alias("line_count"),
        F.sum((~F.col("valid_line")).cast("int")).alias("invalid_lines"),
        F.countDistinct("customer_key").alias("customer_ids"),
        F.countDistinct("Country").alias("countries"),
        F.countDistinct(F.to_date("InvoiceDate")).alias("invoice_days"),
        F.round(F.sum("line_amount"), 2).alias("partial_total"),
    )
    eligible = (
        ~F.col("invoice_key").startswith("C")
        & (F.col("invalid_lines") == 0)
        & (F.col("customer_ids") <= 1)
        & (F.col("countries") == 1)
        & (F.col("invoice_days") == 1)
    )
    orders = (
        orders.withColumn("eligible", eligible)
        .withColumn("order_total", F.when(eligible, F.col("partial_total")))
        .drop("partial_total")
    )
    stats = (
        orders.filter("eligible")
        .agg(
            F.count("*").alias("n"),
            F.avg("order_total").alias("mean"),
            F.stddev_samp("order_total").alias("stddev"),
        )
        .first()
    )
    mean = float(stats["mean"]) if stats["mean"] is not None else None
    stddev = stats["stddev"]
    applied = (
        stats.n >= min_orders
        and mean is not None
        and math.isfinite(mean)
        and stddev is not None
        and math.isfinite(stddev)
        and stddev > 0
    )
    threshold = mean + multiplier * stddev if applied else None
    reason = (
        F.when(F.col("invoice_key").startswith("C"), "cancelled_invoice")
        .when(F.col("invalid_lines") > 0, "invalid_invoice_lines")
        .when(F.col("customer_ids") > 1, "multiple_customer_ids")
        .when(F.col("countries") != 1, "inconsistent_country")
        .when(F.col("invoice_days") != 1, "inconsistent_invoice_date")
        .when(F.lit(stats.n < min_orders), "insufficient_reference")
        .when(F.lit(not applied), "zero_or_invalid_stddev")
    )
    return (
        orders.withColumn("reason", reason)
        .withColumn("reference_n", F.lit(stats.n))
        .withColumn("reference_mean", F.lit(mean).cast("double"))
        .withColumn("reference_stddev", F.lit(stddev).cast("double"))
        .withColumn("upper_bound", F.lit(threshold).cast("double"))
        .withColumn("currency", F.lit("GBP"))
        .withColumn("multiplier", F.lit(multiplier))
        .withColumn("run_date", F.lit(cutoff).cast("date"))
        .withColumn("reference_mode", F.lit("retrospective_before_run_date"))
        .withColumn(
            "status",
            F.when(F.col("reason").isNotNull(), "not_assessable")
            .when(F.col("order_total") > F.col("upper_bound"), "flagged")
            .otherwise("not_flagged"),
        )
    )


def counts(frame, column):
    return {
        row[column]: row["count"]
        for row in frame.groupBy(column).count().collect()
    }


def parquet(frame, path, partitioned=False):
    writer = frame.write.mode("errorifexists")
    if partitioned:
        writer = writer.partitionBy("year", "month")
    writer.parquet(str(path))
    if not (path / "_SUCCESS").is_file():
        raise ValueError(f"Missing success marker: {path}")


def run_clean(spark, context, output, root):
    from pyspark_clean import prepare_datasets

    source, files = validate_landing(
        root,
        context["source_version"],
        context["business_date"],
        checksums=True,
    )
    if json_hash(source) != context["source_hash"]:
        raise ValueError("Source manifest changed since validation")
    transactions, rejected = read_landing(spark, files)
    transactions = transactions.cache()
    try:
        raw_count = transactions.count()
        expected = source["undated"]["rows"] + sum(
            source["partitions"][day]["rows"]
            for day in days(source["first_date"], context["business_date"])
        )
        if raw_count != expected:
            raise ValueError("Landing record count does not match manifest")
        history = transactions.filter(
            F.col("InvoiceDate").isNull()
            | (
                F.col("InvoiceDate")
                < F.lit(context["run_date"]).cast("timestamp")
            )
        )
        if history.count() != raw_count:
            raise ValueError("Landing partition contains a future transaction")
        rfm, audit = prepare_datasets(history)
        rfm = (
            rfm.filter(
                finite(F.col("UnitPrice"))
                & (F.trim("CustomerID") != "")
                & (F.trim("InvoiceNo") != "")
            )
            .withColumn("year", F.year("InvoiceDate"))
            .withColumn("month", F.month("InvoiceDate"))
        )
        audit = audit.cache()
        rfm = rfm.cache()
        try:
            dedup_count, clean_count = audit.count(), rfm.count()
            metrics = {
                "input_rows": raw_count,
                "dedup_rows": dedup_count,
                "duplicates_removed": raw_count - dedup_count,
                "rfm_rows": clean_count,
                "rfm_excluded_unique": dedup_count - clean_count,
                "parse_error_rows": rejected.count(),
            }
            parquet(rfm, output / "transactions", partitioned=True)
            parquet(audit, output / "audit_input")
            parquet(rejected, output / "parse_errors")
        finally:
            audit.unpersist()
            rfm.unpersist()
    finally:
        transactions.unpersist()
    return metrics, ["transactions", "audit_input", "parse_errors"], None


def run_rfm(spark, context, output, root):
    from pyspark_rfm import (
        add_rfm_scores,
        add_rfm_segment,
        compute_rfm,
        select_history,
        validate_curated_contract,
        validate_rfm_output,
    )

    etl = load_stage(root, context, "etl")
    source = spark.read.parquet(
        str(safe_path(root, etl["outputs"]["transactions"]))
    )
    validate_curated_contract(source)
    history = select_history(source, context["run_date"])
    result = add_rfm_segment(
        add_rfm_scores(compute_rfm(history, context["run_date"])),
        k=context["rfm_k"],
    )
    history_days = (
        date.fromisoformat(context["run_date"])
        - date.fromisoformat(context["first_date"])
    ).days
    result = customer_labels(
        result, context["churn_days"], history_days
    ).cache()
    try:
        validate_rfm_output(result, context["run_date"], context["rfm_k"])
        metrics = {
            "customers": result.count(),
            "segments": counts(result, "segment"),
            "churn_status": counts(result, "churn_assessment_status"),
            "clusters": {
                str(k): v for k, v in counts(result, "Cluster").items()
            },
            "high_value": result.filter("is_high_value").count(),
            "inactive": result.filter("is_inactive").count(),
        }
        if sum(metrics["segments"].values()) != metrics["customers"]:
            raise ValueError("Segment counts do not reconcile")
        parquet(result, output / "customers")
        profile = result.groupBy("Cluster").agg(
            F.count("*").alias("customers"),
            *[
                F.avg(c).alias(f"mean_{c}")
                for c in ("Recency", "Frequency", "Monetary")
            ],
        )
        parquet(profile, output / "cluster_profile")
    finally:
        result.unpersist()
    return metrics, ["customers", "cluster_profile"], json_hash(etl)


def run_audit(spark, context, output, root):
    from pyspark_anomalies import build_output, validate_output

    etl = load_stage(root, context, "etl")
    source = spark.read.parquet(
        str(safe_path(root, etl["outputs"]["audit_input"]))
    )
    result = build_output(source, context["run_date"]).cache()
    orders = None
    try:
        validate_output(source, result, context["run_date"])
        orders = assess_orders(source, context["run_date"]).cache()
        metrics = {
            "assessed_rows": result.count(),
            "row_status": counts(result, "assessment_status"),
            "orders": orders.count(),
            "order_status": counts(orders, "status"),
            "rows_without_invoice": source.filter(
                F.col("InvoiceNo").isNull() | (F.trim("InvoiceNo") == "")
            ).count(),
        }
        if sum(metrics["order_status"].values()) != metrics["orders"]:
            raise ValueError("Order counts do not reconcile")
        if (
            orders.select("invoice_key").distinct().count()
            != metrics["orders"]
        ):
            raise ValueError("Duplicate order keys")
        parquet(result, output / "row_assessments")
        parquet(orders, output / "order_assessments")
        parquet(orders.filter("status = 'flagged'"), output / "flagged_orders")
    finally:
        result.unpersist()
        if orders is not None:
            orders.unpersist()
    return (
        metrics,
        ["row_assessments", "order_assessments", "flagged_orders"],
        json_hash(etl),
    )


def run_stage(stage, context_path):
    root = data_root().resolve()
    context = load_context(context_path, root)
    prefixes = {
        "etl": "curated",
        "rfm": "analytics/rfm_daily",
        "audit": "audit/anomalies",
    }
    generation = f"{context['run_key'][:12]}-{uuid4().hex}"
    output = safe_path(
        root,
        f"{prefixes[stage]}/run_date={context['run_date']}/version={generation}",
    )
    output.mkdir(parents=True)
    started = time.monotonic()
    spark = (
        SparkSession.builder.appName(f"retail-{stage}-{context['run_date']}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        metrics, names, etl_hash = {
            "etl": run_clean,
            "rfm": run_rfm,
            "audit": run_audit,
        }[stage](spark, context, output, root)
        manifest = {
            "stage": stage,
            "complete": True,
            "context_hash": json_hash(context),
            "etl_hash": etl_hash,
            "outputs": {
                name: (output / name).relative_to(root).as_posix()
                for name in names
            },
            "metrics": metrics,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        atomic_json(output / "summary.json", manifest)
        atomic_json(stage_file(root, context, stage), manifest)
        LOGGER.warning("STAGE_COMPLETE %s", manifest)
    finally:
        spark.stop()


def pipeline_main(stage):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-context", required=True)
    args = parser.parse_args()
    run_stage(stage, args.pipeline_context)
