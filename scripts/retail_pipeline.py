"""Spark execution contracts for the official DAG and legacy jobs."""

import argparse
import logging
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
    from pyspark_anomalies import assess_orders, build_output, validate_output

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
