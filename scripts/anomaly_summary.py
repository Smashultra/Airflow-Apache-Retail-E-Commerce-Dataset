"""Print Markdown summary tables for anomaly results (row and order level).

Reads one ``run_date`` partition written by ``pyspark_anomalies.py`` and the
same anomaly-input Parquet, re-runs ``assess_orders`` for invoice totals and
prints the tables used in section 9 of ``docs/ANOMALY_DETECTION_VI.md``.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from pyspark_anomalies import assess_orders


def _markdown(frame: DataFrame, limit: int = 200) -> str:
    rows = frame.limit(limit).collect()
    header = "| " + " | ".join(frame.columns) + " |"
    divider = "|" + "|".join("---" for _ in frame.columns) + "|"

    def cell(value: object) -> str:
        if value is None:
            return "–"
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return f"{value:,}"
        return str(value).replace("|", "/")

    return "\n".join([header, divider, *("| " + " | ".join(cell(v) for v in row) + " |" for row in rows)])


def _section(title: str, frame: DataFrame) -> None:
    print(f"\n### {title}\n")
    print(_markdown(frame))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, help="Result partition directory")
    parser.add_argument("--input", required=True, help="Anomaly-input Parquet directory")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args(argv)
    spark = (SparkSession.builder.appName("anomaly-summary")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows = spark.read.parquet(args.results).cache()
        print(f"Rows: {rows.count():,}")
        _section("R1. Trạng thái đánh giá và mức độ cao nhất",
                 rows.groupBy("assessment_status", "max_severity").count()
                 .orderBy("assessment_status", "max_severity"))
        _section("R2. record_type × channel", rows.groupBy("record_type").pivot("channel").count()
                 .orderBy("record_type"))
        checks = rows.select("InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice",
                             "CustomerID", "channel", F.explode("check_results").alias("c")).select(
            "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", "CustomerID", "channel", "c.*")
        flagged = checks.filter(F.col("status") == "flagged").cache()
        _section("R3. Dòng bị flag theo tier/rule", flagged.groupBy("tier", "rule").agg(
            F.count("*").alias("rows"), F.countDistinct("InvoiceNo").alias("invoices"),
            F.round(F.sum("value_at_risk"), 2).alias("value_at_risk_gbp"),
            F.sum((F.col("severity") == "high").cast("int")).alias("high"),
            F.sum((F.col("severity") == "medium").cast("int")).alias("medium"),
            F.sum((F.col("severity") == "low").cast("int")).alias("low"),
        ).orderBy("tier", F.desc("rows")))
        _section("R4. Dòng business bị flag theo kênh", flagged.filter(F.col("tier") == "business")
                 .groupBy("rule").pivot("channel").count().orderBy("rule"))
        _section("R5. Cấp baseline của rule ngữ cảnh (áp dụng / bị flag)",
                 checks.filter(F.col("rule").isin("quantity_deviation", "price_deviation")
                               & (F.col("status") != "not_applied"))
                 .groupBy("rule", "context_level").agg(
                     F.count("*").alias("applied"),
                     F.sum((F.col("status") == "flagged").cast("int")).alias("flagged"))
                 .orderBy("rule", "context_level"))
        contextual = flagged.filter(F.col("rule").isin("quantity_deviation", "price_deviation"))
        _section("R5b. Flag ngữ cảnh: MAD = 0, mức low, phân vị fold và value_at_risk",
                 contextual.groupBy("rule", "context_level").agg(
                     F.count("*").alias("flagged"),
                     F.round(100 * F.avg(F.col("robust_z").isNull().cast("int")), 1).alias("mad0_pct"),
                     F.round(100 * F.avg((F.col("severity") == "low").cast("int")), 1).alias("low_pct"),
                     F.round(F.percentile("fold_change", F.lit(0.1)), 2).alias("fold_p10"),
                     F.round(F.percentile("fold_change", F.lit(0.5)), 2).alias("fold_p50"),
                     F.round(F.percentile("fold_change", F.lit(0.9)), 2).alias("fold_p90"),
                     F.round(F.percentile("value_at_risk", F.lit(0.5)), 2).alias("value_p50"))
                 .orderBy("rule", "context_level"))
        _section("R5c. Flag giá theo chiều lệch và kênh",
                 contextual.filter(F.col("rule") == "price_deviation").groupBy(
                     F.when(F.col("fold_change") < 1, "lower").otherwise("higher").alias("direction"),
                     "channel").agg(F.count("*").alias("rows"),
                                    F.percentile("Quantity", F.lit(0.5)).alias("quantity_p50"))
                 .orderBy("direction", "channel"))
        _section("R6. Lý do không áp dụng", checks.filter(F.col("status") == "not_applied")
                 .groupBy("rule", "reason").count().orderBy("rule", F.desc("count")))
        examples = flagged.filter(F.col("tier") == "business")
        print("\n### R7. Ví dụ lớn nhất theo value_at_risk mỗi rule business")
        for rule in [r.rule for r in flagged.filter(F.col("tier") == "business").select("rule").distinct()
                     .orderBy("rule").collect()]:
            _section(rule, examples.filter(F.col("rule") == rule).orderBy(F.desc_nulls_last("value_at_risk"))
                     .select("InvoiceNo", "StockCode", "Description", "CustomerID", "channel", "context_level",
                             "observed_value", "reference_median", "fold_change", "value_at_risk",
                             "severity").limit(3))
        orders = assess_orders(spark.read.parquet(args.input), args.run_date).cache()
        _section("R8. Hóa đơn: trạng thái theo kênh", orders.groupBy("channel").pivot("status").count()
                 .orderBy("channel"))
        _section("R9. Hóa đơn bị flag theo cấp baseline và mức độ",
                 orders.filter(F.col("status") == "flagged").groupBy("channel", "context_level", "severity")
                 .agg(F.count("*").alias("orders"), F.round(F.sum("value_at_risk"), 2).alias("value_at_risk_gbp"))
                 .orderBy("channel", "context_level", "severity"))
        _section("R10. Lý do hóa đơn không đánh giá được",
                 orders.filter(F.col("status") == "not_assessable").groupBy("reason").count()
                 .orderBy(F.desc("count")))
        _section("R11. Hóa đơn bị flag lớn nhất", orders.filter(F.col("status") == "flagged")
                 .orderBy(F.desc("value_at_risk")).select(
                     "invoice_key", "channel", "customer_key", "context_level", "order_total",
                     "reference_median", "fold_change", "value_at_risk", "severity").limit(5))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
