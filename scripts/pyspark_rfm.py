"""Compute daily customer RFM metrics from curated transactions."""
import argparse
import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("ecommerce_rfm")


# 1. Parse command-line arguments and validate the run date.
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Compute RFM metrics from curated transactions."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the curated Parquet directory.", # nơi đọc curated Parquet
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path to the RFM output directory.", # nơi lưu kết quả RFM
    )

    parser.add_argument(
        "--run-date",
        required=True,
        help="Analysis cutoff date in YYYY-MM-DD format.",
    )

    args = parser.parse_args(argv)

    try:
        parsed_date = date.fromisoformat(args.run_date)

        if parsed_date.isoformat() != args.run_date:
            raise ValueError("Invalid date format.")

    except ValueError:
        parser.error(
            "--run-date must be a valid date in YYYY-MM-DD format."
        )

    return args


# 2. Verify that curated data satisfies the RFM input contract (đủ cột, đúng data type, giá trị hợp lệ)
def validate_curated_contract(df: DataFrame) -> None:
    # Required columns for RFM aggregation.
    required_columns = {
        "CustomerID",
        "InvoiceNo",
        "InvoiceDate",
        "Quantity",
        "UnitPrice",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Data is missing required columns: {sorted(missing_columns)}"
        )

    # The cleaning job must normalize curated data to one stable schema.
    expected_types = {
        "CustomerID": StringType(),
        "InvoiceNo": StringType(),
        "InvoiceDate": TimestampType(),
        "Quantity": IntegerType(),
        "UnitPrice": DoubleType(),
    }

    for column_name, expected_type in expected_types.items():
        actual_type = df.schema[column_name].dataType

        if actual_type != expected_type:
            raise ValueError(
                f"Column '{column_name}' has type "
                f"{actual_type.simpleString()}; "
                f"expected: {expected_type.simpleString()}."
            )

    if df.isEmpty():
        raise ValueError("Input data is empty.")

    # Required columns must not contain null values.
    invalid_row = (
        F.col("CustomerID").isNull()
        | F.col("InvoiceNo").isNull()
        | F.col("InvoiceDate").isNull()
        | F.col("Quantity").isNull()
        | F.col("UnitPrice").isNull()
    )

    # Customer and invoice identifiers must not be blank.
    invalid_row = (
        invalid_row
        | (F.trim(F.col("CustomerID").cast("string")) == "")
        | (F.trim(F.col("InvoiceNo")) == "")
    )

    # This assertion ensures that the cleaning job handled invalid rows.
    invalid_row = (
        invalid_row
        | (F.col("Quantity") <= 0)
        | (F.col("UnitPrice") <= 0)
        | F.isnan("UnitPrice")
        | (F.abs(F.col("UnitPrice")) == F.lit(float("inf")))
    )

    if not df.filter(invalid_row).isEmpty():
        raise ValueError(
            "Curated data contains invalid values: required columns are "
            "null, customer/invoice identifiers are blank, or "
            "Quantity/UnitPrice are not positive finite values. The cleaning job "
            "did not satisfy the curated data contract."
        )


# 3. Select transaction history before the cutoff date.
def select_history(
    df: DataFrame,
    run_date: str,
) -> DataFrame:
    cutoff = F.lit(run_date).cast("date")

    history = df.filter(
        F.col("InvoiceDate") < cutoff
    )

    if history.isEmpty():
        raise ValueError(
            f"No transactions exist before the cutoff date {run_date}."
        )

    return history

# 4. Compute RFM metrics.
def compute_rfm(
    history: DataFrame,
    run_date: str,
) -> DataFrame:
    """Aggregate prefiltered history; cast prices to 4 decimal places,
    then round each customer total to 2 places. Zero after rounding is allowed.
    """

    transactions = history.withColumn(
        "LineTotal",
        (
            F.col("Quantity").cast("decimal(18, 4)")
            * F.col("UnitPrice").cast("decimal(18, 4)")
        ),
    )

    rfm = (
        transactions
        .groupBy("CustomerID")
        .agg(
            F.max("InvoiceDate").alias("LastPurchaseDate"),
            F.countDistinct("InvoiceNo").alias("Frequency"),
            F.round(
                F.sum("LineTotal"), 2
            ).alias("Monetary"),
        )
        .withColumn(
            "Recency",
            F.datediff(
                F.lit(run_date).cast("date"),
                F.to_date("LastPurchaseDate"),
            ),
        )
        .withColumn(
            "run_date",
            F.lit(run_date).cast("date"),
        )
    )

    return rfm.select(
        "CustomerID",
        "run_date",
        "LastPurchaseDate",
        "Recency",
        "Frequency",
        "Monetary",
    )


# 5. Score each metric relative to the current customer population.
def add_rfm_scores(rfm: DataFrame) -> DataFrame:
    """Preserve ties using percent_rank; CustomerID never affects scores.

    Rank intervals [0, .2), [.2, .4), [.4, .6), [.6, .8), [.8, 1]
    map to scores 1 through 5. A constant metric or a single customer
    receives score 1, with no neutral-score override. Groups may be uneven
    and some scores may be absent. Scores can change across analysis dates.
    Input must contain valid metrics and one row per customer.
    """
    result = rfm
    for metric, score_name, ascending in (
        ("Recency", "R_score", False),
        ("Frequency", "F_score", True),
        ("Monetary", "M_score", True),
    ):
        order = F.col(metric).asc() if ascending else F.col(metric).desc()
        rank = F.percent_rank().over(Window.orderBy(order))
        score = (
            F.when(rank < 0.2, 1)
            .when(rank < 0.4, 2)
            .when(rank < 0.6, 3)
            .when(rank < 0.8, 4)
            .otherwise(5)
        )
        result = result.withColumn(score_name, score)

    return result.withColumn(
        "RFM_score",
        F.concat(*(F.col(c).cast("string")
                   for c in ("R_score", "F_score", "M_score"))),
    )


# Check results before writing to output folder
def validate_rfm_output(result: DataFrame, run_date: str) -> None:
    """Reject invalid output before replacing a daily partition."""
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("run_date must use YYYY-MM-DD.")
    required = {
        "CustomerID", "run_date", "LastPurchaseDate", "Recency",
        "Frequency", "Monetary", "R_score", "F_score", "M_score", "RFM_score",
    }
    missing = required - set(result.columns)
    if missing:
        raise ValueError(f"RFM output is missing columns: {sorted(missing)}")
    if result.isEmpty():
        raise ValueError("RFM output is empty.")

    cutoff = F.lit(run_date).cast("date")
    invalid = (
        F.col("CustomerID").isNull()
        | (F.trim(F.col("CustomerID")) == "")
        | F.col("run_date").isNull()
        | (F.col("run_date") != cutoff)
        | F.col("LastPurchaseDate").isNull()
        | (F.col("LastPurchaseDate") >= cutoff)
        | F.col("Recency").isNull()
        | (F.col("Recency") < 1)
        | (F.col("Recency") != F.datediff(cutoff, F.to_date("LastPurchaseDate")))
        | F.col("Frequency").isNull()
        | (F.col("Frequency") < 1)
        | F.col("Monetary").isNull()
        | (F.col("Monetary") < 0)
        | F.isnan(F.col("Monetary").cast("double"))
        | (F.abs(F.col("Monetary").cast("double")) == F.lit(float("inf")))
    )
    for column in ("R_score", "F_score", "M_score"):
        invalid = invalid | F.col(column).isNull() | (~F.col(column).isin(1, 2, 3, 4, 5))
    expected_code = F.concat(*(F.col(c).cast("string")
                              for c in ("R_score", "F_score", "M_score")))
    invalid = invalid | F.col("RFM_score").isNull() | (F.col("RFM_score") != expected_code)
    if not result.filter(invalid).isEmpty():
        raise ValueError("RFM output contains invalid metrics, scores, or run_date.")
    if not result.groupBy("CustomerID").count().filter(F.col("count") > 1).isEmpty():
        raise ValueError("RFM output contains duplicate customers.")


# 6. Write results for the current cutoff date.
def write_rfm(result: DataFrame, output_root: str, run_date: str) -> str:
    """Overwrite only the partition for the current cutoff date."""
    validate_rfm_output(result, run_date)
    output_path = f"{output_root.rstrip('/')}/run_date={run_date}"
    (
        result.drop("run_date")
        .write.mode("overwrite")
        .parquet(output_path)
    )
    return output_path


# 7. Run the complete RFM job.
def main():
    args = parse_args()
    spark = None
    result = None

    try:
        spark = (
            SparkSession.builder
            .appName("CalculateRFM")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )

        logger.info("Starting RFM job: input=%s, run_date=%s",
                    args.input, args.run_date)
        transactions = spark.read.parquet(args.input)
        validate_curated_contract(transactions)
        history = select_history(transactions, args.run_date)

        rfm = compute_rfm(history, args.run_date)
        result = add_rfm_scores(rfm).cache()
        customer_count = result.count()

        if customer_count < 5:
            logger.warning(
                "Only %s customers found: relative scores use a small population "
                "and may not cover all five levels.",
                customer_count,
            )

        output_path = write_rfm(result, args.output, args.run_date)
        logger.info("RFM job completed: customers=%s, output=%s",
                    customer_count, output_path)

    except Exception:
        logger.exception("RFM job failed.")
        raise

    finally:
        try:
            if result is not None:
                result.unpersist()
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
