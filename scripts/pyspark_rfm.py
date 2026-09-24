"""Compute daily customer RFM metrics from curated transactions."""
import argparse
import logging
from datetime import date

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("ecommerce_rfm")
DEFAULT_KMEANS_SEED = 42
# k không có giá trị mặc định: số cụm phải được truyền tường minh (CLI --k),
# vì đây là quyết định phân tích (elbow/silhouette) chứ không nên "ngầm định".


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

    parser.add_argument(
        "--k",
        required=True,
        type=int,
        help=(
            "Number of K-means clusters. No default on purpose: choose it "
            "via a one-off elbow/silhouette analysis, then pass it explicitly."
        ),
    )

    args = parser.parse_args(argv)

    if args.k < 2:
        parser.error("--k must be at least 2.")

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
    cutoff_date = date.fromisoformat(run_date)

    if {"year", "month"}.issubset(df.columns):
        df = df.filter(
            (F.col("year") < F.lit(cutoff_date.year))
            | ((F.col("year") == F.lit(cutoff_date.year))
            & (F.col("month") <= F.lit(cutoff_date.month)))
        )

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

# Shared feature prep for K-means, reused by both add_rfm_segment (production
# clustering, fixed k) and scripts/select_kmeans_k.py (offline elbow/silhouette
# analysis to *choose* k). Keeping this in one place means the k you pick
# during analysis is guaranteed to be scored on the exact same feature space
# used in the daily job — log1p skew correction + zero-mean/unit-variance
# scaling on Recency/Frequency/Monetary.
def prepare_kmeans_features(rfm: DataFrame) -> DataFrame:
    return (
        rfm
        .withColumn("Recency_log", F.log1p(F.col("Recency").cast("double")))
        .withColumn("Frequency_log", F.log1p(F.col("Frequency").cast("double")))
        .withColumn("Monetary_log", F.log1p(F.col("Monetary").cast("double")))
    )


def kmeans_feature_pipeline() -> Pipeline:
    """Assembles the *_log columns into a vector and standard-scales them
    (no KMeans stage). Fit once and reuse across every k you try.
    """
    assembler = VectorAssembler(
        inputCols=["Recency_log", "Frequency_log", "Monetary_log"],
        outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features",
        withMean=True, withStd=True,
    )
    return Pipeline(stages=[assembler, scaler])


# 5b. Cluster customers on Recency/Frequency/Monetary with K-means
def add_rfm_segment(
    rfm: DataFrame,
    k: int,
    seed: int = DEFAULT_KMEANS_SEED,
) -> DataFrame:
    """Cluster customers into k groups using K-means.

    k has no default: it's an analysis decision (elbow/silhouette), not
    an implicit default baked into the pipeline. Features are log1p
    Recency/Frequency/Monetary, standard-scaled (see prepare_kmeans_features
    / kmeans_feature_pipeline).

    Adds one column: Cluster (raw K-means id, 0-indexed). Cluster ids are
    arbitrary and can shift between runs as the input data changes — do
    not assume "Cluster 0" means the same thing across different dates.
    Comparing cluster centroids by value and naming them is a manual EDA
    step done later, not part of this function.
    """
    if k < 2:
        raise ValueError("k must be at least 2 for K-means segmentation.")

    customer_count = rfm.count()
    if k > customer_count:
        raise ValueError(
            f"k={k} exceeds the number of customers ({customer_count}); "
            "reduce k or provide more data."
        )

    prepared = prepare_kmeans_features(rfm)

    distinct_vectors = prepared.select(
        "Recency_log", "Frequency_log", "Monetary_log"
    ).distinct().count()
    if distinct_vectors < k:
        raise ValueError(
            f"Only {distinct_vectors} distinct feature vectors are available "
            f"for k={k} clusters; reduce k or provide more varied data."
        )

    kmeans = KMeans(featuresCol="features", predictionCol="Cluster", k=k, seed=seed)
    pipeline = Pipeline(stages=[*kmeans_feature_pipeline().getStages(), kmeans])

    model = pipeline.fit(prepared)
    clustered = model.transform(prepared)

    occupied = clustered.select("Cluster").distinct().count()
    if occupied < k:
        raise ValueError(
            f"K-means produced {occupied} occupied clusters; expected {k}."
        )

    return clustered.drop(
        "Recency_log", "Frequency_log", "Monetary_log", "raw_features", "features"
    )


# Check results before writing to output folder
def validate_rfm_output(result: DataFrame, run_date: str, k: int) -> None:
    """Reject invalid output before replacing a daily partition."""
    if k < 2:
        raise ValueError("k must be at least 2 for K-means segmentation.")
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("run_date must use YYYY-MM-DD.")
    required = {
        "CustomerID", "run_date", "LastPurchaseDate", "Recency",
        "Frequency", "Monetary", "R_score", "F_score", "M_score", "RFM_score",
        "Cluster",
    }
    missing = required - set(result.columns)
    if missing:
        raise ValueError(f"RFM output is missing columns: {sorted(missing)}")
    if result.isEmpty():
        raise ValueError("RFM output is empty.")

    cluster_dtype = result.schema["Cluster"].dataType
    if not isinstance(cluster_dtype, (IntegerType, LongType)):
        raise ValueError(
            f"Cluster must have an integer type; got {cluster_dtype.simpleString()}."
        )

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

    invalid_cluster = (
        F.col("Cluster").isNull() | (F.col("Cluster") < 0) | (F.col("Cluster") >= k)
    )
    if not result.filter(invalid_cluster).isEmpty():
        raise ValueError(
            f"RFM output contains invalid Cluster values (must be an integer in [0, {k}))."
        )

    if not result.groupBy("CustomerID").count().filter(F.col("count") > 1).isEmpty():
        raise ValueError("RFM output contains duplicate customers.")

# 6. Write results for the current cutoff date.
def write_rfm(
    result: DataFrame, output_root: str, run_date: str, k: int
) -> str:
    """Overwrite only the partition for the current cutoff date."""
    validate_rfm_output(result, run_date, k=k)
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

        logger.info("Starting RFM job: input=%s, run_date=%s, k=%s",
                    args.input, args.run_date, args.k)
        transactions = spark.read.parquet(args.input)
        validate_curated_contract(transactions)
        history = select_history(transactions, args.run_date)

        rfm = compute_rfm(history, args.run_date)
        result = add_rfm_segment(add_rfm_scores(rfm), k=args.k).cache()
        customer_count = result.count()

        if customer_count < 5:
            logger.warning(
                "Only %s customers found: relative scores use a small population "
                "and may not cover all five levels.",
                customer_count,
            )

        output_path = write_rfm(result, args.output, args.run_date, k=args.k)
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
