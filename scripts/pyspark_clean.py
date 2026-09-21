"""Create the RFM and anomaly Parquet datasets from raw retail CSV data."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_INPUT_PATH = "data/raw/data.csv"
DEFAULT_RFM_OUTPUT_PATH = "data/curated/RFM.parquet"
DEFAULT_ANOMALIES_OUTPUT_PATH = "data/audit/anomalies.parquet"
TRANSACTION_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
    ]
)


def read_transactions(spark: SparkSession, input_path: str) -> DataFrame:
    """Read retail CSV data using the stable transaction schema."""
    return (
        spark.read.option("header", True)
        .option("timestampFormat", "M/d/yyyy H:mm")
        .option("nullValue", "NaN")
        .schema(TRANSACTION_SCHEMA)
        .csv(input_path)
    )


def prepare_datasets(transactions: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return cleaned RFM input and a deduplicated anomaly dataset."""
    anomalies = transactions.dropDuplicates()
    rfm = (
        anomalies.dropna(how="any")
        .filter(
            ~F.upper(F.trim(F.col("InvoiceNo").cast("string"))).startswith("C")
        )
        .filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
    )
    return rfm, anomalies


def write_datasets(
    rfm: DataFrame,
    anomalies: DataFrame,
    rfm_output_path: str,
    anomalies_output_path: str,
) -> None:
    """Overwrite both output datasets as Parquet directories."""
    rfm.write.mode("overwrite").parquet(rfm_output_path)
    anomalies.write.mode("overwrite").parquet(anomalies_output_path)


def run_cleaning_job(
    spark: SparkSession,
    input_path: str,
    rfm_output_path: str,
    anomalies_output_path: str,
) -> tuple[DataFrame, DataFrame]:
    """Read the input, prepare both datasets, write them, and return the frames."""
    transactions = read_transactions(spark, input_path)
    rfm, anomalies = prepare_datasets(transactions)
    write_datasets(rfm, anomalies, rfm_output_path, anomalies_output_path)
    return rfm, anomalies


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate retail data and create RFM and anomaly Parquet outputs."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Raw CSV path")
    parser.add_argument(
        "--rfm-output",
        default=DEFAULT_RFM_OUTPUT_PATH,
        help="RFM Parquet output path",
    )
    parser.add_argument(
        "--anomalies-output",
        default=DEFAULT_ANOMALIES_OUTPUT_PATH,
        help="Anomalies Parquet output path",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    spark = (
        SparkSession.builder
        .appName("retail-pyspark-clean")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        run_cleaning_job(
            spark,
            args.input,
            args.rfm_output,
            args.anomalies_output,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
