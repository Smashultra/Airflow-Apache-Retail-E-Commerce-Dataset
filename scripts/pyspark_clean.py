"""Create the RFM and anomaly Parquet datasets from raw retail CSV data."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession


DEFAULT_INPUT_PATH = "data/raw/data.csv"
DEFAULT_RFM_OUTPUT_PATH = "data/curated/RFM.parquet"
DEFAULT_ANOMALIES_OUTPUT_PATH = "data/audit/anomalies.parquet"


def read_transactions(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the retail CSV with its header and infer the source column types."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )


def prepare_datasets(transactions: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return RFM-ready and anomaly datasets using their respective null rules."""
    anomalies = transactions.dropDuplicates()
    rfm = anomalies.dropna(how="any")
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
    spark = SparkSession.builder.appName("retail-pyspark-clean").getOrCreate()
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
