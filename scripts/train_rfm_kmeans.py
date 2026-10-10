"""Fit and save an RFM pipeline once for reuse by daily scoring."""
import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
# fit và lưu pipeline để daily sử dụng lại
from pyspark.sql import DataFrame, SparkSession

# Chạy theo package hoặc spark-submit đều dùng helper trong pyspark_rfm.
if __package__:
    from .pyspark_rfm import (
        DEFAULT_KMEANS_SEED, LOG_FEATURES,
        kmeans_feature_pipeline, logger, prepare_kmeans_features,
        assert_separate_paths, add_store_arguments, validate_store_arguments,
        ingest_store, read_store_rfm, state_to_rfm, read_store_table,
    )
else:
    from pyspark_rfm import (
        DEFAULT_KMEANS_SEED, LOG_FEATURES,
        kmeans_feature_pipeline, logger, prepare_kmeans_features,
        assert_separate_paths, add_store_arguments, validate_store_arguments,
        ingest_store, read_store_rfm, state_to_rfm, read_store_table,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--k", required=True, type=int,
                        help="Explicit k chosen by offline analysis.")
    parser.add_argument("--model-output", required=True,
                        help="New version directory; never overwritten.")
    parser.add_argument("--seed", type=int, default=DEFAULT_KMEANS_SEED)
    add_store_arguments(parser)
    args = parser.parse_args(argv)
    validate_store_arguments(parser, args)
    if args.k < 2:
        parser.error("--k must be at least 2.")
    if not args.model_output.strip():
        parser.error("--model-output must not be blank.")
    return args


def train_model(rfm: DataFrame, k: int,
                seed: int = DEFAULT_KMEANS_SEED):
    """Preserve training guards and return the whole fitted pipeline."""
    if k < 2:
        raise ValueError("k must be at least 2 for K-means segmentation.")
    customer_count = rfm.count()
    if k > customer_count:
        raise ValueError(
            f"k={k} exceeds the number of customers ({customer_count}); "
            "reduce k or provide more data."
        )
    # Tạo log features; cache vì training sử dụng nhiều lần.
    prepared = prepare_kmeans_features(rfm).cache()
    try:
        distinct_vectors = prepared.select(*LOG_FEATURES).distinct().count()
        if distinct_vectors < k:
            raise ValueError(
                f"Only {distinct_vectors} distinct feature vectors are "
                f"available for k={k} clusters; reduce k or provide "
                "more varied data."
            )
        # k và seed do lần training này lựa chọn.
        kmeans = KMeans(featuresCol="features", predictionCol="Cluster",
                        k=k, seed=seed)
        pipeline = Pipeline(stages=[
            *kmeans_feature_pipeline().getStages(), kmeans,
        ])
        model = pipeline.fit(prepared)
        # kiểm tra số cụm có khách sau training (phân cụm lại chính tập training bằng model vừa học, rồi đếm số ID cụm xuất hiện)
        occupied = (model.transform(prepared)
                    .select("Cluster").distinct().count())
        if occupied < k: # chỉ áp dụng cho training
            raise ValueError(
                f"K-means produced {occupied} occupied clusters; expected {k}."
            )
        return model
    finally:
        prepared.unpersist()


def main():
    args = parse_args()
    spark = None
    try:
        spark = (SparkSession.builder.appName("TrainRFMKMeans")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        if args.input:
            assert_separate_paths(spark, args.input, args.model_output)
        # Khởi tạo từ curated một lần; các lần sau đọc trạng thái tích lũy.
        assert_separate_paths(spark, args.store_path, args.model_output)
        if args.input:
            manifest = ingest_store(
                spark, args.input, args.store_path, args.batch_id,
                args.run_date, args.initialize_store, args.num_buckets,
            )
            rfm = state_to_rfm(
                read_store_table(spark, manifest, 'state'), args.run_date)
        else:
            rfm, manifest = read_store_rfm(spark, args.store_path, args.run_date)
        # Chỉ file training gọi fit; daily chỉ load và transform.
        model = train_model(rfm, args.k, args.seed)
        # No overwrite: a released version must not silently change.
        model.write().save(args.model_output)
        # Audit the training cutoff and selected state snapshot alongside model.
        metadata = {'run_date': args.run_date, 'k': args.k, 'seed': args.seed,
                    'store_version': manifest['version'],
                    'store_path': args.store_path,
                    'log_features': LOG_FEATURES}
        spark.createDataFrame([(json.dumps(metadata, sort_keys=True),)], ['value']).coalesce(1).write.text(
            args.model_output.rstrip('/') + '/rfm_training_info')
        logger.info("Training completed: cutoff=%s, k=%s, seed=%s, model=%s",
                    args.run_date, args.k, args.seed, args.model_output)
    except Exception:
        logger.exception("RFM training failed.")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
