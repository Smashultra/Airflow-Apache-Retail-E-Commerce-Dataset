"""Offline helper: score K-means over a range of k (WSSSE + silhouette)
to help you pick k for RFM segmentation. Not run by the daily DAG -
read k_scores.csv / the charts, then supply rfm_k to the training DAG.
The DAG passes --pipeline-context to reuse its frozen ETL and cutoff.

Usage:
    spark-submit scripts/select_kmeans_k.py \
        --input /data/curated --run-date 2026-09-01 \
        --k-min 2 --k-max 10 --output /data/analytics/k_selection/2026-09-01

Output: k_scores.csv (k, wssse, silhouette), plus elbow.png/silhouette.png
if matplotlib is available.
"""
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("select_kmeans_k")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Score K-means over a range of k.")
    parser.add_argument("--pipeline-context", help="Frozen training context from the DAG.")
    parser.add_argument("--input", help="Curated Parquet directory.")
    parser.add_argument("--run-date", help="Cutoff date, YYYY-MM-DD.")
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", help="Where to write standalone results.")

    args = parser.parse_args(argv)
    if args.pipeline_context:
        if any((args.input, args.run_date, args.output)):
            parser.error("--pipeline-context cannot be combined with --input/--run-date/--output")
    elif not all((args.input, args.run_date, args.output)):
        parser.error("Provide --pipeline-context or --input, --run-date and --output")
    if args.k_min < 2:
        parser.error("--k-min must be at least 2.")
    if args.k_max < args.k_min:
        parser.error("--k-max must be >= --k-min.")
    return args


def score_k_range(features_df, k_min: int, k_max: int, seed: int):
    """Fit KMeans for each k, return [(k, wssse, silhouette), ...].
    features_df must already have a scaled "features" column (fit the
    scaler once, outside this loop, so every k is scored consistently).
    """
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator

    evaluator = ClusteringEvaluator(
        predictionCol="Cluster", featuresCol="features", metricName="silhouette"
    )
    results = []
    for k in range(k_min, k_max + 1):
        model = KMeans(featuresCol="features", predictionCol="Cluster", k=k, seed=seed).fit(features_df)
        wssse = model.summary.trainingCost  # elbow metric
        silhouette = evaluator.evaluate(model.transform(features_df))
        results.append((k, wssse, silhouette))
        logger.info("k=%s  WSSSE=%.4f  silhouette=%.4f", k, wssse, silhouette)
    return results


def write_report(results, output_root: str) -> None:
    """Write k_scores.csv, plus elbow.png/silhouette.png if matplotlib
    is installed (charts are optional - the CSV is always enough)."""
    os.makedirs(output_root, exist_ok=True)
    csv_path = os.path.join(output_root, "k_scores.csv")
    with open(csv_path, "w") as f:
        f.write("k,wssse,silhouette\n")
        for k, wssse, silhouette in results:
            f.write(f"{k},{wssse},{silhouette}\n")
    logger.info("Wrote %s", csv_path)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not available; skipping charts.")
        return

    ks = [r[0] for r in results]
    for name, ylabel, color in [
        ("elbow", "WSSSE", None),
        ("silhouette", "Silhouette score", "orange"),
    ]:
        values = [r[1] if name == "elbow" else r[2] for r in results]
        plt.figure()
        plt.plot(ks, values, marker="o", color=color)
        plt.xlabel("k")
        plt.ylabel(ylabel)
        plt.title(f"{name.capitalize()} method")
        path = os.path.join(output_root, f"{name}.png")
        plt.savefig(path)
        plt.close()
        logger.info("Wrote %s", path)


def resolve_inputs(args):
    """Bind the report to exactly the ETL generation used by training."""
    if not args.pipeline_context:
        return None
    from retail_contracts import data_root, json_hash, load_context, safe_path
    from retail_models import read_training_etl

    root = data_root().resolve()
    context = load_context(args.pipeline_context, root)
    if context.get("kind") != "training":
        raise ValueError("Expected a training context")
    etl, source = read_training_etl(root, context["etl_manifest"])
    if json_hash(etl) != context["etl_hash"] or source["run_date"] != context["run_date"]:
        raise ValueError("Training ETL generation or cutoff changed")
    args.input = str(safe_path(root, etl["outputs"]["transactions"]))
    args.run_date = context.get("classifier_train_date", context["run_date"])
    args.seed = context["seed"]
    args.output = str(safe_path(root, f"analytics/k_selection/{context['run_key']}"))
    return context


def main():
    from pyspark.sql import SparkSession
    from pyspark_rfm import (
        compute_rfm, kmeans_feature_pipeline, prepare_kmeans_features,
        select_history, validate_curated_contract,
    )

    args = parse_args()
    context = resolve_inputs(args)
    spark = None
    try:
        spark = (SparkSession.builder.appName("SelectKMeansK")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())

        transactions = spark.read.parquet(args.input)
        validate_curated_contract(transactions)
        rfm = compute_rfm(select_history(transactions, args.run_date), args.run_date)

        prepared = prepare_kmeans_features(rfm)
        features_df = kmeans_feature_pipeline().fit(prepared).transform(prepared).cache()

        customer_count = features_df.count()
        k_max = min(args.k_max, max(customer_count - 1, args.k_min))
        if customer_count <= args.k_min:
            raise ValueError(f"Only {customer_count} customers; need more than --k-min ({args.k_min}).")
        if k_max < args.k_max:
            logger.warning("Reducing --k-max to %s (only %s customers).", k_max, customer_count)

        results = score_k_range(features_df, args.k_min, k_max, args.seed)
        write_report(results, args.output)
        if context:
            from pathlib import Path
            from retail_contracts import atomic_json

            atomic_json(Path(args.output) / "report.json", {
                "training_cutoff": args.run_date,
                "etl_manifest": context["etl_manifest"],
                "etl_hash": context["etl_hash"],
                "seed": args.seed,
                "rfm_k": context["rfm_k"],
                "customers": customer_count,
                "scores": [dict(k=k, wssse=w, silhouette=s) for k, w, s in results],
            })
        logger.info("Report: %s; cutoff=%s; ETL=%s. Choose rfm_k manually; no automatic selection.",
                    args.output, args.run_date, context["etl_manifest"] if context else args.input)
        features_df.unpersist()

    except Exception:
        logger.exception("k-selection job failed.")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
