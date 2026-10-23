"""Train immutable RFM models or bootstrap ETL without a scoring model."""

import argparse
import math
from uuid import uuid4

from retail_contracts import (
    atomic_json,
    build_context,
    context_file,
    data_root,
    json_hash,
    load_context,
    safe_path,
)
from retail_models import FEATURE_VERSION, artifact_hashes, read_training_etl


def train_classifier(spark, context, source, root, output, version, etl):
    """Use the current registry/publication protocol for a supervised bundle."""
    from pyspark_rfm import compute_rfm, select_history, fit_rfm_model
    from rfm_supervised import select_and_refit, save_bundle, load_bundle, score
    from retail_models import bundle_hashes
    from datetime import date, timedelta
    dates = [context['classifier_train_date'], context['classifier_validation_date'],
             context['classifier_test_date'], context['run_date']]
    frames = [compute_rfm(select_history(source, cutoff), cutoff) for cutoff in dates]
    teacher = fit_rfm_model(frames[0], context['rfm_k'], context['seed'])
    student, info = select_and_refit(*frames, teacher, train_date=dates[0], validation_date=dates[1],
                                    test_date=dates[2], refit_date=dates[3],
                                    trees=context['num_trees_grid'], depths=context['max_depth_grid'],
                                    seed=context['seed'], minimum=context['min_macro_f1'])
    info.update(etl_manifest=context['etl_manifest'], etl_hash=context['etl_hash'],
                snapshot_mode='retrospective_event_time_slices_of_frozen_etl')
    bundle = output / 'bundle'
    save_bundle(spark, str(bundle), student, teacher, info)
    restored, _ = load_bundle(spark, str(bundle), (date.fromisoformat(dates[3]) + timedelta(days=1)).isoformat())
    expected = score(frames[3], student, version).select('CustomerID', 'Cluster')
    actual = score(frames[3], restored, version).select('CustomerID', 'Cluster')
    if not expected.exceptAll(actual).isEmpty() or not actual.exceptAll(expected).isEmpty():
        raise ValueError('Classifier save/load changed assignments')
    pipeline = (bundle / 'student').relative_to(root).as_posix()
    manifest = dict(complete=True, model_kind='classifier', model_version=version,
                    feature_version=FEATURE_VERSION, pipeline_path=pipeline,
                    bundle_path=bundle.relative_to(root).as_posix(),
                    artifact_hashes=artifact_hashes(root, pipeline),
                    bundle_hashes=bundle_hashes(root, bundle.relative_to(root).as_posix()),
                    supervised_info=info, spark_version=spark.version,
                    training_cutoff=context['run_date'], source_version=context['source_version'],
                    source_hash=context['source_hash'], etl_manifest=context['etl_manifest'],
                    etl_hash=json_hash(etl), training_context_hash=json_hash(context),
                    training_code_version=context['code_version'], k=context['rfm_k'], seed=context['seed'])
    atomic_json(output / 'model.json', manifest)
    candidate = dict(context_hash=json_hash(context), model_version=version,
                     model_manifest=(output / 'model.json').relative_to(root).as_posix(),
                     model_manifest_hash=json_hash(manifest))
    atomic_json(context_file(root, context['run_key']).with_name('training.json'), candidate)
    return candidate


def train_model(spark, context, root):
    """Fit on a frozen ETL snapshot and commit a verified candidate only."""
    from pyspark.ml import PipelineModel

    from pyspark_rfm import (
        apply_rfm_model,
        compute_rfm,
        fit_rfm_model,
        select_history,
        validate_curated_contract,
    )

    if context.get("kind") != "training":
        raise ValueError("Expected a training context")
    etl, _ = read_training_etl(root, context["etl_manifest"])
    if json_hash(etl) != context["etl_hash"]:
        raise ValueError("Training ETL generation changed")
    source = spark.read.parquet(
        str(safe_path(root, etl["outputs"]["transactions"]))
    )
    validate_curated_contract(source)
    rfm = compute_rfm(
        select_history(source, context["run_date"]), context["run_date"]
    ).cache()
    version = uuid4().hex
    output = safe_path(root, f"models/rfm/version={version}")
    output.mkdir(parents=True)
    try:
        if context.get('model_kind', 'kmeans') == 'classifier':
            return train_classifier(spark, context, source, root, output, version, etl)
        model = fit_rfm_model(rfm, context["rfm_k"], context["seed"])
        scaler, kmeans = model.stages[-2:]
        values = [*scaler.mean, *scaler.std]
        centers = [list(map(float, v)) for v in kmeans.clusterCenters()]
        values.extend(v for center in centers for v in center)
        if not all(math.isfinite(float(v)) for v in values):
            raise ValueError("Non-finite model parameters")
        pipeline_path = (output / "pipeline").relative_to(root).as_posix()
        model.write().save(str(output / "pipeline"))
        restored = PipelineModel.load(str(output / "pipeline"))
        expected = apply_rfm_model(rfm, model).select("CustomerID", "Cluster")
        actual = apply_rfm_model(rfm, restored).select("CustomerID", "Cluster")
        if (
            not expected.exceptAll(actual).isEmpty()
            or not actual.exceptAll(expected).isEmpty()
        ):
            raise ValueError("Model save/load changed assignments")
        profile = {
            str(row.Cluster): row["count"]
            for row in actual.groupBy("Cluster").count().collect()
        }
        manifest = {
            "complete": True,
            "model_version": version,
            "feature_version": FEATURE_VERSION,
            "pipeline_path": pipeline_path,
            "artifact_hashes": artifact_hashes(root, pipeline_path),
            "spark_version": spark.version,
            "training_cutoff": context["run_date"],
            "source_version": context["source_version"],
            "source_hash": context["source_hash"],
            "etl_manifest": context["etl_manifest"],
            "etl_hash": context["etl_hash"],
            "training_context_hash": json_hash(context),
            "training_code_version": context["code_version"],
            "k": context["rfm_k"],
            "seed": context["seed"],
            "centers": centers,
            "scaler_mean": list(map(float, scaler.mean)),
            "scaler_std": list(map(float, scaler.std)),
            "cluster_sizes": profile,
        }
        atomic_json(output / "model.json", manifest)
        candidate = {
            "context_hash": json_hash(context),
            "model_version": version,
            "model_manifest": (output / "model.json")
            .relative_to(root)
            .as_posix(),
            "model_manifest_hash": json_hash(manifest),
        }
        atomic_json(
            context_file(root, context["run_key"]).with_name("training.json"),
            candidate,
        )
        return candidate
    finally:
        rfm.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-context")
    parser.add_argument("--bootstrap-date")
    parser.add_argument("--run-id")
    parser.add_argument("--source-version", default="online-retail-v1")
    args = parser.parse_args()
    root = data_root().resolve()
    if args.bootstrap_date:
        if args.pipeline_context or not args.run_id:
            parser.error("Bootstrap needs --run-id and no --pipeline-context")
        from retail_pipeline import run_stage

        metadata = build_context(
            root,
            "retail_bootstrap",
            args.run_id,
            args.bootstrap_date,
            {"source_version": args.source_version, "churn_days": 90},
            kind="bootstrap",
        )
        run_stage("etl", metadata["context_path"])
        print(context_file(root, metadata["run_key"]).with_name("etl.json"))
        return
    if not args.pipeline_context:
        parser.error("Provide --pipeline-context or --bootstrap-date")
    from pyspark.sql import SparkSession

    context = load_context(args.pipeline_context, root)
    spark = (
        SparkSession.builder.appName("retail-rfm-training")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(train_model(spark, context, root))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
