"""Real Spark bootstrap, model persistence and daily inference lifecycle."""

import csv
from pathlib import Path

import pytest
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

import retail_contracts as contracts
import retail_models as models
from prepare_daily_landing import prepare
from retail_pipeline import run_stage
from retail_training import train_model


def test_bootstrap_training_daily_and_failed_republication(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("RETAIL_DATA_ROOT", str(tmp_path))
    # Small, distinct customers; two natural spending groups.
    source = tmp_path / "raw.csv"
    with source.open("w", encoding="cp1252", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(contracts.COLUMNS)
        for i in range(8):
            writer.writerow([
                str(i), "A", "Product", i + 1,
                f"12/{1 + i % 6}/2011 10:00", float(1 + i * i),
                str(i), "UK",
            ])
        writer.writerow(["new", "A", "Product", 2,
                         "12/9/2011 10:00", 5, "0", "UK"])
    prepare(source, tmp_path, "fixture")
    params = {"source_version": "fixture", "churn_days": 90}
    bootstrap = contracts.build_context(
        tmp_path, "bootstrap", "first", "2011-12-06", params,
        kind="bootstrap",
    )
    run_stage("etl", bootstrap["context_path"])
    training_params = {
        "source_version": "fixture", "rfm_k": 2,
        "effective_from": "2011-12-08", "etl_manifest": None,
    }
    metadata = models.build_training_context(
        tmp_path, "training", "first", "2011-12-07", training_params
    )
    context = contracts.load_context(metadata["context_path"], tmp_path)
    # No daily completion exists: training reads only committed ETL.
    assert not (tmp_path / "manifests/published").exists()
    with pytest.raises(ValueError, match="after training"):
        models.build_training_context(
            tmp_path, "training", "invalid", "2011-12-07",
            {**training_params, "effective_from": "2011-12-07"},
        )
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("retail-training-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        candidate = train_model(spark, context, tmp_path)
    finally:
        spark.stop()
    activation = models.publish_model(tmp_path, context)
    assert models.publish_model(tmp_path, context) == activation
    registry_path = tmp_path / "manifests/models/registry.json"
    registry_before = registry_path.read_bytes()
    model = models.read_model(tmp_path, candidate, verify_artifact=True)
    assert sum(model["cluster_sizes"].values()) == 8
    daily = contracts.build_context(
        tmp_path, "daily", "first", "2011-12-09", params
    )
    assert daily["rfm_k"] == 2

    def forbidden_fit(*args, **kwargs):
        raise AssertionError("Daily scoring must never fit a Pipeline")

    monkeypatch.setattr(Pipeline, "fit", forbidden_fit)
    run_stage("etl", daily["context_path"])
    run_stage("rfm", daily["context_path"])
    run_stage("audit", daily["context_path"])
    daily_context = contracts.load_context(daily["context_path"], tmp_path)
    summary = contracts.publish_completion(tmp_path, daily_context)
    assert summary["stages"]["rfm"]["metrics"]["customers"] == 8
    assert summary["stages"]["rfm"]["model"] == activation
    # A new activation must not silently change a frozen run.
    assert contracts.build_context(
        tmp_path, "daily", "first", "2011-12-09", params
    ) == daily

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("retail-model-readback").getOrCreate()
    )
    try:
        from pyspark.ml import PipelineModel
        from pyspark_rfm import (
            apply_rfm_model, validate_rfm_output,
        )

        result = spark.read.parquet(str(contracts.safe_path(
            tmp_path, summary["stages"]["rfm"]["outputs"]["customers"]
        )))
        assert result.select("model_version").first()[0] == (
            candidate["model_version"]
        )
        # Fewer customers than k and unoccupied clusters are valid inference.
        single = result.limit(1).drop("Cluster")
        loaded = PipelineModel.load(str(contracts.safe_path(
            tmp_path, model["pipeline_path"]
        )))
        scored = apply_rfm_model(single, loaded)
        validate_rfm_output(scored, "2011-12-10", 2)
        assert scored.count() == 1
    finally:
        spark.stop()

    # Corruption must fail closed and leave the serving registry untouched.
    artifact_name = next(iter(model["artifact_hashes"]))
    artifact = contracts.safe_path(tmp_path, model["pipeline_path"])
    artifact = Path(artifact) / artifact_name
    artifact.write_bytes(artifact.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="artifact hash mismatch"):
        models.publish_model(tmp_path, context)
    assert registry_path.read_bytes() == registry_before

    # Validation cannot silently switch to a different ETL generation.
    etl_path = contracts.safe_path(tmp_path, context["etl_manifest"])
    original = contracts.read_json(etl_path)
    contracts.atomic_json(etl_path, {**original, "duration_seconds": -1})
    with pytest.raises(ValueError, match="inputs changed"):
        models.build_training_context(
            tmp_path, "training", "first", "2011-12-07", training_params
        )
