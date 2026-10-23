"""Same-run ETL replay preserves published downstream generations."""

from unittest.mock import Mock

import pytest

import retail_pipeline as pipeline
from retail_contracts import atomic_json, context_file, json_hash, stage_file
from retail_support.publication import branch_publication


@pytest.fixture
def reusable(analytics_ready, monkeypatch):
    root, context, stages = analytics_ready
    source = {"fixture": "unchanged"}
    context.update(business_date="2011-12-09", source_hash=json_hash(source))
    atomic_json(context_file(root, context["run_key"]), context)
    for name, manifest in stages.items():
        manifest["context_hash"] = json_hash(context)
        if name != "etl":
            manifest["etl_hash"] = json_hash(stages["etl"])
        atomic_json(stage_file(root, context, name), manifest)
    landing = Mock(return_value=(source, []))
    monkeypatch.setattr(pipeline, "validate_landing", landing)
    spark = Mock(side_effect=AssertionError("Spark must not start on reuse"))
    monkeypatch.setattr(pipeline, "SparkSession", spark)
    return root, context, stages, landing, spark


def test_clear_etl_preserves_published_branches(reusable, caplog):
    root, context, _, landing, spark = reusable
    before = stage_file(root, context, "etl").read_bytes()
    published = {
        name: branch_publication(root, context, name, publish=True)
        for name in ("rfm", "audit")
    }
    for _ in range(2):
        pipeline.run_stage("etl", context_file(root, context["run_key"]))
    assert stage_file(root, context, "etl").read_bytes() == before
    for name in published:
        assert branch_publication(root, context, name) == published[name]
    assert "ETL_REUSED" in caplog.text
    assert not spark.mock_calls
    landing.assert_called_with(root, "fixture", "2011-12-09", checksums=True)


@pytest.mark.parametrize("damage", ["output", "context", "source", "checksum", "schema"])
def test_invalid_reuse_fails_without_replacing_etl(reusable, damage):
    root, context, stages, landing, spark = reusable
    if damage == "output":
        (root / stages["etl"]["outputs"]["transactions"] / "_SUCCESS").unlink()
    elif damage == "context":
        stages["etl"]["context_hash"] = "changed"
    elif damage == "source":
        landing.return_value = ({"changed": True}, [])
    elif damage == "checksum":
        landing.side_effect = ValueError("Landing checksum mismatch")
    else:
        stages["etl"]["outputs"].pop("transactions")
    atomic_json(stage_file(root, context, "etl"), stages["etl"])
    before = stage_file(root, context, "etl").read_bytes()
    with pytest.raises(ValueError):
        pipeline.run_stage("etl", context_file(root, context["run_key"]))
    assert stage_file(root, context, "etl").read_bytes() == before
    assert not spark.mock_calls


def test_reuse_preserves_newer_discovery_pointer(reusable):
    root, context, _, _, _ = reusable
    pointer = root / "manifests/etl/fixture/2011-12-10.json"
    atomic_json(pointer, {"newer_run": True})
    before = pointer.read_bytes()
    pipeline.run_stage("etl", context_file(root, context["run_key"]))
    assert pointer.read_bytes() == before


def test_missing_stage_still_runs_computation(reusable, monkeypatch):
    root, context, _, _, _ = reusable
    stage_file(root, context, "etl").unlink()
    session = Mock()
    monkeypatch.setattr(pipeline, "SparkSession", session)
    clean = Mock(return_value=({}, ["transactions", "audit_input", "parse_errors"], None))
    monkeypatch.setattr(pipeline, "run_clean", clean)
    pipeline.run_stage("etl", context_file(root, context["run_key"]))
    clean.assert_called_once()
    assert stage_file(root, context, "etl").exists()
