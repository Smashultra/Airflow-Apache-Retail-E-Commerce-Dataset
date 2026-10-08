"""Model activation failure boundaries without starting Spark."""

import pytest

import retail_contracts as contracts
import retail_models as models


@pytest.fixture
def candidate(tmp_path):
    context = {
        "kind": "training",
        "run_key": contracts.json_hash("training-run"),
        "run_date": "2011-12-01",
        "effective_from": "2011-12-02",
        "etl_hash": "etl-hash",
    }
    pipeline = tmp_path / "models/rfm/version=v1/pipeline"
    (pipeline / "metadata").mkdir(parents=True)
    (pipeline / "metadata/part-00000").write_text("fixture")
    model_path = "models/rfm/version=v1/model.json"
    model = {
        "complete": True,
        "model_version": "v1",
        "feature_version": models.FEATURE_VERSION,
        "k": 2,
        "training_cutoff": context["run_date"],
        "training_context_hash": contracts.json_hash(context),
        "etl_hash": context["etl_hash"],
        "pipeline_path": pipeline.relative_to(tmp_path).as_posix(),
    }
    model["artifact_hashes"] = models.artifact_hashes(
        tmp_path, model["pipeline_path"]
    )
    contracts.atomic_json(tmp_path / model_path, model)
    binding = {
        "context_hash": contracts.json_hash(context),
        "model_version": "v1",
        "model_manifest": model_path,
        "model_manifest_hash": contracts.json_hash(model),
    }
    contracts.atomic_json(
        contracts.context_file(tmp_path, context["run_key"]).with_name(
            "training.json"
        ),
        binding,
    )
    return tmp_path, context


def test_failed_activation_preserves_registry_and_releases_lock(
    candidate, monkeypatch
):
    root, context = candidate
    registry = root / "manifests/models/registry.json"
    contracts.atomic_json(registry, {"activations": []})
    before = registry.read_bytes()
    original_replace = contracts.os.replace

    def fail_registry(source, destination):
        if destination == registry:
            raise OSError("simulated interrupted activation")
        return original_replace(source, destination)

    monkeypatch.setattr(contracts.os, "replace", fail_registry)
    with pytest.raises(OSError, match="interrupted"):
        models.publish_model(root, context)
    assert registry.read_bytes() == before
    assert not (registry.parent / ".registry.lock").exists()
    monkeypatch.setattr(contracts.os, "replace", original_replace)
    activation = models.publish_model(root, context)
    assert models.publish_model(root, context) == activation
    assert len(contracts.read_json(registry)["activations"]) == 1


def test_incompatible_feature_and_context_fail_before_activation(candidate):
    root, context = candidate
    with pytest.raises(ValueError, match="context mismatch"):
        models.publish_model(root, {**context, "effective_from": "2011-12-03"})
    path = root / "models/rfm/version=v1/model.json"
    model = contracts.read_json(path)
    model["feature_version"] = "unknown-feature-version"
    contracts.atomic_json(path, model)
    binding = {
        "model_version": "v1",
        "model_manifest": path.relative_to(root).as_posix(),
        "model_manifest_hash": contracts.json_hash(model),
    }
    with pytest.raises(ValueError, match="Incompatible"):
        models.read_model(root, binding)
    assert not (root / "manifests/models/registry.json").exists()
