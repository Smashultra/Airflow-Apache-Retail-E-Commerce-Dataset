"""Make the same mounted modules importable in host and Docker tests."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for folder in (ROOT, ROOT / "scripts", ROOT / "dags"):
    sys.path.insert(0, str(folder))


@pytest.fixture
def model_factory():
    """Small registry fixture; Spark artifact behavior is tested separately."""
    from retail_contracts import atomic_json, json_hash, read_json, safe_path
    from retail_models import FEATURE_VERSION

    def create(root, source, version="v1", cutoff="2011-12-08",
               effective="2011-12-09"):
        path = f"models/rfm/version={version}/model.json"
        model = {
            "complete": True, "model_version": version, "k": 2,
            "feature_version": FEATURE_VERSION,
            "source_version": source["source_version"],
            "source_hash": json_hash(source), "training_cutoff": cutoff,
            "pipeline_path": f"models/rfm/version={version}/pipeline",
        }
        atomic_json(safe_path(root, path), model)
        registry_path = safe_path(root, "manifests/models/registry.json")
        registry = read_json(registry_path) if registry_path.exists() else {
            "activations": []
        }
        binding = {
            "model_version": version, "model_manifest": path,
            "model_manifest_hash": json_hash(model),
            "effective_from": effective,
            "sequence": len(registry["activations"]) + 1,
        }
        registry["activations"].append(binding)
        atomic_json(registry_path, registry)
        return binding

    return create
