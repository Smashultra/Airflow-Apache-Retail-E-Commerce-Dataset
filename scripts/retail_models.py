"""Versioned model and training contracts; safe to import without Spark."""

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from retail_contracts import (
    atomic_json,
    code_version,
    context_file,
    digest,
    iso_date,
    json_hash,
    read_json,
    safe_path,
    token,
)

FEATURE_VERSION = "rfm-log1p-standard-v1"
SCORE_REFERENCE_MODE = "daily_population_percent_rank"


@contextmanager
def registry_lock(root):
    """Serialize activation and daily model binding on shared local storage."""
    path = safe_path(root, "manifests/models/.registry.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.close(descriptor)
        yield
    finally:
        path.unlink()


def artifact_hashes(root, pipeline_path):
    directory = safe_path(root, pipeline_path)
    files = {}
    for path in sorted(directory.rglob("*")):
        checked = safe_path(directory, path)
        if checked.is_file():
            files[path.relative_to(directory).as_posix()] = digest(checked)
    if not files or "metadata/part-00000" not in files:
        # Spark versions may use generated names for metadata parts.
        if not any(p.startswith("metadata/part-") for p in files):
            raise ValueError("Incomplete model artifact metadata")
    return files


def read_model(root, binding, *, verify_artifact=False):
    model = read_json(safe_path(root, binding["model_manifest"]))
    if json_hash(model) != binding["model_manifest_hash"]:
        raise ValueError("Model manifest hash mismatch")
    if (
        not model.get("complete")
        or model["model_version"] != binding["model_version"]
        or model["feature_version"] != FEATURE_VERSION
        or type(model["k"]) is not int
        or not 2 <= model["k"] <= 20
    ):
        raise ValueError("Incompatible or incomplete model")
    token(model["model_version"])
    iso_date(model["training_cutoff"])
    safe_path(root, model["pipeline_path"])
    if (
        verify_artifact
        and artifact_hashes(root, model["pipeline_path"])
        != (model["artifact_hashes"])
    ):
        raise ValueError("Model artifact hash mismatch")
    return model


def validate_binding(root, binding, cutoff, source_version, source_hash):
    model = read_model(root, binding)
    if not (
        iso_date(model["training_cutoff"]) < iso_date(cutoff)
        and iso_date(binding["effective_from"]) <= iso_date(cutoff)
        and iso_date(model["training_cutoff"])
        < iso_date(binding["effective_from"])
    ):
        raise ValueError("Model is not eligible for scoring cutoff")
    if (
        model["source_version"] != source_version
        or model["source_hash"] != source_hash
    ):
        raise ValueError("Model source is incompatible with daily source")
    return model


def resolve_model(root, cutoff, source_version, source_hash, requested=None):
    """Caller holds registry lock until the daily context is committed."""
    if requested is not None:
        token(requested)
    path = safe_path(root, "manifests/models/registry.json")
    entries = read_json(path)["activations"] if path.exists() else []
    candidates = []
    for entry in entries:
        if requested and entry["model_version"] != requested:
            continue
        # Incompatible source versions may coexist in the registry.
        model = read_model(root, entry)
        if (
            model["training_cutoff"] < cutoff
            and entry["effective_from"] <= cutoff
            and model["source_version"] == source_version
            and model["source_hash"] == source_hash
        ):
            validate_binding(root, entry, cutoff, source_version, source_hash)
            candidates.append(entry)
    if not candidates:
        raise ValueError("No eligible model; bootstrap/train before daily")
    return max(candidates, key=lambda e: (e["effective_from"], e["sequence"]))


def read_training_etl(root, manifest_path):
    path = safe_path(root, manifest_path)
    etl = read_json(path)
    source_context = read_json(path.with_name("context.json"))
    if (
        path
        != context_file(root, source_context["run_key"]).with_name("etl.json")
        or etl.get("stage") != "etl"
        or not etl.get("complete")
        or etl.get("context_hash") != json_hash(source_context)
    ):
        raise ValueError("Invalid committed ETL manifest")
    for output in etl["outputs"].values():
        if not (safe_path(root, output) / "_SUCCESS").is_file():
            raise ValueError("Incomplete training ETL output")
    return etl, source_context


def build_training_context(root, dag_id, run_id, cutoff, params):
    """Pin a committed ETL generation; never require daily completion."""
    iso_date(cutoff)
    effective = params["effective_from"]
    if iso_date(effective) <= iso_date(cutoff):
        raise ValueError("effective_from must be after training cutoff")
    k = params["rfm_k"]
    if type(k) is not int or not 2 <= k <= 20:
        raise ValueError("rfm_k must be between 2 and 20")
    run_key = json_hash([dag_id, run_id])
    path = context_file(root, run_key)
    saved = read_json(path) if path.exists() else None
    requested_etl = params.get("etl_manifest")
    source_version = token(params["source_version"])
    pointer = None
    if requested_etl:
        manifest_path = safe_path(root, requested_etl)
    elif saved:
        manifest_path = safe_path(root, saved["etl_manifest"])
    else:
        pointer = read_json(
            safe_path(root, f"manifests/etl/{source_version}/{cutoff}.json")
        )
        manifest_path = safe_path(root, pointer["etl_manifest"])
    etl, source = read_training_etl(root, manifest_path)
    if (
        source["run_date"] != cutoff
        or source["source_version"] != source_version
        or (pointer and pointer["etl_hash"] != json_hash(etl))
    ):
        raise ValueError("Training ETL cutoff mismatch")
    value = {
        "kind": "training",
        "dag_id": dag_id,
        "run_id": run_id,
        "run_key": run_key,
        "requested_etl": requested_etl,
        "code_version": code_version(),
        "run_date": cutoff,
        "source_version": source["source_version"],
        "source_hash": source["source_hash"],
        "feature_version": FEATURE_VERSION,
        "rfm_k": k,
        "seed": 42,
        "effective_from": effective,
        "etl_manifest": manifest_path.relative_to(
            Path(root).resolve()
        ).as_posix(),
        "etl_hash": json_hash(etl),
    }
    path = context_file(root, value["run_key"])
    with registry_lock(root):
        if path.exists() and read_json(path) != value:
            raise ValueError("Training inputs changed: create a new DagRun")
        atomic_json(path, value)
    return {"context_path": str(path), **value}


def publish_model(root, context):
    """Activate only a verified candidate; repeat publication is idempotent."""
    candidate = read_json(
        context_file(root, context["run_key"]).with_name("training.json")
    )
    if context.get("kind") != "training" or candidate[
        "context_hash"
    ] != json_hash(context):
        raise ValueError("Training candidate context mismatch")
    model = read_model(root, candidate, verify_artifact=True)
    if (
        model["training_context_hash"] != json_hash(context)
        or model["training_cutoff"] != context["run_date"]
        or model["etl_hash"] != context["etl_hash"]
    ):
        raise ValueError("Training model provenance mismatch")
    with registry_lock(root):
        path = safe_path(root, "manifests/models/registry.json")
        registry = read_json(path) if path.exists() else {"activations": []}
        for entry in registry["activations"]:
            if entry["training_run_key"] == context["run_key"]:
                if (
                    entry["model_manifest_hash"]
                    != (candidate["model_manifest_hash"])
                ):
                    raise ValueError("Already published; create a new run")
                return entry
        entry = {
            key: candidate[key]
            for key in (
                "model_version",
                "model_manifest",
                "model_manifest_hash",
            )
        }
        entry.update(
            effective_from=context["effective_from"],
            training_run_key=context["run_key"],
            registered_at=datetime.now(timezone.utc).isoformat(),
            sequence=len(registry["activations"]) + 1,
        )
        registry["activations"].append(entry)
        atomic_json(path, registry)
    return entry
