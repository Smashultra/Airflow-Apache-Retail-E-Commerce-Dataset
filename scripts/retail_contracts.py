"""Shared filesystem contracts without Spark imports."""

import csv
import hashlib
import json
import math
import os
import re
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]
RULE_VERSION = "retail-v2"
SCHEMA_VERSION = 1


def data_root():
    """Resolve trusted deployment configuration, never a UI-provided path."""
    return Path(os.environ.get("RETAIL_DATA_ROOT", "/opt/airflow/data"))


def safe_path(root, relative):
    root = Path(root).resolve()
    candidate = (root / relative).resolve()
    if candidate == root or root not in candidate.parents:
        raise ValueError(f"Path must be below {root}: {relative}")
    return candidate


def token(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,80}", value
    ):
        raise ValueError("Invalid source version")
    return value


def iso_date(value):
    if (
        not isinstance(value, str)
        or date.fromisoformat(value).isoformat() != value
    ):
        raise ValueError("Expected an ISO YYYY-MM-DD date")
    return date.fromisoformat(value)


def days(first, last):
    current = iso_date(first)
    end = iso_date(last)
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def json_hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def read_json(path):
    if Path(path).stat().st_size > 2 * 1024 * 1024:
        raise ValueError("Metadata exceeds 2 MiB limit")
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def atomic_json(path, value):
    """Atomically replace one metadata file on the same filesystem."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def code_version():
    """Fingerprint executable business code, including uncommitted changes."""
    directory = Path(__file__).parent
    names = [
        "retail_contracts.py",
        "retail_pipeline.py",
        "retail_models.py",
        "retail_training.py",
        "pyspark_clean.py",
        "pyspark_rfm.py",
        "train_rfm_kmeans.py",
        "rfm_supervised.py",
        "train_rfm_classifier.py",
        "pyspark_anomalies.py",
    ]
    return json_hash({name: digest(directory / name) for name in names})


def validate_landing(root, version, business_date, checksums=False):
    """Check metadata/header readiness; full hashes are reserved for Spark."""
    base = safe_path(root, f"raw/landing/{token(version)}")
    manifest = read_json(base / "manifest.json")
    if (
        not manifest.get("complete")
        or manifest.get("source_version") != version
    ):
        raise ValueError("Source manifest is incomplete or has wrong version")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported landing schema version")
    day = iso_date(business_date)
    if (
        not iso_date(manifest["first_date"])
        <= day
        <= iso_date(manifest["last_date"])
    ):
        raise ValueError("Business date is outside source coverage")
    entries = []
    for item in days(manifest["first_date"], business_date):
        if item not in manifest["partitions"]:
            raise ValueError(f"Missing daily partition in manifest: {item}")
        entries.append(manifest["partitions"][item])
    entries.append(manifest["undated"])
    for entry in entries:
        path = safe_path(base, entry["path"])
        if not path.is_file() or path.stat().st_size <= 0:
            raise ValueError(f"Missing or zero-byte landing file: {path}")
        if path.stat().st_size != entry["bytes"]:
            raise ValueError(f"Landing file size mismatch: {path}")
        with path.open(encoding="utf-8", newline="") as stream:
            if next(csv.reader(stream), None) != COLUMNS:
                raise ValueError(f"CSV header mismatch: {path}")
        if checksums and digest(path) != entry["sha256"]:
            raise ValueError(f"Landing checksum mismatch: {path}")
    return manifest, [str(safe_path(base, e["path"])) for e in entries]


def context_file(root, run_key):
    if not re.fullmatch(r"[0-9a-f]{64}", run_key):
        raise ValueError("Invalid run key")
    return safe_path(root, f"manifests/runs/{run_key}/context.json")


def build_context(
    root, dag_id, run_id, business_date, params, *, kind="daily"
):
    from retail_models import (
        FEATURE_VERSION,
        registry_lock,
        resolve_model,
        validate_binding,
    )

    if kind not in {"daily", "bootstrap"}:
        raise ValueError("Unknown context kind")
    version = token(params["source_version"])
    source, _ = validate_landing(root, version, business_date)
    k, churn = params.get("rfm_k"), params["churn_days"]
    if k is not None and (type(k) is not int or not 2 <= k <= 20):
        raise ValueError("rfm_k must be an integer between 2 and 20")
    if type(churn) is not int or not 1 <= churn <= 3650:
        raise ValueError("churn_days must be an integer between 1 and 3650")
    run_key = json_hash([dag_id, run_id])
    context = {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "dag_id": dag_id,
        "run_id": run_id,
        "run_key": run_key,
        "business_date": business_date,
        "run_date": (iso_date(business_date) + timedelta(days=1)).isoformat(),
        "source_version": version,
        "source_hash": json_hash(source),
        "rule_version": RULE_VERSION,
        "code_version": code_version(),
        "requested_k": k,
        "requested_model": params.get("model_version"),
        "feature_version": FEATURE_VERSION,
        "churn_days": churn,
        "first_date": source["first_date"],
    }
    path = context_file(root, run_key)
    with registry_lock(root):
        if path.exists():
            saved = read_json(path)
            base = {
                k: v for k, v in saved.items() if k not in {"model", "rfm_k"}
            }
            if base != context:
                raise ValueError("Run inputs changed: create a new DagRun")
            context = saved
        elif kind == "daily":
            context["model"] = resolve_model(
                root,
                context["run_date"],
                version,
                context["source_hash"],
                context["requested_model"],
            )
        if kind == "daily":
            model = validate_binding(
                root,
                context["model"],
                context["run_date"],
                version,
                context["source_hash"],
            )
            if k is not None and k != model["k"]:
                raise ValueError("Daily rfm_k must match the pinned model")
            context["rfm_k"] = model["k"]
        else:
            context["rfm_k"] = k or 2
        atomic_json(path, context)
    return {"context_path": str(path), **context}


def load_context(path, root=None):
    root = data_root() if root is None else Path(root)
    path = safe_path(root, path)
    value = read_json(path)
    if path != context_file(root, value["run_key"]):
        raise ValueError("Unexpected context path")
    if value["code_version"] != code_version():
        raise ValueError("Business code changed; start a new DagRun")
    return value


def stage_file(root, context, stage):
    if stage not in {"etl", "rfm", "audit"}:
        raise ValueError("Unknown stage")
    return context_file(root, context["run_key"]).with_name(f"{stage}.json")


def load_stage(root, context, stage):
    result = read_json(stage_file(root, context, stage))
    if result.get("context_hash") != json_hash(context):
        raise ValueError(f"{stage} has mismatched input context")
    if not result.get("complete"):
        raise ValueError(f"{stage} is incomplete")
    for output in result["outputs"].values():
        if not (safe_path(root, output) / "_SUCCESS").is_file():
            raise ValueError(f"Incomplete Parquet output: {output}")
    return result


@contextmanager
def publication_lock(root, run_date):
    iso_date(run_date)
    path = safe_path(root, f"manifests/.publish-{run_date}.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.close(descriptor)
        yield
    finally:
        path.unlink()


def publish_completion(root, context):
    """Publish three fully written stages with consistent inputs."""
    stages = {s: load_stage(root, context, s) for s in ("etl", "rfm", "audit")}
    if context.get("kind") != "daily":
        raise ValueError("Only daily contexts may publish analytics")
    if stages["rfm"].get("model") != context["model"]:
        raise ValueError("RFM model does not match frozen context")
    etl_hash = json_hash(stages["etl"])
    if any(stages[s]["etl_hash"] != etl_hash for s in ("rfm", "audit")):
        raise ValueError(
            "Analytics branches consumed different ETL generations"
        )
    summary = {"context": context, "stages": stages, "complete": True}
    with publication_lock(root, context["run_date"]):
        completion = context_file(root, context["run_key"]).with_name(
            "completion.json"
        )
        atomic_json(completion, summary)
        atomic_json(
            safe_path(root, f"manifests/published/{context['run_date']}.json"),
            summary,
        )
    return summary


# RFM classifier: temporal validation, metrics and parameter selection.
def check_dates(train_date, validation_date, test_date, refit_date=None):
    values = [date.fromisoformat(v) for v in (train_date, validation_date, test_date)]
    if not values[0] < values[1] < values[2]:
        raise ValueError('Require train < validation < test cutoff')
    if refit_date is not None and date.fromisoformat(refit_date) < values[2]:
        raise ValueError('Refit cutoff must be at or after test cutoff')


def parameter_grid(trees, depths):
    if not trees or not depths:
        raise ValueError('RF grid must not be empty')
    if any(type(n) is not int or not 1 <= n <= 1000 for n in trees):
        raise ValueError('numTrees must be in [1,1000]')
    if any(type(n) is not int or not 1 <= n <= 30 for n in depths):
        raise ValueError('maxDepth must be in [1,30]')
    return [dict(num_trees=n, max_depth=d) for n in sorted(set(trees)) for d in sorted(set(depths))]


def confusion_metrics(cells, k):
    """Macro over union of actual/predicted labels; unused labels have F1=None."""
    if type(k) is not int or k < 2:
        raise ValueError('k must be >=2')
    counts = {}
    for row in cells:
        a, p, n = int(row['label']), int(row['predicted']), int(row['count'])
        if not (0 <= a < k and 0 <= p < k and n > 0):
            raise ValueError('Invalid confusion cell')
        counts[a, p] = counts.get((a, p), 0) + n
    total = sum(counts.values())
    classes = []
    for c in range(k):
        tp = counts.get((c, c), 0)
        actual = sum(n for (a, _), n in counts.items() if a == c)
        predicted = sum(n for (_, p), n in counts.items() if p == c)
        included = bool(actual or predicted)
        classes.append(dict(cluster=c, support=actual, predicted=predicted,
                            precision=tp / predicted if predicted else None,
                            recall=tp / actual if actual else None,
                            f1=2 * tp / (actual + predicted) if included else None,
                            included_in_macro=included))
    f1s = [c['f1'] for c in classes if c['included_in_macro']]
    return dict(customers=total, agreement=sum(n for (a, p), n in counts.items() if a == p) / total if total else None,
                macro_f1=sum(f1s) / len(f1s) if f1s else None,
                macro_labels=[c['cluster'] for c in classes if c['included_in_macro']],
                macro_definition='union_of_true_and_predicted_labels', per_class=classes,
                confusion=[dict(label=a, predicted=p, count=n) for (a, p), n in sorted(counts.items())])


def choose_candidate(candidates, minimum):
    if not math.isfinite(minimum) or not 0 <= minimum <= 1:
        raise ValueError('Validation threshold must be in [0,1]')
    eligible = [c for c in candidates if c['validation']['new']['macro_f1'] is not None]
    if not eligible:
        raise ValueError('No new validation customers; change split instead of falling back to seen customers')
    # Predeclared deterministic tie-break: smaller forest, then shallower trees.
    winner = min(eligible, key=lambda c: (-c['validation']['new']['macro_f1'], c['params']['num_trees'], c['params']['max_depth']))
    if winner['validation']['new']['macro_f1'] < minimum:
        raise ValueError('New-customer validation gate failed; no bundle released')
    return winner
