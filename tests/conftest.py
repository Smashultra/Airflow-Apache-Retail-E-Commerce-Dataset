"""Make the same mounted modules importable in host and Docker tests."""

import sys
import shutil
from pathlib import Path

import pytest

import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(__file__).resolve().parents[1]
for folder in (ROOT, ROOT / "scripts", ROOT / "dags"):
    sys.path.insert(0, str(folder))


@pytest.fixture
def model_factory():
    """Small registry fixture; Spark artifact behavior is tested separately."""
    from retail_contracts import atomic_json, json_hash, read_json, safe_path
    from retail_models import FEATURE_VERSION

    def create(
        root, source, version="v1", cutoff="2011-12-08", effective="2011-12-09"
    ):
        path = f"models/rfm/version={version}/model.json"
        model = {
            "complete": True,
            "model_version": version,
            "k": 2,
            "feature_version": FEATURE_VERSION,
            "source_version": source["source_version"],
            "source_hash": json_hash(source),
            "training_cutoff": cutoff,
            "pipeline_path": f"models/rfm/version={version}/pipeline",
        }
        atomic_json(safe_path(root, path), model)
        registry_path = safe_path(root, "manifests/models/registry.json")
        registry = (
            read_json(registry_path)
            if registry_path.exists()
            else {"activations": []}
        )
        binding = {
            "model_version": version,
            "model_manifest": path,
            "model_manifest_hash": json_hash(model),
            "effective_from": effective,
            "sequence": len(registry["activations"]) + 1,
        }
        registry["activations"].append(binding)
        atomic_json(registry_path, registry)
        return binding

    return create


@pytest.fixture
def rfm_ready(tmp_path, monkeypatch):
    """Real small manifests/markers with no audit or completion yet."""
    from retail_contracts import (
        atomic_json,
        code_version,
        context_file,
        json_hash,
        stage_file,
    )

    monkeypatch.setenv("RETAIL_DATA_ROOT", str(tmp_path))
    context = {
        "kind": "daily",
        "dag_id": "ecommerce_etl_dag",
        "run_id": "early",
        "run_key": json_hash(["ecommerce_etl_dag", "early"]),
        "run_date": "2011-12-10",
        "code_version": code_version(),
        "source_version": "fixture",
        "source_hash": "source",
        "model": {"model_version": "v1", "model_manifest_hash": "model"},
    }
    atomic_json(context_file(tmp_path, context["run_key"]), context)
    stages = {}
    for stage, names in {
        "etl": ["transactions", "audit_input", "parse_errors"],
        "rfm": ["customers", "cluster_profile"],
    }.items():
        outputs = {}
        for name in names:
            relative = f"fixture/{stage}/version=one/{name}"
            output = tmp_path / relative
            output.mkdir(parents=True)
            (output / "_SUCCESS").touch()
            outputs[name] = relative
        value = {
            "stage": stage,
            "complete": True,
            "outputs": outputs,
            "context_hash": json_hash(context),
            "metrics": {"rows": 2},
            "etl_hash": json_hash(stages["etl"]) if stages else None,
        }
        if stage == "rfm":
            value["model"] = context["model"]
        stages[stage] = value
        atomic_json(stage_file(tmp_path, context, stage), value)
    return tmp_path, context, stages


@pytest.fixture
def analytics_ready(rfm_ready):
    from retail_contracts import atomic_json, json_hash, stage_file

    root, context, stages = rfm_ready
    outputs = {}
    for name in ("row_assessments", "order_assessments", "flagged_orders"):
        path = root / "fixture/audit/version=one" / name
        path.mkdir(parents=True)
        (path / "_SUCCESS").touch()
        outputs[name] = path.relative_to(root).as_posix()
    stages["audit"] = {
        "stage": "audit",
        "complete": True,
        "outputs": outputs,
        "context_hash": json_hash(context),
        "etl_hash": json_hash(stages["etl"]),
        "metrics": {"assessed_rows": 2, "orders": 1},
    }
    atomic_json(stage_file(root, context, "audit"), stages["audit"])
    return root, context, stages

@pytest.fixture(scope="session")
def spark():
    """Shared local Spark session for PySpark tests."""
    from pyspark.sql import SparkSession

    # 1) Dùng jar Spark đi kèm pip, bỏ qua bản Spark cài riêng.
    os.environ.pop("SPARK_HOME", None)
    # 2) Spark 4 cần Java 17+: lấy JDK của lệnh `java` đang có trên PATH.
    java = shutil.which("java")
    if java:
        home = Path(java).resolve().parent.parent
        if (home / "bin").is_dir() and (home / "lib").is_dir():
            os.environ["JAVA_HOME"] = str(home)
    # 3) Python worker phải là Python đang chạy pytest, không phải lối tắt Store.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("retail-project-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()

@pytest.fixture
def script_env():
    """Môi trường cho test chạy script bằng subprocess."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "scripts"), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    return env