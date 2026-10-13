"""Pinned offline Spark listener configuration; no PySpark import."""

import hashlib
import os
import tempfile
import urllib.request
from pathlib import Path

VERSION = "1.52.0"
JAR_NAME = f"openlineage-spark_2.13-{VERSION}.jar"
JAR_PATH = Path("/opt/airflow/data/lineage/jars") / JAR_NAME
JAR_URL = (
    "https://repo.maven.apache.org/maven2/io/openlineage/"
    f"openlineage-spark_2.13/{VERSION}/{JAR_NAME}"
)
SHA512 = (
    "a8ae09db698aab723fe33f4a56e3a63026938301f769d15883197987a164fefd42d9ade"
    "c1e8c3c28e647ca682ec5db3ba1e063b757f1ab4b5b82ddb70b24ac10"
)
LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"


def flag(name, default="false"):
    value = os.environ.get(name, default).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


def enabled():
    return flag("RETAIL_SPARK_LINEAGE_ENABLED") and not flag(
        "AIRFLOW__OPENLINEAGE__DISABLED"
    )


def verify_jar(path=JAR_PATH):
    with Path(path).open("rb") as source:
        digest = hashlib.file_digest(source, "sha512").hexdigest()
    if digest != SHA512:
        raise ValueError(f"OpenLineage JAR checksum mismatch: {path}")


def install_jar(path=JAR_PATH):
    """Explicit one-time download; never called from a task or DAG parse."""
    path = Path(path)
    if path.exists():
        verify_jar(path)
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, suffix=".part", delete=False
        ) as target:
            temporary = Path(target.name)
            with urllib.request.urlopen(JAR_URL, timeout=60) as source:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
        verify_jar(temporary)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


def listener_conf(location, *, marquez=False):
    conf = {
        "spark.extraListeners": LISTENER,
        "spark.openlineage.namespace": "retail-spark",
        "spark.openlineage.transport.type": "file",
        "spark.openlineage.transport.location": str(location),
        # Do not embed a potentially large SQL plan in every event.
        "spark.openlineage.facets.spark_logicalPlan.disabled": "true",
    }
    if marquez:
        conf.pop("spark.openlineage.transport.location")
        prefix = "spark.openlineage.transport."
        conf.update(
            {
                prefix + "type": "composite",
                prefix + "continueOnFailure": "true",
                prefix + "transports.local.type": "file",
                prefix + "transports.local.location": str(location),
                prefix + "transports.marquez.type": "http",
                prefix + "transports.marquez.url": "http://marquez:5000",
                prefix + "transports.marquez.endpoint": "/api/v1/lineage",
                prefix + "transports.marquez.timeoutInMillis": "3000",
            }
        )
    return conf


if __name__ == "__main__":
    print(f"Verified listener: {install_jar()}")
