"""Listener preflight/configuration without starting a JVM."""

from pathlib import Path
from io import BytesIO

import pytest
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

import retail_spark_lineage as config
from retail_support import spark_lineage as integration
from retail_support.lineage import RetailExtractor


def test_disabled_needs_no_jar(monkeypatch):
    monkeypatch.delenv("RETAIL_SPARK_LINEAGE_ENABLED", raising=False)
    monkeypatch.setattr(
        integration, "verify_jar", lambda: pytest.fail("read JAR")
    )
    monkeypatch.setattr(SparkSubmitOperator, "execute", lambda self, ctx: "ok")
    task = integration.LineageSparkSubmitOperator(task_id="fixture")
    assert task.execute({}) == "ok"
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "true")
    assert task.execute({}) == "ok"


def test_invalid_flag_and_checksum(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_ENABLED", "tru")
    with pytest.raises(ValueError, match="true or false"):
        config.enabled()
    jar = tmp_path / "bad.jar"
    jar.write_bytes(b"not a jar")
    with pytest.raises(ValueError, match="checksum"):
        config.verify_jar(jar)
    with pytest.raises(FileNotFoundError):
        config.verify_jar(tmp_path / "missing.jar")


def test_enabled_preserves_conf_and_provider_parent(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "false")
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_MARQUEZ", "true")
    monkeypatch.setattr(integration, "verify_jar", lambda: None)
    monkeypatch.setattr(integration, "version", lambda name: "4.2.0")
    monkeypatch.setattr(integration, "Path", lambda value: tmp_path)
    captured = []

    def submit(self, context):
        captured.append(dict(self.conf))
        assert self._openlineage_inject_parent_job_info is True
        assert self._openlineage_inject_transport_info is False
        assert self.jars.split(",") == ["existing.jar", str(config.JAR_PATH)]

    monkeypatch.setattr(SparkSubmitOperator, "execute", submit)
    task = integration.LineageSparkSubmitOperator(
        task_id="fixture",
        jars="existing.jar",
        conf={
            "spark.extraListeners": "custom.Listener",
            "spark.sql.session.timeZone": "UTC",
        },
    )
    task.execute({})
    task.execute({})
    assert captured[0]["spark.sql.session.timeZone"] == "UTC"
    assert (
        captured[0]["spark.extraListeners"]
        == f"custom.Listener,{config.LISTENER}"
    )
    key = "spark.openlineage.transport.transports.local.location"
    assert captured[0][key] != captured[1][key]
    assert (
        "LineageSparkSubmitOperator"
        in RetailExtractor.get_operator_classnames()
    )


def test_preflight_failure_never_submits(monkeypatch):
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "false")
    monkeypatch.setattr(integration, "version", lambda name: "4.2.0")
    monkeypatch.setattr(
        SparkSubmitOperator,
        "execute",
        lambda *args: pytest.fail("submitted corrupt JAR"),
    )

    def corrupt():
        raise ValueError("checksum")

    monkeypatch.setattr(integration, "verify_jar", corrupt)
    task = integration.LineageSparkSubmitOperator(task_id="fixture")
    with pytest.raises(ValueError, match="checksum"):
        task.execute({})


def test_offline_file_transport():
    conf = config.listener_conf(Path("/tmp/events.jsonl"))
    assert conf["spark.openlineage.transport.type"] == "file"
    assert not any(key.endswith("url") for key in conf)


def test_download_rejects_corrupt_bytes_and_cleans_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(
        config.urllib.request, "urlopen", lambda *a, **kw: BytesIO(b"corrupt")
    )
    with pytest.raises(ValueError, match="checksum"):
        config.install_jar(tmp_path / "listener.jar")
    assert list(tmp_path.iterdir()) == []


def test_unverified_spark_upgrade_is_rejected(monkeypatch):
    monkeypatch.setenv("RETAIL_SPARK_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "false")
    monkeypatch.setattr(integration, "version", lambda name: "9.0.0")
    monkeypatch.setattr(
        SparkSubmitOperator,
        "execute",
        lambda *args: pytest.fail("submitted unverified Spark"),
    )
    with pytest.raises(ValueError, match="upgrading Spark"):
        integration.LineageSparkSubmitOperator(task_id="fixture").execute({})
