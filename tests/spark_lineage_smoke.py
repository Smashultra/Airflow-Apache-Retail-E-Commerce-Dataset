"""Real tiny Spark fixture; never reads or publishes retail data.

Run in the scheduler container. --marquez checks HTTP delivery as well;
--broken-http checks computation/file capture with an unavailable HTTP sink.
"""

import argparse
import json
import subprocess
import urllib.request
from pathlib import Path
from uuid import uuid4

from retail_spark_lineage import JAR_PATH, listener_conf, verify_jar


def transform(folder):
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.appName(
        "retail-spark-lineage-fixture"
    ).getOrCreate()
    try:
        source = (
            spark.read.schema("customer string, qty int, price double")
            .option("header", True)
            .csv(str(folder / "input.csv"))
        )
        result = (
            source.filter(F.col("qty") > 0)
            .groupBy("customer")
            .agg(F.sum(F.col("qty") * F.col("price")).alias("revenue"))
        )
        result.write.mode("error").parquet(str(folder / "output"))
        rows = {row.customer: row.revenue for row in result.collect()}
        assert rows == {"A": 7.0, "B": 6.0}, rows
    finally:
        spark.stop()


def smoke(*, marquez=False, broken_http=False):
    verify_jar()
    folder = Path("/opt/airflow/logs/spark_lineage_fixture") / str(uuid4())
    folder.mkdir(parents=True)
    (folder / "input.csv").write_text(
        "customer,qty,price\nA,2,2.0\nA,1,3.0\nB,3,2.0\nB,-1,99.0\n"
    )
    events_path = folder / "events.jsonl"
    parent_id = str(uuid4())
    conf = listener_conf(events_path, marquez=marquez or broken_http)
    conf.update(
        {
            "spark.openlineage.parentJobNamespace": "retail-local",
            "spark.openlineage.parentJobName": "spark_fixture.transform",
            "spark.openlineage.parentRunId": parent_id,
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "1",
            "spark.sql.session.timeZone": "UTC",
        }
    )
    if broken_http:
        conf["spark.openlineage.transport.transports.marquez.url"] = (
            "http://127.0.0.1:1"
        )
        conf[
            "spark.openlineage.transport.transports.marquez.timeoutInMillis"
        ] = "200"
    command = [
        "spark-submit",
        "--master",
        "local[1]",
        "--driver-memory",
        "768m",
        "--driver-class-path",
        str(JAR_PATH),
        "--jars",
        str(JAR_PATH),
    ]
    for key, value in conf.items():
        command += ["--conf", f"{key}={value}"]
    command += [str(Path(__file__).resolve()), "--transform", str(folder)]
    with (folder / "spark.log").open("w") as log:
        subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=180,
        )
    events = [
        json.loads(line) for line in events_path.read_text().splitlines()
    ]
    completed = [
        event for event in events if event.get("eventType") == "COMPLETE"
    ]
    writes = [
        event
        for event in completed
        if any(
            dataset["name"].endswith(str(folder / "output"))
            for dataset in event.get("outputs", [])
        )
    ]
    assert writes, "No completed Spark write event"
    write = writes[-1]
    assert any(
        dataset["name"].endswith(str(folder / "input.csv"))
        for dataset in write["inputs"]
    ), write
    assert any(
        event.get("eventType") == "START"
        and event["run"]["runId"] == write["run"]["runId"]
        for event in events
    ), "Missing matching START"
    # SQL runs may point to the Spark application, then to the supplied parent.
    assert any(
        event["run"]
        .get("facets", {})
        .get("parent", {})
        .get("run", {})
        .get("runId")
        == parent_id
        for event in events
    ), "Missing injected parent"
    facets = write["outputs"][0].get("facets", {})
    assert "schema" in facets, facets
    assert "columnLineage" in facets, facets
    fields = facets["columnLineage"]["fields"]
    assert {item["field"] for item in fields["revenue"]["inputFields"]} == {
        "qty",
        "price",
    }, fields
    assert {item["field"] for item in fields["customer"]["inputFields"]} == {
        "customer"
    }, fields
    if marquez:
        url = "http://marquez:5000/api/v1/jobs/runs/" + write["run"]["runId"]
        with urllib.request.urlopen(url, timeout=10) as response:
            remote = json.load(response)
        assert remote["state"] == "COMPLETED", remote
    evidence = {
        "verified": True,
        "fixture_only": True,
        "spark": "4.2.0",
        "listener": "1.52.0",
        "events": len(events),
        "parent_run_id": parent_id,
        "write_run_id": write["run"]["runId"],
        "column_lineage": facets["columnLineage"],
        "marquez": marquez,
        "broken_http": broken_http,
    }
    (folder / "verification.json").write_text(json.dumps(evidence, indent=2))
    print(json.dumps({"verified": True, "evidence": str(folder)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transform", type=Path)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--marquez", action="store_true")
    modes.add_argument("--broken-http", action="store_true")
    args = parser.parse_args()
    if args.transform:
        transform(args.transform)
    else:
        smoke(marquez=args.marquez, broken_http=args.broken_http)
