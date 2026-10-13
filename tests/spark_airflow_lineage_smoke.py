"""Isolated dag.test: match Spark parent UUID to the actual Airflow event."""

import json
import os
from pathlib import Path

import pendulum
from airflow.sdk import DAG

from retail_support.spark_lineage import LineageSparkSubmitOperator


class SparkFixtureOperator(LineageSparkSubmitOperator):
    def get_openlineage_facets_on_start(self):
        from airflow.providers.openlineage.extractors import OperatorLineage

        return OperatorLineage()

    def get_openlineage_facets_on_complete(self, task_instance):
        return self.get_openlineage_facets_on_start()

    def get_openlineage_facets_on_failure(self, task_instance):
        return self.get_openlineage_facets_on_start()


with DAG(
    dag_id="retail_spark_lineage_smoke",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 13, tz="UTC"),
    is_paused_upon_creation=True,
) as dag:
    SparkFixtureOperator(
        task_id="transform",
        application="/opt/airflow/tests/spark_lineage_smoke.py",
        application_args=[
            "--transform",
            os.environ.get("RETAIL_SPARK_FIXTURE_DIR", "/unused"),
        ],
        conn_id="spark_lineage_fixture",
        driver_memory="768m",
        conf={
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "1",
        },
        durable=False,
        do_xcom_push=False,
    )


if __name__ == "__main__":
    import runpy
    import shutil
    from uuid import uuid4

    folder = Path("/opt/airflow/logs/spark_airflow_fixture") / str(uuid4())
    bundle = folder / "bundle"
    bundle.mkdir(parents=True)
    (folder / "input.csv").write_text(
        "customer,qty,price\nA,2,2.0\nA,1,3.0\nB,3,2.0\nB,-1,99.0\n"
    )
    os.environ.update(
        {
            "RETAIL_SPARK_FIXTURE_DIR": str(folder),
            "RETAIL_SPARK_LINEAGE_ENABLED": "true",
            "RETAIL_SPARK_LINEAGE_MARQUEZ": "false",
            "AIRFLOW__OPENLINEAGE__DISABLED": "false",
            "AIRFLOW_CONN_SPARK_LINEAGE_FIXTURE": json.dumps(
                {
                    "conn_type": "spark",
                    "host": "local[1]",
                }
            ),
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
                [
                    {
                        "name": "retail-spark-lineage-fixture",
                        "classpath": (
                            "airflow.dag_processing.bundles.local."
                            "LocalDagBundle"
                        ),
                        "kwargs": {"path": str(bundle)},
                    }
                ]
            ),
        }
    )
    target = bundle / Path(__file__).name
    shutil.copyfile(__file__, target)
    fixture = runpy.run_path(str(target), run_name="spark_lineage_fixture")
    run = fixture["dag"].test()
    assert run.state == "success", run.state
    airflow_events = []
    for path in Path("/opt/airflow/logs").glob("openlineage-*.json"):
        event = json.loads(path.read_text())
        if (
            event.get("job", {}).get("name")
            == ("retail_spark_lineage_smoke.transform")
            and event.get("eventType") == "COMPLETE"
        ):
            airflow_events.append(event)
    parent = max(airflow_events, key=lambda event: event["eventTime"])
    matched = []
    for path in Path("/opt/airflow/logs/spark_openlineage").glob("*.jsonl"):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            facet = event["run"].get("facets", {}).get("parent", {})
            if facet.get("run", {}).get("runId") == parent["run"][
                "runId"
            ] and all(
                facet.get("job", {}).get(key) == parent["job"][key]
                for key in ("namespace", "name")
            ):
                matched.append(event)
    assert {"START", "COMPLETE"} <= {
        event["eventType"] for event in matched
    }, "Spark application parent did not match actual Airflow task event"
    report = {
        "verified": True,
        "fixture_only": True,
        "airflow_task_run_id": parent["run"]["runId"],
        "spark_parent_events": len(matched),
    }
    (folder / "verification.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"verified": True, "evidence": str(folder)}))
