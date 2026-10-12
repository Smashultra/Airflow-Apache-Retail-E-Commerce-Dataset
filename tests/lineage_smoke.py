"""One-task provider smoke test; delivery uses configured lineage transport."""

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def no_work():
    return "OpenLineage lifecycle smoke only"


class LineageSmokeOperator(PythonOperator):
    def get_openlineage_facets_on_start(self):
        from airflow.providers.openlineage.extractors import OperatorLineage

        return OperatorLineage()

    def get_openlineage_facets_on_complete(self, task_instance):
        from airflow.providers.openlineage.extractors import OperatorLineage
        from openlineage.client.event_v2 import Dataset

        from retail_support.lineage import RetailRunFacet

        return OperatorLineage(
            inputs=[Dataset(namespace="retail-fixture", name="input")],
            outputs=[Dataset(namespace="retail-fixture", name="output")],
            run_facets={
                "retail_provenance": RetailRunFacet({"fixture": True}),
            },
        )

    def get_openlineage_facets_on_failure(self, task_instance):
        return self.get_openlineage_facets_on_start()


with DAG(
    dag_id="retail_lineage_smoke",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 11, tz="UTC"),
    is_paused_upon_creation=True,
) as dag:
    LineageSmokeOperator(task_id="metadata_only", python_callable=no_work)

if __name__ == "__main__":
    # Airflow 3.3 dag.test syncs its configured bundle before executing.
    # Isolate the fixture so no production DAG is serialized by this test.
    import json
    import os
    import runpy
    import shutil
    from pathlib import Path

    folder = Path("/opt/airflow/logs/lineage_fixture")
    folder.mkdir(exist_ok=True)
    target = folder / "lineage_smoke.py"
    shutil.copyfile(__file__, target)
    os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps([
        {"name": "retail-lineage-fixture",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": str(folder)}},
    ])
    fixture = runpy.run_path(str(target), run_name="lineage_fixture")
    run = fixture["dag"].test()
    assert run.state == "success", run.state
