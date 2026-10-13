"""Airflow-level lineage from small manifests; no Spark or Parquet reads."""

import logging

from attrs import define
from airflow.providers.openlineage.extractors.base import (
    BaseExtractor,
    OperatorLineage,
)
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import DatasetFacet, RunFacet

from retail_contracts import (
    context_file,
    data_root,
    json_hash,
    load_stage,
    read_json,
    safe_path,
)
from retail_support.publication import branch_publication_path

LOGGER = logging.getLogger(__name__)
SCHEMA = (
    "file:///opt/airflow/dags/retail_support/schemas/retail-lineage-v1.json"
)
STAGES = {
    "submit_pyspark_etl": "etl",
    "compute_rfm_metrics": "rfm",
    "detect_anomalies": "audit",
}
PATHS = {
    "etl": "curated",
    "rfm": "analytics/rfm_daily",
    "audit": "audit/anomalies",
}
PUBLISHED = {
    "etl": ["transactions"],
    "rfm": ["customers"],
    "audit": ["flagged_orders"],
}
CONSUMERS = {
    "retail_marketing_consumer_dag": [("rfm", "customers")],
    "retail_executive_dashboard_dag": [],
}


@define
class RetailRunFacet(RunFacet):
    metadata: dict

    @staticmethod
    def _get_schema():
        return SCHEMA + "#/$defs/RetailRunFacet"


@define
class RetailDatasetFacet(DatasetFacet):
    metadata: dict

    @staticmethod
    def _get_schema():
        return SCHEMA + "#/$defs/RetailDatasetFacet"


def dataset(relative, physical=None):
    """Stable logical name; exact generation belongs in the dataset facet."""
    name = str(safe_path(data_root(), relative))
    facets = {}
    if physical is not None:
        facets["retail_snapshot"] = RetailDatasetFacet(
            {"physical_path": str(safe_path(data_root(), physical))}
        )
    return Dataset(namespace="file://", name=name, facets=facets)


def stage_dataset(stage, name, physical=None, published=False):
    prefix = "" if published else "staged/"
    return dataset(f"{prefix}{PATHS[stage]}/{name}", physical)


def consumer_lineage(dag_id):
    if dag_id == "retail_executive_dashboard_dag":
        return OperatorLineage(inputs=[dataset("analytics/retail_snapshot")])
    return OperatorLineage(
        inputs=[
            stage_dataset(stage, name, published=True)
            for stage, name in CONSUMERS.get(dag_id, [])
        ]
    )


def completed_lineage(ti):
    """Use the current run's manifests, never the mutable date pointer."""
    if ti.dag_id in CONSUMERS:
        return consumer_lineage(ti.dag_id)
    if ti.dag_id == "ecommerce_etl_dag_allinone":
        if ti.task_id == "execute_voucher_campaign":
            return consumer_lineage("retail_marketing_consumer_dag")
        if ti.task_id == "generate_executive_summary":
            return consumer_lineage("retail_executive_dashboard_dag")
    if ti.dag_id not in {
        "ecommerce_etl_dag", "ecommerce_etl_dag_allinone",
        "retail_rfm_training_dag",
    }:
        return OperatorLineage()
    root = data_root()
    path = context_file(root, json_hash([ti.dag_id, ti.run_id]))
    context = read_json(path)
    if (context["dag_id"], context["run_id"]) != (ti.dag_id, ti.run_id):
        raise ValueError("Lineage context identity mismatch")
    metadata = {
        key: context[key]
        for key in (
            "run_key",
            "run_date",
            "code_version",
            "source_version",
            "source_hash",
        )
    }
    metadata["context_hash"] = json_hash(context)
    metadata["publication_state"] = "not_published"
    inputs, outputs = [], []
    task = ti.task_id
    if task == "validate_raw_data":
        inputs = [dataset("raw/data.csv")]
        outputs = [dataset(f"raw/landing/{context['source_version']}")]
    elif task in STAGES:
        stage = STAGES[task]
        manifest = load_stage(root, context, stage)
        metadata.update(
            etl_hash=manifest["etl_hash"], metrics=manifest["metrics"]
        )
        metadata["publication_state"] = "computed"
        if stage == "etl":
            inputs = [dataset(f"raw/landing/{context['source_version']}")]
        else:
            etl = load_stage(root, context, "etl")
            if manifest["etl_hash"] != json_hash(etl):
                raise ValueError("Lineage ETL generation mismatch")
            source = "transactions" if stage == "rfm" else "audit_input"
            inputs = [stage_dataset("etl", source, etl["outputs"][source])]
            if stage == "rfm":
                from retail_models import read_model

                model = read_model(root, context["model"])
                inputs.append(dataset(model["pipeline_path"]))
                metadata["model_version"] = model["model_version"]
        outputs = [
            stage_dataset(stage, name, physical)
            for name, physical in manifest["outputs"].items()
        ]
        if stage in {"rfm", "audit"}:
            marker = branch_publication_path(root, context, stage)
            published = read_json(marker)
            if (
                not published.get("complete")
                or published["context"] != context
                or published["stages"] != {"etl": etl, stage: manifest}
                or (stage == "rfm" and manifest["model"] != context["model"])
            ):
                raise ValueError("No matching branch publication")
            outputs = [
                stage_dataset(stage, name, physical, name in PUBLISHED[stage])
                for name, physical in manifest["outputs"].items()
            ]
            metadata.update(
                publication_state=f"{stage}_published",
                **{
                    f"{stage}_hash": json_hash(manifest),
                    f"{stage}_manifest": str(marker),
                },
            )
    elif task == "notify_completion":
        summary = read_json(path.with_name("completion.json"))
        if not summary.get("complete") or summary["context"] != context:
            raise ValueError("No matching published completion")
        stages = summary["stages"]
        if any(
            s["context_hash"] != json_hash(context) or not s["complete"]
            for s in stages.values()
        ):
            raise ValueError("Publication lineage context mismatch")
        if any(
            stages[s]["etl_hash"] != json_hash(stages["etl"])
            for s in ("rfm", "audit")
        ):
            raise ValueError("Publication lineage ETL mismatch")
        if stages["rfm"]["model"] != context["model"]:
            raise ValueError("Publication lineage model mismatch")
        for branch in ("rfm", "audit"):
            early = read_json(branch_publication_path(root, context, branch))
            if (
                not early.get("complete")
                or early["context"] != context
                or early["stages"]
                != {name: stages[name] for name in ("etl", branch)}
            ):
                raise ValueError(
                    "Final lineage differs from branch publication"
                )
        for stage, names in PUBLISHED.items():
            for name in names:
                physical = stages[stage]["outputs"][name]
                inputs.append(
                    stage_dataset(stage, name, physical, stage != "etl")
                )
                if stage == "etl":
                    outputs.append(stage_dataset(stage, name, physical, True))
        outputs.append(
            dataset(
                "analytics/retail_snapshot",
                str(path.with_name("completion.json")),
            )
        )
        metadata.update(
            publication_state="published",
            etl_hash=json_hash(stages["etl"]),
            model_version=context["model"]["model_version"],
            completion_manifest=str(path.with_name("completion.json")),
            metrics={s: stages[s]["metrics"] for s in stages},
        )
    elif task in {
        "validate_training_input",
        "train_rfm_model",
        "publish_model",
    }:
        from retail_models import read_model, read_training_etl

        etl, _ = read_training_etl(root, context["etl_manifest"])
        if json_hash(etl) != context["etl_hash"]:
            raise ValueError("Training ETL lineage mismatch")
        inputs = [
            stage_dataset(
                "etl", "transactions", etl["outputs"]["transactions"]
            )
        ]
        metadata["etl_hash"] = context["etl_hash"]
        if task != "validate_training_input":
            candidate = read_json(path.with_name("training.json"))
            if candidate["context_hash"] != json_hash(context):
                raise ValueError("Training candidate lineage mismatch")
            model = read_model(root, candidate)
            metadata["model_version"] = model["model_version"]
            outputs = [dataset(candidate["model_manifest"])]
            if task == "publish_model":
                registry = read_json(root / "manifests/models/registry.json")
                if not any(
                    e["model_manifest_hash"]
                    == candidate["model_manifest_hash"]
                    and e["model_version"] == model["model_version"]
                    for e in registry["activations"]
                ):
                    raise ValueError("Model not activated")
                inputs = outputs
                outputs = [dataset(model["pipeline_path"])]
                metadata["publication_state"] = "model_activated"
    return OperatorLineage(
        inputs=inputs,
        outputs=outputs,
        run_facets={
            "retail_provenance": RetailRunFacet(metadata),
        },
    )


class RetailExtractor(BaseExtractor):
    """Provider handles event IDs/timestamps/transport and task lifecycle."""

    @classmethod
    def get_operator_classnames(cls):
        return [
            "SparkSubmitOperator",
            "LineageSparkSubmitOperator",
            "RfmSparkSubmitOperator",
            "AuditSparkSubmitOperator",
            "PythonOperator",
            "LandingValidationSensor",
        ]

    def _execute_extraction(self):
        # START must not advertise output, including a previous retry's output.
        # Nonempty real inputs prevent provider fallback to declared outlets.
        if (
            self.operator.dag_id in {
                "ecommerce_etl_dag", "ecommerce_etl_dag_allinone"
            }
            and self.operator.task_id == "notify_completion"
        ):
            return OperatorLineage(
                inputs=[
                    stage_dataset(stage, name, published=stage != "etl")
                    for stage, names in PUBLISHED.items()
                    for name in names
                ]
            )
        if (
            self.operator.dag_id in {
                "ecommerce_etl_dag", "ecommerce_etl_dag_allinone"
            }
            and self.operator.task_id
            in {"compute_rfm_metrics", "detect_anomalies"}
        ):
            return OperatorLineage(
                inputs=[
                    stage_dataset(
                        "etl",
                        "transactions"
                        if self.operator.task_id == "compute_rfm_metrics"
                        else "audit_input",
                    )
                ]
            )
        if self.operator.dag_id == "ecommerce_etl_dag_allinone":
            consumer = {
                "execute_voucher_campaign": "retail_marketing_consumer_dag",
                "generate_executive_summary": "retail_executive_dashboard_dag",
            }.get(self.operator.task_id)
            return consumer_lineage(consumer)
        return consumer_lineage(self.operator.dag_id)

    def extract_on_complete(self, task_instance):
        try:
            return completed_lineage(task_instance)
        except Exception:
            # Extraction errors must not fail a successful business task.
            LOGGER.warning("Retail lineage extraction failed", exc_info=True)
            return self._execute_extraction()

    def extract_on_failure(self, task_instance):
        # Never delegate failure to completion: a stale manifest can exist.
        return self._execute_extraction()
