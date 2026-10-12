"""Publish each frozen analytics snapshot independently of the other branch."""

from retail_contracts import (
    atomic_json,
    context_file,
    json_hash,
    load_stage,
    publication_lock,
    read_json,
)


def branch_publication_path(root, context, stage):
    if stage not in {"rfm", "audit"}:
        raise ValueError("Unknown analytics branch")
    return context_file(root, context["run_key"]).with_name(
        f"{stage}_publication.json"
    )


def branch_publication(root, context, stage, *, publish=False):
    """Validate ETL/model/output gates; never replace an advertised snapshot.

    The marker survives a task retry between publication and Asset emission.
    A cleared task reuses it; changed upstream generations require a new run.
    Only small JSON manifests and Parquet success markers are inspected.
    """
    if context.get("kind") != "daily":
        raise ValueError("Only daily contexts may publish analytics")
    path = branch_publication_path(root, context, stage)
    stages = {name: load_stage(root, context, name) for name in ("etl", stage)}
    etl, branch = stages["etl"], stages[stage]
    if branch.get("etl_hash") != json_hash(etl):
        raise ValueError(f"{stage} consumed a different ETL generation")
    if stage == "rfm" and branch.get("model") != context["model"]:
        raise ValueError("RFM model does not match frozen context")
    required = (
        {"customers", "cluster_profile"}
        if stage == "rfm"
        else {"row_assessments", "order_assessments", "flagged_orders"}
    )
    source = "transactions" if stage == "rfm" else "audit_input"
    if (
        source not in etl["outputs"]
        or not required <= branch["outputs"].keys()
    ):
        raise ValueError(f"Missing required {stage} publication outputs")
    summary = {"context": context, "stages": stages, "complete": True}
    with publication_lock(root, context["run_date"]):
        if path.exists():
            if read_json(path) != summary:
                raise ValueError(
                    f"Published {stage} changed; start a new DagRun"
                )
        elif publish:
            atomic_json(path, summary)
        else:
            raise ValueError(f"{stage} has not been published")
    return summary


def rfm_publication_path(root, context):
    return branch_publication_path(root, context, "rfm")


def rfm_publication(root, context, *, publish=False):
    return branch_publication(root, context, "rfm", publish=publish)


def branch_event_metadata(root, summary, stage):
    if stage == "rfm":
        return rfm_event_metadata(root, summary)
    context, stages = summary["context"], summary["stages"]
    return {
        "publication_version": 3,
        "publication_kind": "audit",
        "run_date": context["run_date"],
        "run_key": context["run_key"],
        "source_dag_id": context["dag_id"],
        "source_run_id": context["run_id"],
        "context_hash": json_hash(context),
        "etl_hash": json_hash(stages["etl"]),
        "audit_hash": json_hash(stages["audit"]),
        "audit_manifest": str(branch_publication_path(root, context, "audit")),
    }


def rfm_event_metadata(root, summary):
    """RFM metadata retained for Marketing and aggregate snapshot identity."""
    context, stages = summary["context"], summary["stages"]
    return {
        "publication_version": 2,
        "run_date": context["run_date"],
        "source_dag_id": context["dag_id"],
        "source_run_id": context["run_id"],
        "run_key": context["run_key"],
        "context_hash": json_hash(context),
        "etl_hash": json_hash(stages["etl"]),
        "rfm_hash": json_hash(stages["rfm"]),
        "rfm_manifest": str(rfm_publication_path(root, context)),
    }
