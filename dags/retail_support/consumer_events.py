"""Resolve triggering publications without guessing dates or latest paths."""

IDENTITY = ("run_date", "source_dag_id", "source_run_id", "run_key")
SNAPSHOT = ("context_hash", "etl_hash", "rfm_hash", "rfm_manifest")
COMPLETION = ("published_pointer", "completion_manifest")


def event_metadata(extra, uri):
    """Keep legacy queued events readable, without mixing contract versions."""
    if not isinstance(extra, dict):
        raise ValueError(f"Incomplete publication metadata: {uri}")
    version = extra.get("publication_version", 1)
    if version not in (1, 2, 3):
        raise ValueError("Unsupported publication metadata version")
    required = IDENTITY + (SNAPSHOT if version == 2 else COMPLETION)
    if version == 3:
        if extra.get("publication_kind") != "snapshot":
            raise ValueError("Expected aggregate snapshot event")
        required = (
            IDENTITY
            + SNAPSHOT
            + COMPLETION
            + (
                "audit_hash",
                "audit_manifest",
                "completion_hash",
            )
        )
    if version == 2 and any(key in extra for key in COMPLETION):
        required += COMPLETION
    if any(
        not isinstance(extra.get(key), str) or not extra[key]
        for key in required
    ):
        raise ValueError(f"Incomplete publication metadata: {uri}")
    result = {key: extra[key] for key in required}
    if version in (2, 3):
        result["publication_version"] = version
    if version == 3:
        result["publication_kind"] = "snapshot"
    return result


def publication_events(triggering_events, assets):
    """Read one readiness stream; retain runs and deduplicate retry events."""
    if len(assets) != 1:
        raise ValueError("Consumers require one readiness Asset")
    uri = assets[0].uri
    by_uri = {
        getattr(key, "uri", key): events
        for key, events in (triggering_events or {}).items()
    }
    events = by_uri.get(uri, [])
    if not events:
        raise ValueError(f"Missing triggering events for {uri}")
    indexed = {}
    for event in events:
        value = event_metadata(event.extra, uri)
        identity = (value["run_date"], value["run_key"])
        if identity in indexed and indexed[identity] != value:
            raise ValueError("Conflicting publication metadata")
        indexed[identity] = value
    return [indexed[key] for key in sorted(indexed)]


def snapshot_events(triggering_events, asset):
    """Resolve the exact immutable completion, not a mutable date pointer."""
    from retail_contracts import (
        context_file,
        data_root,
        json_hash,
        read_json,
        safe_path,
    )
    from retail_support.publication import branch_publication_path

    publications = publication_events(triggering_events, [asset])
    for extra in publications:
        if extra.get("publication_version") != 3:
            raise ValueError("Dashboard requires an aggregate snapshot event")
        root = data_root()
        path = safe_path(root, extra["completion_manifest"])
        summary = read_json(path)
        if (
            not summary.get("complete")
            or json_hash(summary) != extra["completion_hash"]
        ):
            raise ValueError("Snapshot completion hash mismatch")
        context, stages = summary["context"], summary["stages"]
        identity = {
            "run_date": context["run_date"],
            "run_key": context["run_key"],
            "source_dag_id": context["dag_id"],
            "source_run_id": context["run_id"],
        }
        if (
            any(extra[key] != value for key, value in identity.items())
            or extra["context_hash"] != json_hash(context)
            or path
            != context_file(root, context["run_key"]).with_name(
                "completion.json"
            )
            or context["run_key"]
            != json_hash([context["dag_id"], context["run_id"]])
        ):
            raise ValueError("Snapshot identity mismatch")
        for stage in ("etl", "rfm", "audit"):
            manifest = stages[stage]
            if (
                not manifest.get("complete")
                or manifest["context_hash"] != extra["context_hash"]
                or json_hash(manifest) != extra[f"{stage}_hash"]
            ):
                raise ValueError("Snapshot stage mismatch")
            if stage != "etl":
                if manifest["etl_hash"] != extra["etl_hash"]:
                    raise ValueError("Snapshot ETL mismatch")
                marker = safe_path(root, extra[f"{stage}_manifest"])
                if marker != branch_publication_path(root, context, stage):
                    raise ValueError("Snapshot publication path mismatch")
                if read_json(marker) != {
                    "context": context,
                    "complete": True,
                    "stages": {"etl": stages["etl"], stage: manifest},
                }:
                    raise ValueError("Snapshot publication mismatch")
            for output in manifest["outputs"].values():
                if not (safe_path(root, output) / "_SUCCESS").is_file():
                    raise ValueError("Snapshot output is incomplete")
        if stages["rfm"]["model"] != context["model"]:
            raise ValueError("Snapshot model mismatch")
    return publications
