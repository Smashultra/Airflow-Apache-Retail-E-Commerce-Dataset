"""Early publication must stand alone and preserve its advertised snapshot."""

import pytest

from retail_contracts import atomic_json, context_file, stage_file
from retail_support.publication import (
    rfm_event_metadata,
    rfm_publication,
    rfm_publication_path,
)


def test_publish_without_audit_or_completion_and_reuse_marker(rfm_ready):
    root, context, stages = rfm_ready
    summary = rfm_publication(root, context, publish=True)
    marker = rfm_publication_path(root, context)
    before = marker.read_bytes()
    assert summary["stages"] == stages
    assert rfm_publication(root, context, publish=True) == summary
    assert marker.read_bytes() == before
    assert not stage_file(root, context, "audit").exists()
    assert (
        not context_file(root, context["run_key"])
        .with_name("completion.json")
        .exists()
    )
    extra = rfm_event_metadata(root, summary)
    assert extra["rfm_manifest"] == str(marker)
    assert "completion_manifest" not in extra
    assert "published_pointer" not in extra


@pytest.mark.parametrize(
    "damage",
    [
        "etl_hash",
        "context_hash",
        "model",
        "incomplete",
        "marker",
        "outputs",
    ],
)
def test_corrupt_rfm_cannot_publish(rfm_ready, damage):
    root, context, stages = rfm_ready
    rfm = stages["rfm"]
    if damage in {"etl_hash", "context_hash", "model"}:
        rfm[damage] = "wrong"
    elif damage == "incomplete":
        rfm["complete"] = False
    elif damage == "marker":
        (root / rfm["outputs"]["customers"] / "_SUCCESS").unlink()
    else:
        del rfm["outputs"]["customers"]
    atomic_json(stage_file(root, context, "rfm"), rfm)
    with pytest.raises(ValueError):
        rfm_publication(root, context, publish=True)
    assert not rfm_publication_path(root, context).exists()


def test_changed_generation_cannot_replace_an_advertised_snapshot(rfm_ready):
    root, context, stages = rfm_ready
    rfm_publication(root, context, publish=True)
    marker = rfm_publication_path(root, context)
    before = marker.read_bytes()
    stages["rfm"]["metrics"] = {"rows": 999}
    atomic_json(stage_file(root, context, "rfm"), stages["rfm"])
    with pytest.raises(ValueError, match="new DagRun"):
        rfm_publication(root, context, publish=True)
    assert marker.read_bytes() == before


def test_failed_atomic_publication_leaves_no_ready_marker(
    rfm_ready, monkeypatch
):
    root, context, _ = rfm_ready

    def fail(*args):
        raise OSError("disk unavailable")

    monkeypatch.setattr("retail_contracts.os.replace", fail)
    with pytest.raises(OSError, match="disk unavailable"):
        rfm_publication(root, context, publish=True)
    assert not rfm_publication_path(root, context).exists()
