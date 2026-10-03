"""Input identity, raw preservation and crash-safe metadata publication."""

import csv
from pathlib import Path

import pytest

import retail_contracts as contracts
from prepare_daily_landing import prepare


@pytest.fixture
def landing(tmp_path):
    source = tmp_path / "original.csv"
    with source.open("w", encoding="cp1252", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(contracts.COLUMNS)
        writer.writerows(
            [
                ["1", "A", 'Café, "gift"', 1, "12/7/2011 10:00", 2, "1", "UK"],
                ["2", "B", "B", 1, "12/9/2011 10:00", 3, "2", "UK"],
                ["3", "B", "B", 1, "invalid", 3, "", "UK"],
            ]
        )
    before = contracts.digest(source)
    manifest = prepare(source, tmp_path, "fixture")
    assert contracts.digest(source) == before
    return tmp_path, source, manifest


def test_bootstrap_roundtrip_empty_day_and_idempotency(landing):
    root, source, manifest_path = landing
    manifest, paths = contracts.validate_landing(
        root, "fixture", "2011-12-09", checksums=True
    )
    assert manifest["rows"] == 3
    assert manifest["partitions"]["2011-12-08"]["rows"] == 0
    with Path(paths[0]).open(encoding="utf-8", newline="") as stream:
        assert list(csv.reader(stream))[1][2] == 'Café, "gift"'
    assert prepare(source, root, "fixture") == manifest_path


def test_committed_source_cannot_change(landing):
    root, source, _ = landing
    with source.open("a", encoding="cp1252") as stream:
        stream.write("\n")
    with pytest.raises(ValueError, match="Source changed"):
        prepare(source, root, "fixture")


def test_missing_empty_corrupt_files_and_invalid_day(landing):
    root, _, manifest_path = landing
    with pytest.raises(ValueError, match="coverage"):
        contracts.validate_landing(root, "fixture", "2026-01-01")
    manifest = contracts.read_json(manifest_path)
    path = manifest_path.parent / manifest["partitions"]["2011-12-08"]["path"]
    path.write_bytes(b"")
    with pytest.raises(ValueError, match="zero-byte"):
        contracts.validate_landing(root, "fixture", "2011-12-09")


def test_paths_and_date_reject_unsafe_inputs(tmp_path):
    with pytest.raises(ValueError):
        contracts.safe_path(tmp_path, "../outside")
    with pytest.raises(ValueError):
        contracts.token("../../raw")
    with pytest.raises(ValueError):
        contracts.iso_date("2011-02-30")


def test_checksum_detects_same_size_change(landing):
    root, _, manifest_path = landing
    manifest = contracts.read_json(manifest_path)
    entry = manifest["partitions"]["2011-12-09"]
    path = manifest_path.parent / entry["path"]
    original = path.read_bytes()
    path.write_bytes(original.replace(b"12/9/2011", b"12/8/2011"))
    assert path.stat().st_size == entry["bytes"]
    with pytest.raises(ValueError, match="checksum"):
        contracts.validate_landing(root, "fixture", "2011-12-09", True)


def test_context_is_frozen_and_cutoff_is_next_day(landing):
    root, _, _ = landing
    params = {"source_version": "fixture", "rfm_k": 2, "churn_days": 90}
    metadata = contracts.build_context(
        root, "dag", "run", "2011-12-09", params
    )
    assert metadata["run_date"] == "2011-12-10"
    assert contracts.load_context(metadata["context_path"], root)["rfm_k"] == 2
    with pytest.raises(ValueError, match="inputs changed"):
        contracts.build_context(
            root, "dag", "run", "2011-12-09", {**params, "rfm_k": 3}
        )


def test_atomic_replace_failure_preserves_published_file(
    tmp_path, monkeypatch
):
    target = tmp_path / "published.json"
    contracts.atomic_json(target, {"version": "old"})

    def fail_replace(*args):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(contracts.os, "replace", fail_replace)
    with pytest.raises(OSError):
        contracts.atomic_json(target, {"version": "partial"})
    assert contracts.read_json(target) == {"version": "old"}


def test_publish_rejects_mixed_generations_and_missing_markers(landing):
    root, _, _ = landing
    params = {"source_version": "fixture", "rfm_k": 2, "churn_days": 90}
    meta = contracts.build_context(root, "dag", "run", "2011-12-09", params)
    context = contracts.load_context(meta["context_path"], root)
    stages = {}
    for stage in ("etl", "rfm", "audit"):
        output = root / "test_outputs" / stage
        output.mkdir(parents=True)
        (output / "_SUCCESS").touch()
        value = {
            "complete": True,
            "context_hash": contracts.json_hash(context),
            "outputs": {"data": output.relative_to(root).as_posix()},
            "etl_hash": contracts.json_hash(stages["etl"]) if stages else None,
        }
        stages[stage] = value
        contracts.atomic_json(
            contracts.stage_file(root, context, stage), value
        )
    contracts.publish_completion(root, context)
    pointer = root / "manifests/published/2011-12-10.json"
    before = pointer.read_bytes()
    audit = {**stages["audit"], "etl_hash": "wrong"}
    contracts.atomic_json(contracts.stage_file(root, context, "audit"), audit)
    with pytest.raises(ValueError, match="different ETL"):
        contracts.publish_completion(root, context)
    assert pointer.read_bytes() == before
    (root / "test_outputs/etl/_SUCCESS").unlink()
    with pytest.raises(ValueError, match="Incomplete Parquet"):
        contracts.publish_completion(root, context)
