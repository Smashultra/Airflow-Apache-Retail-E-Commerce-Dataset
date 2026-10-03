"""Bootstrap immutable UTF-8 daily CSV partitions from the original CSV."""

import argparse
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from retail_contracts import (
    COLUMNS,
    SCHEMA_VERSION,
    atomic_json,
    data_root,
    days,
    digest,
    read_json,
    safe_path,
    token,
)


def prepare(source, root, version="online-retail-v1", encoding="cp1252"):
    """Preserve the source and any already committed version."""
    source = Path(source)
    source_hash = digest(source)
    destination = safe_path(root, f"raw/landing/{token(version)}")
    if (destination / "manifest.json").exists():
        old = read_json(destination / "manifest.json")
        if (
            old["source_sha256"] != source_hash
            or old["source_encoding"] != encoding
        ):
            raise ValueError("Source changed: choose a new source version")
        return destination / "manifest.json"
    if destination.exists():
        raise ValueError(
            "Uncommitted destination exists; inspect before retry"
        )
    groups = defaultdict(list)
    with source.open(encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != COLUMNS:
            raise ValueError("Source header does not match the eight columns")
        for ordinal, row in enumerate(reader, 2):
            if len(row) != len(COLUMNS):
                raise ValueError(f"Malformed CSV record {ordinal}")
            try:
                day = datetime.strptime(row[4], "%m/%d/%Y %H:%M").date()
                key = day.isoformat()
            except ValueError:
                key = "undated"
            groups[key].append(row)
    dates = sorted(k for k in groups if k != "undated")
    if not dates:
        raise ValueError("Source contains no valid invoice dates")
    staging = destination.with_name(f".{version}-{uuid4().hex}")
    staging.mkdir(parents=True)
    entries = {}
    for day in [*days(dates[0], dates[-1]), "undated"]:
        relative = (
            f"date={day}/data.csv" if day != "undated" else "undated/data.csv"
        )
        output = safe_path(staging, relative)
        output.parent.mkdir(parents=True)
        with output.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(COLUMNS)
            writer.writerows(groups[day])
        with output.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.reader(stream))
        if rows[1:] != groups[day]:
            raise ValueError(f"CSV round-trip mismatch: {day}")
        entries[day] = {
            "path": relative,
            "bytes": output.stat().st_size,
            "sha256": digest(output),
            "rows": len(groups[day]),
        }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "source_version": version,
        "source_sha256": source_hash,
        "source_encoding": encoding,
        "parser": "stdlib-csv-to-utf8-v1",
        "first_date": dates[0],
        "last_date": dates[-1],
        "rows": sum(len(v) for v in groups.values()),
        "undated": entries.pop("undated"),
        "partitions": entries,
        "complete": True,
    }
    if digest(source) != source_hash:
        raise ValueError("Source changed during bootstrap")
    atomic_json(staging / "manifest.json", manifest)
    staging.rename(destination)
    return destination / "manifest.json"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(data_root() / "raw/data.csv"))
    parser.add_argument("--data-root", default=str(data_root()))
    parser.add_argument("--source-version", default="online-retail-v1")
    parser.add_argument("--encoding", default="cp1252")
    args = parser.parse_args()
    print(
        prepare(args.input, args.data_root, args.source_version, args.encoding)
    )
