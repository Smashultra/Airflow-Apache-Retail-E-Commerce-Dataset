"""Manual integration check after lineage_smoke.py, no Spark execution."""

import json
from pathlib import Path
from urllib.parse import quote

import requests


def main():
    events = []
    for path in Path("/opt/airflow/logs").glob("openlineage-*.json"):
        event = json.loads(path.read_text())
        if (event.get("job", {}).get("name")
                == "retail_lineage_smoke.metadata_only"):
            events.append(event)
    complete = max(
        (e for e in events if e["eventType"] == "COMPLETE"),
        key=lambda e: e["eventTime"],
    )
    run_id = complete["run"]["runId"]
    assert any(e["eventType"] == "START" and e["run"]["runId"] == run_id
               for e in events)

    def get(path):
        response = requests.get("http://marquez:5000/api/v1/" + path,
                                timeout=5)
        response.raise_for_status()
        return response.json()

    run = get("jobs/runs/" + run_id)
    assert run["state"] == "COMPLETED", run
    facet = run["facets"]["retail_provenance"]
    assert facet["metadata"]["fixture"] is True, facet
    job = get("namespaces/retail-local/jobs/"
              "retail_lineage_smoke.metadata_only")
    assert {x["name"] for x in job["inputs"]} == {"input"}, job
    assert {x["name"] for x in job["outputs"]} == {"output"}, job
    node = quote("job:retail-local:retail_lineage_smoke.metadata_only",
                 safe="")
    graph = get("lineage?nodeId=" + node)
    assert len(graph["graph"]) >= 3, graph
    report = {"verified": True, "run_id": run_id,
              "state": run["state"], "graph_nodes": len(graph["graph"]),
              "custom_facet": facet}
    Path("/opt/airflow/logs/marquez-verification.json").write_text(
        json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
