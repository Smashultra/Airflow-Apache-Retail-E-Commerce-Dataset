#!/usr/bin/env python3
"""
OpenLineage Interactive Demo for Retail E-Commerce Pipeline
Mô phỏng thu thập metadata, sinh OpenLineage v2 RunEvents, validate JSON Schema,
và khởi chạy giao diện Web Lineage Graph trực quan (tương tự Marquez).
"""

import http.server
import json
import os
import socketserver
import sys

# Đảm bảo UTF-8 trên Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import jsonschema
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde

# Thiết lập thư mục: scripts/ và thư mục gốc của repository
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
LOGS_DIR = REPO_ROOT / "logs" / "openlineage"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

SCHEMA_PATH = REPO_ROOT / "dags" / "retail_support" / "schemas" / "retail-lineage-v1.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8")) if SCHEMA_PATH.exists() else None
HTML_FILE = SCRIPTS_DIR / "demo_lineage_ui.html"

PIPELINE_STAGES = [
    {
        "job": "validate_raw_data",
        "title": "1. Validate Raw Data",
        "type": "Sensor / Validation",
        "inputs": ["file:///opt/airflow/data/raw/data.csv"],
        "outputs": ["file:///opt/airflow/data/raw/landing/source_v1"],
        "metadata": {
            "source_version": "source_v1",
            "source_hash": "a1b2c3d4e5f67890",
            "publication_state": "not_published"
        }
    },
    {
        "job": "submit_pyspark_etl",
        "title": "2. PySpark Clean & ETL",
        "type": "SparkSubmitOperator",
        "inputs": ["file:///opt/airflow/data/raw/landing/source_v1"],
        "outputs": [
            "file:///opt/airflow/data/staged/curated/transactions",
            "file:///opt/airflow/data/staged/audit/audit_input",
            "file:///opt/airflow/data/staged/curated/parse_errors"
        ],
        "metadata": {
            "etl_hash": "e7f80912ab345cd6",
            "publication_state": "computed",
            "metrics": {"total_rows": 541909, "clean_rows": 391057, "parse_errors": 0}
        }
    },
    {
        "job": "compute_rfm_metrics",
        "title": "3. Compute RFM Metrics",
        "type": "RfmSparkSubmitOperator",
        "inputs": [
            "file:///opt/airflow/data/staged/curated/transactions",
            "file:///opt/airflow/data/models/rfm/version=v1/pipeline"
        ],
        "outputs": [
            "file:///opt/airflow/data/analytics/rfm_daily/customers",
            "file:///opt/airflow/data/staged/analytics/rfm_daily/cluster_profile"
        ],
        "metadata": {
            "model_version": "v1",
            "publication_state": "rfm_published",
            "rfm_hash": "c4d5e6f7a8b90123",
            "metrics": {"rfm_customers": 4334, "clusters": 2}
        }
    },
    {
        "job": "detect_anomalies",
        "title": "4. Detect Anomalies & Fraud",
        "type": "AuditSparkSubmitOperator",
        "inputs": ["file:///opt/airflow/data/staged/audit/audit_input"],
        "outputs": [
            "file:///opt/airflow/data/audit/anomalies/flagged_orders",
            "file:///opt/airflow/data/staged/audit/anomalies/row_assessments"
        ],
        "metadata": {
            "publication_state": "audit_published",
            "audit_hash": "f1e2d3c4b5a67890",
            "metrics": {"evaluated_orders": 25900, "flagged_orders": 54}
        }
    },
    {
        "job": "notify_completion",
        "title": "5. Notify Completion & Publish",
        "type": "PythonOperator",
        "inputs": [
            "file:///opt/airflow/data/analytics/rfm_daily/customers",
            "file:///opt/airflow/data/audit/anomalies/flagged_orders",
            "file:///opt/airflow/data/staged/curated/transactions"
        ],
        "outputs": [
            "file:///opt/airflow/data/curated/transactions",
            "file:///opt/airflow/data/analytics/retail_snapshot"
        ],
        "metadata": {
            "publication_state": "published",
            "model_version": "v1",
            "completion_manifest": "/opt/airflow/data/manifests/2011-12-10/completion.json",
            "metrics": {
                "etl_rows": 391057,
                "rfm_customers": 4334,
                "flagged_orders": 54
            }
        }
    },
    {
        "job": "retail_marketing_consumer_dag.send_vouchers",
        "title": "6. Marketing Consumer DAG",
        "type": "Asset Consumer",
        "inputs": ["file:///opt/airflow/data/analytics/rfm_daily/customers"],
        "outputs": [],
        "metadata": {
            "consumer": "Marketing Voucher Automation",
            "publication_state": "consumed"
        }
    },
    {
        "job": "retail_executive_dashboard_dag.render_bi",
        "title": "7. Executive Dashboard DAG",
        "type": "Asset Consumer",
        "inputs": ["file:///opt/airflow/data/analytics/retail_snapshot"],
        "outputs": [],
        "metadata": {
            "consumer": "PowerBI / Executive Summary",
            "publication_state": "consumed"
        }
    }
]


def emit_pipeline_lineage(fail_task=None):
    """
    Sinh các OpenLineage RunEvent (START và COMPLETE / FAIL) cho từng task.
    Lưu từng event vào thư mục logs/openlineage/.
    """
    emitted_events = []
    pipeline_run_key = uuid4().hex[:16]

    print("=" * 70)
    print(f"BẮT ĐẦU CHẠY PIPELINE OPENLINEAGE (Run Key: {pipeline_run_key})")
    print("=" * 70)

    for stage in PIPELINE_STAGES:
        job_name = stage["job"]
        is_failing = (job_name == fail_task)
        run_id = str(uuid4())

        # 1. START EVENT: Không bao giờ quảng bá output (nguyên tắc trong OPENLINEAGE_VI.md)
        start_inputs = [Dataset(namespace="retail-local", name=inp) for inp in stage["inputs"]]
        start_event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(
                runId=run_id,
                facets={
                    "retail_provenance": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "file:///opt/airflow/dags/retail_support/schemas/retail-lineage-v1.json#/$defs/RetailRunFacet",
                        "metadata": {
                            "run_key": pipeline_run_key,
                            "run_date": "2011-12-10",
                            "code_version": "retail-v2",
                            "lifecycle": "START"
                        }
                    }
                }
            ),
            job=Job(namespace="retail-local", name=job_name),
            inputs=start_inputs,
            outputs=[],  # Tuyệt đối rỗng khi START
            producer="https://github.com/OpenLineage/OpenLineage"
        )
        emitted_events.append(start_event)

        # 2. COMPLETE HOẶC FAIL EVENT
        if is_failing:
            # FAIL EVENT: Không quảng bá output
            fail_event = RunEvent(
                eventType=RunState.FAIL,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(
                    runId=run_id,
                    facets={
                        "retail_provenance": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "file:///opt/airflow/dags/retail_support/schemas/retail-lineage-v1.json#/$defs/RetailRunFacet",
                            "metadata": {
                                "run_key": pipeline_run_key,
                                "lifecycle": "FAIL",
                                "error": "Simulated task failure"
                            }
                        }
                    }
                ),
                job=Job(namespace="retail-local", name=job_name),
                inputs=start_inputs,
                outputs=[],
                producer="https://github.com/OpenLineage/OpenLineage"
            )
            emitted_events.append(fail_event)
            print(f" [FAIL] Job {job_name} thất bại! Outputs bị thu hồi (suppressed).")
            break
        else:
            # COMPLETE EVENT: Quảng bá output hợp lệ kèm Facets
            complete_outputs = []
            for out in stage["outputs"]:
                facets = {}
                if "retail_snapshot" in out or "curated" in out or "analytics" in out:
                    facets["retail_snapshot"] = {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "file:///opt/airflow/dags/retail_support/schemas/retail-lineage-v1.json#/$defs/RetailDatasetFacet",
                        "metadata": {
                            "physical_path": f"/opt/airflow/data/physical/{out.split('/')[-1]}_{pipeline_run_key}.parquet"
                        }
                    }
                complete_outputs.append(Dataset(namespace="retail-local", name=out, facets=facets))

            meta = dict(stage["metadata"])
            meta.update({
                "run_key": pipeline_run_key,
                "run_date": "2011-12-10",
                "code_version": "retail-v2"
            })

            complete_event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(
                    runId=run_id,
                    facets={
                        "retail_provenance": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "file:///opt/airflow/dags/retail_support/schemas/retail-lineage-v1.json#/$defs/RetailRunFacet",
                            "metadata": meta
                        }
                    }
                ),
                job=Job(namespace="retail-local", name=job_name),
                inputs=start_inputs,
                outputs=complete_outputs,
                producer="https://github.com/OpenLineage/OpenLineage"
            )
            emitted_events.append(complete_event)
            print(f" [SUCCESS] Job {job_name:42} -> {len(stage['inputs'])} inputs, {len(stage['outputs'])} outputs")

    # Lưu events ra file JSON bằng Serde chuẩn của OpenLineage
    timestamp_str = int(time.time() * 1000)
    event_file = LOGS_DIR / f"openlineage-{timestamp_str}.json"
    serializable = []
    for ev in emitted_events:
        serializable.append(json.loads(Serde.to_json(ev)))

    event_file.write_text(json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n Đã ghi {len(serializable)} OpenLineage events vào: {event_file}")

    # Validate Facets theo JSON schema
    if SCHEMA and "$defs" in SCHEMA:
        for ev in serializable:
            prov = ev.get("run", {}).get("facets", {}).get("retail_provenance")
            if prov:
                jsonschema.validate(prov, SCHEMA["$defs"]["RetailRunFacet"])
        print(" Đã kiểm chứng toàn bộ facet với schema `retail-lineage-v1.json`: VALID!")

    return serializable, pipeline_run_key


def generate_html_dashboard():
    """Tạo giao diện trực quan đồ thị Lineage và chi tiết OpenLineage metadata."""
    html_content = """<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>OpenLineage & Marquez Demo Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/vis-network@9.1.9/dist/vis-network.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
        body { background: #0f172a; color: #f8fafc; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }
        header { background: #1e293b; padding: 14px 24px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }
        .logo { display: flex; align-items: center; gap: 12px; font-size: 1.25rem; font-weight: 700; color: #38bdf8; }
        .tag { background: #0284c7; color: white; padding: 4px 8px; border-radius: 6px; font-size: 0.75rem; text-transform: uppercase; font-weight: 600; }
        .controls { display: flex; gap: 10px; }
        button { background: #2563eb; color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: 0.2s; }
        button:hover { background: #1d4ed8; }
        button.danger { background: #dc2626; }
        button.danger:hover { background: #b91c1c; }
        
        .main-container { display: flex; flex: 1; height: calc(100vh - 65px); }
        #network-pane { flex: 2; position: relative; border-right: 1px solid #334155; background: #090d16; }
        #network { width: 100%; height: 100%; }
        
        #detail-pane { flex: 1.2; background: #1e293b; display: flex; flex-direction: column; overflow: hidden; }
        .tabs { display: flex; border-bottom: 1px solid #334155; background: #0f172a; }
        .tab-btn { flex: 1; padding: 12px; background: none; border: none; color: #94a3b8; font-weight: 600; cursor: pointer; border-bottom: 2px solid transparent; }
        .tab-btn.active { color: #38bdf8; border-color: #38bdf8; background: #1e293b; }
        .tab-content { flex: 1; padding: 20px; overflow-y: auto; }
        
        .node-card { background: #0f172a; border-radius: 8px; padding: 16px; margin-bottom: 16px; border: 1px solid #334155; }
        .badge { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 700; margin-bottom: 8px; }
        .badge-job { background: #3b82f6; color: white; }
        .badge-dataset { background: #10b981; color: white; }
        .badge-staged { background: #f59e0b; color: white; }
        
        h3 { font-size: 1.1rem; color: #f1f5f9; margin-bottom: 8px; }
        p { font-size: 0.88rem; color: #94a3b8; line-height: 1.4; margin-bottom: 8px; }
        pre { background: #030712; padding: 12px; border-radius: 6px; font-family: monospace; font-size: 0.8rem; color: #a5f3fc; overflow-x: auto; border: 1px solid #1e293b; }
        
        .legend { position: absolute; bottom: 16px; left: 16px; background: rgba(30, 41, 59, 0.9); padding: 12px 16px; border-radius: 8px; border: 1px solid #475569; font-size: 0.8rem; backdrop-filter: blur(4px); }
        .legend-item { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
        .legend-color { width: 14px; height: 14px; border-radius: 4px; }
    </style>
</head>
<body>
    <header>
        <div class="logo">
            <span>✨ OpenLineage & Marquez Local Explorer</span>
            <span class="tag">Namespace: retail-local</span>
        </div>
        <div class="controls">
            <button onclick="triggerRun('success')"> Chạy Pipeline Thành Công</button>
            <button class="danger" onclick="triggerRun('fail')">⚡ Test Thất Bại (Suppress Output)</button>
        </div>
    </header>

    <div class="main-container">
        <div id="network-pane">
            <div id="network"></div>
            <div class="legend">
                <div class="legend-item"><div class="legend-color" style="background: #3b82f6;"></div> Airflow / Spark Task (Job)</div>
                <div class="legend-item"><div class="legend-color" style="background: #10b981;"></div> Published Dataset (Đã công bố)</div>
                <div class="legend-item"><div class="legend-color" style="background: #f59e0b;"></div> Staged Dataset (Chưa duyệt)</div>
                <div class="legend-item"><div class="legend-color" style="background: #a855f7;"></div> Raw Source Data (Gốc)</div>
            </div>
        </div>

        <div id="detail-pane">
            <div class="tabs">
                <button class="tab-btn active" onclick="switchTab('node-info', this)">Thông Tin Node</button>
                <button class="tab-btn" onclick="switchTab('facets', this)">Custom Facets</button>
                <button class="tab-btn" onclick="switchTab('events', this)">Sự Kiện JSON Thô</button>
            </div>

            <div id="tab-node-info" class="tab-content">
                <div class="node-card">
                    <span class="badge badge-job">HƯỚNG DẪN</span>
                    <h3>Đồ thị Data Lineage</h3>
                    <p>Click vào bất kỳ Node nào trên đồ thị bên trái để xem metadata chi tiết, input/output datasets và cấu hình OpenLineage.</p>
                </div>
                <div id="selection-details"></div>
            </div>

            <div id="tab-facets" class="tab-content" style="display: none;">
                <div class="node-card">
                    <h3>Custom Facet: retail_provenance</h3>
                    <p>Facet độc quyền của Retail E-Commerce định nghĩa trong schema <code>retail-lineage-v1.json</code>:</p>
                    <pre id="facet-code">// Hãy click một Node Job để xem facet</pre>
                </div>
            </div>

            <div id="tab-events" class="tab-content" style="display: none;">
                <div class="node-card">
                    <h3>OpenLineage RunEvents v2</h3>
                    <p>Danh sách các sự kiện START và COMPLETE định dạng chuẩn OpenLineage:</p>
                    <pre id="raw-events">// Đang tải sự kiện...</pre>
                </div>
            </div>
        </div>
    </div>

    <script>
        let network = null;
        let pipelineData = null;

        function switchTab(tabId, el) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
            document.getElementById('tab-' + tabId).style.display = 'block';
            if (el) el.classList.add('active');
        }

        async function loadData() {
            try {
                const res = await fetch('/api/lineage');
                pipelineData = await res.json();
                renderGraph(pipelineData);
                document.getElementById('raw-events').textContent = JSON.stringify(pipelineData.events.slice(0, 4), null, 2);
                showNodeDetail('notify_completion');
            } catch (e) {
                console.error(e);
            }
        }

        function renderGraph(data) {
            const nodes = new vis.DataSet(data.nodes);
            const edges = new vis.DataSet(data.edges);

            const container = document.getElementById('network');
            const graphData = { nodes: nodes, edges: edges };
            const options = {
                layout: {
                    hierarchical: {
                        direction: 'LR',
                        sortMethod: 'directed',
                        levelSeparation: 220,
                        nodeSpacing: 100
                    }
                },
                nodes: {
                    shape: 'box',
                    margin: 12,
                    font: { color: '#ffffff', size: 13, face: 'monospace' },
                    borderWidth: 2,
                    shadow: true
                },
                edges: {
                    arrows: { to: { enabled: true, scaleFactor: 0.8 } },
                    color: { color: '#64748b', highlight: '#38bdf8' },
                    smooth: { type: 'cubicBezier', forceDirection: 'horizontal' }
                },
                physics: false
            };

            network = new vis.Network(container, graphData, options);

            network.on('click', function(params) {
                if (params.nodes.length > 0) {
                    const nodeId = params.nodes[0];
                    showNodeDetail(nodeId);
                }
            });
        }

        function showNodeDetail(nodeId) {
            const node = pipelineData.nodes.find(n => n.id === nodeId);
            if (!node) return;

            let html = '';
            if (node.group === 'job') {
                html = `
                    <div class="node-card">
                        <span class="badge badge-job">AIRFLOW JOB</span>
                        <h3>${node.label}</h3>
                        <p><strong>Job Type:</strong> ${node.job_type || 'Python/Spark'}</p>
                        <p><strong>Run ID:</strong> <code>${node.run_id}</code></p>
                        <p><strong>State:</strong> <span style="color: #4ade80; font-weight: bold;">${node.state}</span></p>
                    </div>
                    <div class="node-card">
                        <h3>Input Datasets (${node.inputs.length})</h3>
                        ${node.inputs.map(i => `<p>📥 <code>${i}</code></p>`).join('') || '<p><em>Không có input</em></p>'}
                    </div>
                    <div class="node-card">
                        <h3>Output Datasets (${node.outputs.length})</h3>
                        ${node.outputs.map(o => `<p>📤 <code>${o}</code></p>`).join('') || '<p><em>Không có output</em></p>'}
                    </div>
                `;
                document.getElementById('facet-code').textContent = JSON.stringify(node.facets || {}, null, 2);
            } else {
                html = `
                    <div class="node-card">
                        <span class="badge ${node.group === 'staged' ? 'badge-staged' : 'badge-dataset'}">DATASET</span>
                        <h3>${node.label}</h3>
                        <p><strong>Namespace:</strong> <code>retail-local</code></p>
                        <p><strong>Tên logic:</strong> <code>${node.id}</code></p>
                        <p><strong>Phân loại:</strong> ${node.group === 'staged' ? '⚠️ Dữ liệu tạm thời (Staged - chưa công bố)' : '✅ Dữ liệu hoàn chỉnh (Published)'}</p>
                    </div>
                    <div class="node-card">
                        <h3>Dataset Facets</h3>
                        <pre>${JSON.stringify(node.facets || { "retail_snapshot": { "physical_path": node.id + ".parquet" } }, null, 2)}</pre>
                    </div>
                `;
            }
            document.getElementById('selection-details').innerHTML = html;
        }

        async function triggerRun(mode) {
            const btn = event.target;
            btn.disabled = true;
            btn.textContent = '⏳ Đang chạy pipeline...';
            try {
                const res = await fetch('/api/run?mode=' + mode);
                const data = await res.json();
                pipelineData = data;
                renderGraph(data);
                document.getElementById('raw-events').textContent = JSON.stringify(data.events.slice(0, 4), null, 2);
                alert(mode === 'success' ? ' Chạy Pipeline thành công! Toàn bộ OpenLineage events đã được cập nhật.' : '⚠️ Đã mô phỏng Task thất bại! Kiểm tra Task `compute_rfm_metrics`: Outputs bị thu hồi hoàn toàn.');
            } finally {
                btn.disabled = false;
                btn.textContent = (mode === 'success' ? ' Chạy Pipeline Thành Công' : '⚡ Test Thất Bại (Suppress Output)');
            }
        }

        window.onload = loadData;
    </script>
</body>
</html>
"""
    HTML_FILE.write_text(html_content, encoding="utf-8")
    return HTML_FILE


def build_graph_data(events, run_key):
    """Xây dựng cấu trúc Node và Edge cho đồ thị từ các OpenLineage events."""
    nodes = []
    edges = []
    node_set = set()

    for stage in PIPELINE_STAGES:
        job_id = stage["job"]
        # Thêm Job Node
        nodes.append({
            "id": job_id,
            "label": stage["title"],
            "group": "job",
            "color": {"background": "#1e3a8a", "border": "#3b82f6"},
            "job_type": stage["type"],
            "run_id": uuid4().hex,
            "state": "COMPLETED",
            "inputs": stage["inputs"],
            "outputs": stage["outputs"],
            "facets": {
                "retail_provenance": {
                    "metadata": dict(stage["metadata"], run_key=run_key, run_date="2011-12-10")
                }
            }
        })

        # Thêm Input Nodes & Edges
        for inp in stage["inputs"]:
            clean_name = inp.replace("file:///opt/airflow/data/", "")
            if clean_name not in node_set:
                is_staged = "staged/" in clean_name
                is_raw = "raw/" in clean_name
                bg = "#581c87" if is_raw else ("#78350f" if is_staged else "#064e3b")
                border = "#a855f7" if is_raw else ("#f59e0b" if is_staged else "#10b981")
                nodes.append({
                    "id": clean_name,
                    "label": clean_name,
                    "group": "raw" if is_raw else ("staged" if is_staged else "dataset"),
                    "color": {"background": bg, "border": border}
                })
                node_set.add(clean_name)
            edges.append({"from": clean_name, "to": job_id})

        # Thêm Output Nodes & Edges
        for out in stage["outputs"]:
            clean_name = out.replace("file:///opt/airflow/data/", "")
            if clean_name not in node_set:
                is_staged = "staged/" in clean_name
                bg = "#78350f" if is_staged else "#064e3b"
                border = "#f59e0b" if is_staged else "#10b981"
                nodes.append({
                    "id": clean_name,
                    "label": clean_name,
                    "group": "staged" if is_staged else "dataset",
                    "color": {"background": bg, "border": border}
                })
                node_set.add(clean_name)
            edges.append({"from": job_id, "to": clean_name})

    return {"nodes": nodes, "edges": edges, "events": events}


class LineageDemoHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP Server phục vụ giao diện Web và Lineage API."""

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_FILE.read_bytes())
        elif self.path.startswith("/api/lineage"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            events, run_key = emit_pipeline_lineage()
            graph_data = build_graph_data(events, run_key)
            self.wfile.write(json.dumps(graph_data).encode("utf-8"))
        elif self.path.startswith("/api/run"):
            fail_task = "compute_rfm_metrics" if "mode=fail" in self.path else None
            events, run_key = emit_pipeline_lineage(fail_task=fail_task)
            graph_data = build_graph_data(events, run_key)
            if fail_task:
                # Điều chỉnh graph khi lỗi
                for n in graph_data["nodes"]:
                    if n["id"] == fail_task:
                        n["state"] = "FAILED"
                        n["color"] = {"background": "#7f1d1d", "border": "#ef4444"}
                        n["outputs"] = []
                graph_data["edges"] = [e for e in graph_data["edges"] if e["from"] != fail_task]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(graph_data).encode("utf-8"))
        else:
            super().do_GET()

    def log_message(self, format, *args):
        # Giảm ồn console log
        return


def run_demo_server(port=8050):
    generate_html_dashboard()
    print("\n Khởi tạo OpenLineage Events lần đầu...")
    emit_pipeline_lineage()

    handler = LineageDemoHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        url = f"http://localhost:{port}"
        print("=" * 70)
        print(f" DEMO OPENLINEAGE DASHBOARD ĐÃ SẴN SÀNG!")
        print(f" Mở trình duyệt tại: {url}")
        print("=" * 70)
        print("Nhấn Ctrl+C để dừng server khi xem xong.")
        try:
            webbrowser.open(url)
        except Exception:
            pass
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nĐã tắt Demo Server.")


if __name__ == "__main__":
    port = 8050
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    run_demo_server(port)
