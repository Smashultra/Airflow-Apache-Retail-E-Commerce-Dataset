from pathlib import Path


def test_required_project_structure_exists():
    root = Path(__file__).parents[1]
    required = {
        "AGENTS.md",
        "CONTRIBUTING.md",
        "Dockerfile",
        "docker-compose.yaml",
        "requirements.txt",
        ".env.example",
        ".agent/HANDOFF.md",
        ".agent/TODO.md",
        ".agent/DECISIONS.md",
        ".agent/PLANS.md",
        ".codex/config.toml",
        "dags/ecommerce_etl_dag.py",
        "scripts/pyspark_clean.py",
        "scripts/pyspark_rfm.py",
        "scripts/pyspark_anomalies.py",
        "docs/REPORT.md",
        "docs/SETUP.md",
    }

    assert not {path for path in required if not (root / path).is_file()}
