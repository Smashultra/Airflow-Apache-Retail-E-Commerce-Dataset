import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
SCRIPTS_DIR = ROOT / "scripts"

if "airflow" not in sys.modules:
    home = Path(tempfile.mkdtemp(prefix="airflow_pytest_"))

    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_DIR)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
        f"sqlite:///{(home / 'airflow.db').as_posix()}"
    )

for p in (DAGS_DIR, SCRIPTS_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
