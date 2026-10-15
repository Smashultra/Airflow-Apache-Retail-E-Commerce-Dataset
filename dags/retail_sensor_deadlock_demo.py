"""Isolated UI demo: one pool slot, a waiting sensor and its producer."""

import logging
from datetime import timedelta
from hashlib import sha256

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Param

from retail_contracts import atomic_json, data_root

LOGGER = logging.getLogger(__name__)
POOL = "retail_sensor_demo"


def signal_path(demo_key):
    # Hash the UI key so it cannot escape the isolated demo directory.
    key = sha256(demo_key.encode("utf-8")).hexdigest()
    return data_root() / "demos" / "sensor_deadlock" / f"{key}.json"


def signal_ready(params):
    ready = signal_path(params["demo_key"]).exists()
    LOGGER.info("SENSOR_DEMO key=%s ready=%s", params["demo_key"], ready)
    return ready


def produce_signal(params):
    atomic_json(signal_path(params["demo_key"]), {"ready": True})
    LOGGER.info("SENSOR_DEMO produced key=%s", params["demo_key"])


def make_demo(kind):
    with DAG(
        dag_id=f"retail_sensor_demo_{kind}",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=10),
        default_args={"retries": 0, "pool": POOL, "pool_slots": 1},
        params={"demo_key": Param("demo_01", type="string", minLength=1, maxLength=80)},
        tags=["retail", "demo", "sensor-deadlock"],
        doc_md="UI-only demo. Create retail_sensor_demo pool with 1 slot. "
        "Start poke/reschedule sensor first; then producer with the same fresh demo_key. "
        "See docs/DEMO_SENSOR_DEADLOCK_VI.md. No business data is processed.",
    ) as dag:
        if kind == "producer":
            PythonOperator(task_id="produce_signal", python_callable=produce_signal)
        else:
            PythonSensor(
                task_id="wait_for_signal", python_callable=signal_ready,
                mode=kind, poke_interval=15, timeout=180,
            )
    return dag


poke_demo = make_demo("poke")
reschedule_demo = make_demo("reschedule")
producer_demo = make_demo("producer")
