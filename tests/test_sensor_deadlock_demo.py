"""Demo isolation, signal matching and scheduler-facing configuration."""

from airflow.serialization.serialized_objects import DagSerialization
import retail_sensor_deadlock_demo as demo


def test_signal_isolated_by_key(tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "data_root", lambda: tmp_path)
    assert not demo.signal_ready({"demo_key": "a"})
    demo.produce_signal({"demo_key": "a"})
    assert demo.signal_ready({"demo_key": "a"})
    assert not demo.signal_ready({"demo_key": "b"})
    assert demo.signal_path("../../escape").parent == tmp_path / "demos" / "sensor_deadlock"


def test_demo_serialization_and_pool():
    for dag in (demo.poke_demo, demo.reschedule_demo, demo.producer_demo):
        restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
        assert len(restored.tasks) == 1
        task = restored.tasks[0]
        assert task.pool == "retail_sensor_demo"
        assert task.pool_slots == 1
        assert task.retries == 0
    for dag, mode in ((demo.poke_demo, "poke"), (demo.reschedule_demo, "reschedule")):
        task = dag.get_task("wait_for_signal")
        assert task.mode == mode
        assert task.timeout == 180
        assert task.poke_interval == 15
