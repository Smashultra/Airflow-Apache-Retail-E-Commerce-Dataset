"""Attach the optional JVM listener immediately before Spark submission."""

from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from retail_spark_lineage import (
    JAR_PATH,
    LISTENER,
    enabled,
    flag,
    listener_conf,
    verify_jar,
)


class LineageSparkSubmitOperator(SparkSubmitOperator):
    """Preserve Asset gates; missing or corrupt opt-in JAR fails fast."""

    def execute(self, context):
        if enabled():
            if version("pyspark") != "4.2.0":
                raise ValueError("Revalidate lineage before upgrading Spark")
            verify_jar()
            folder = Path("/opt/airflow/logs/spark_openlineage")
            folder.mkdir(parents=True, exist_ok=True)
            # Separate files for every submission, including repeated attempts.
            location = folder / f"{uuid4()}.jsonl"
            conf = dict(self.conf or {})
            listeners = conf.get("spark.extraListeners", "").split(",")
            conf.update(
                listener_conf(
                    location, marquez=flag("RETAIL_SPARK_LINEAGE_MARQUEZ")
                )
            )
            conf["spark.extraListeners"] = ",".join(
                dict.fromkeys(item for item in [*listeners, LISTENER] if item)
            )
            self.conf = conf
            self.jars = ",".join(
                dict.fromkeys(
                    item
                    for item in [*(self.jars or "").split(","), str(JAR_PATH)]
                    if item
                )
            )
            # Load Spark 4 extension interfaces in the driver's classloader.
            self.driver_class_path = ":".join(
                dict.fromkeys(
                    item
                    for item in [
                        *(self.driver_class_path or "").split(":"),
                        str(JAR_PATH),
                    ]
                    if item
                )
            )
            # Provider computes the same parent UUID as its Airflow task event.
            self._openlineage_inject_parent_job_info = True
            # Python transport keys are not Java transport keys.
            self._openlineage_inject_transport_info = False
            self.log.info("Spark lineage events: %s", location)
        return super().execute(context)
