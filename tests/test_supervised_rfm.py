"""Runtime regression: daily Cluster is exactly the classifier prediction."""
import os
import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
pytest.importorskip('pyspark')
from pyspark.sql import SparkSession, functions as F
from rfm_supervised import fit_student, score, evaluate
from train_rfm_kmeans import train_model
from pyspark_rfm import prepare_kmeans_features

@pytest.fixture(scope='module')
def spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    session = SparkSession.builder.master('local[1]').appName('direct-cluster-test').config('spark.sql.shuffle.partitions', '1').getOrCreate()
    yield session
    session.stop()

def test_direct_prediction_matches_classifier(spark):
    rfm = spark.createDataFrame([(str(i), float(2+i%3) if i < 20 else float(90+i%3), float(30+i%4) if i < 20 else float(1+i%2), float(3000+i*10) if i < 20 else float(10+i)) for i in range(40)], 'CustomerID string, Recency double, Frequency double, Monetary double')
    teacher = train_model(rfm, 2)
    student = fit_student(rfm, teacher)
    later = rfm.withColumn('Recency', F.col('Recency') + 2)
    expected = student.transform(prepare_kmeans_features(later)).select('CustomerID', F.col('prediction').cast('int').alias('Cluster'))
    result = score(later, student, 'v1')
    assert result.select('CustomerID', 'Cluster').exceptAll(expected).isEmpty()
    assert expected.exceptAll(result.select('CustomerID', 'Cluster')).isEmpty()
    assert not {'RawCluster', 'is_vip', 'segment', 'candidate_days', 'candidate_cluster', 'transition_reason'} & set(result.columns)
    assert evaluate(later, teacher, student)['agreement'] >= 0.9
