"""Predict numeric KMeans pseudo-labels directly with a frozen classifier.

Probability is a classifier score, not calibrated business confidence or churn.
"""
import json
from datetime import date

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F


def check_dates(train_date, validation_date, test_date):
    dates = [date.fromisoformat(v) for v in (train_date, validation_date, test_date)]
    if not dates[0] < dates[1] < dates[2]:
        raise ValueError('Require train < validation < test cutoff')


def fit_student(rfm, teacher, seed=42):
    from pyspark_rfm import prepare_kmeans_features
    labeled = teacher.transform(prepare_kmeans_features(rfm))
    classifier = RandomForestClassifier(
        labelCol='Cluster', featuresCol='features', predictionCol='prediction',
        numTrees=60, maxDepth=8, seed=seed,
    ).fit(labeled)
    return PipelineModel(stages=[*teacher.stages[:2], classifier])


def evaluate(rfm, teacher, student):
    """Agreement with a frozen teacher, NOT ground-truth business accuracy."""
    from pyspark_rfm import prepare_kmeans_features
    labeled = teacher.transform(prepare_kmeans_features(rfm)).select(
        'CustomerID', F.col('Cluster').alias('label'))
    predicted = student.transform(prepare_kmeans_features(rfm)).select(
        'CustomerID', F.col('prediction').cast('int').alias('predicted'))
    cells = predicted.join(labeled, 'CustomerID').groupBy('label', 'predicted').count().collect()
    total = sum(r['count'] for r in cells)
    k = teacher.stages[-1].getK()
    per_class = []
    for c in range(k):
        tp = sum(r['count'] for r in cells if r['label'] == c == r['predicted'])
        actual = sum(r['count'] for r in cells if r['label'] == c)
        emitted = sum(r['count'] for r in cells if r['predicted'] == c)
        precision = tp / emitted if emitted else 0.0
        recall = tp / actual if actual else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class.append(dict(cluster=c, support=actual, precision=precision, recall=recall, f1=f1))
    return dict(customers=total, agreement=sum(r['count'] for r in cells if r['label'] == r['predicted']) / total,
                macro_f1=sum(r['f1'] for r in per_class) / k,
                per_class=per_class, confusion=[r.asDict() for r in cells],
                target='frozen_kmeans_pseudo_label')


def load_bundle(spark, path, run_date):
    rows = spark.read.text(path.rstrip('/') + '/supervised_info').collect()
    if len(rows) != 1:
        raise ValueError('Expected one supervised metadata record')
    info = json.loads(rows[0]['value'])
    if info['schema_version'] != 2 or info['target'] != 'frozen_kmeans_pseudo_label':
        raise ValueError('Unsupported supervised model contract')
    # Evaluation dates also influenced release selection; do not backdate deployment.
    if date.fromisoformat(run_date) <= date.fromisoformat(info['test_date']):
        raise ValueError('Daily cutoff must be after the held-out test cutoff')
    model = PipelineModel.load(path.rstrip('/') + '/student')
    if model.uid != info['model_id'] or model.stages[-1].numClasses != info['k']:
        raise ValueError('Classifier classes do not match metadata')
    return model, info


def score(rfm, student, version):
    from pyspark_rfm import prepare_kmeans_features, LOG_FEATURES
    return (student.transform(prepare_kmeans_features(rfm))
            .withColumn('Cluster', F.col('prediction').cast('int'))
            .withColumn('confidence', F.array_max(vector_to_array('probability')))
            .withColumn('model_version', F.lit(version))
            .withColumn('supervised_model_id', F.lit(student.uid))
            .drop(*LOG_FEATURES, 'raw_features', 'features', 'prediction', 'rawPrediction', 'probability'))


def daily(spark, rfm, bundle_path, run_date, version):
    """Load the frozen classifier and predict Cluster; no previous labels needed."""
    model, info = load_bundle(spark, bundle_path, run_date)
    return score(rfm, model, version), info['k']
