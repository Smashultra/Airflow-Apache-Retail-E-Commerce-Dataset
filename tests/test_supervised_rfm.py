"""Real Spark inference, cohorts, temporal selection and persistence contracts."""
import json
import os
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

import rfm_supervised as supervised
import train_rfm_classifier as training
from train_rfm_kmeans import train_model
from pyspark_rfm import add_rfm_scores, write_rfm


@pytest.fixture(scope='module')
def model_data(spark):
    frame = spark.createDataFrame([(str(i), float(2+i%3) if i < 12 else float(90+i%3),
                                    float(30+i%4) if i < 12 else float(1+i%2),
                                    float(3000+i*10) if i < 12 else float(10+i)) for i in range(24)],
                                   'CustomerID string, Recency double, Frequency double, Monetary double')
    teacher = train_model(frame, 2)
    student = supervised.fit_student(frame, teacher, num_trees=4, max_depth=3)
    info = dict(schema_version=3, target='frozen_kmeans_pseudo_label', k=2, model_id=student.uid,
                train_date='2020-01-01', validation_date='2020-02-01', test_date='2020-03-01',
                fit_cutoff='2020-03-15', selected_params=dict(num_trees=4, max_depth=3))
    return frame, teacher, student, info


def test_unseen_extremes_and_output_schema(spark, model_data):
    _, teacher, model, _ = model_data
    new = spark.createDataFrame([('new-high', 2., 33., 3100.), ('new-low', 91., 1., 30.)],
                               'CustomerID string, Recency double, Frequency double, Monetary double')
    result = supervised.score(new, model, 'v3')
    # Independent cluster meaning from the teacher centroids: larger scaled M.
    high = max(range(2), key=lambda c: teacher.stages[-1].clusterCenters()[c][2])
    labels = {r.CustomerID: r.Cluster for r in result.collect()}
    assert labels == {'new-high': high, 'new-low': 1-high}
    assert set(result.columns) == set(new.columns) | {'Cluster','confidence','model_version','supervised_model_id'}
    assert isinstance(result.schema['Cluster'].dataType, IntegerType)
    assert isinstance(result.schema['confidence'].dataType, DoubleType)
    assert isinstance(result.schema['model_version'].dataType, StringType)
    assert result.filter((F.col('confidence') < 0) | (F.col('confidence') > 1)).isEmpty()


def test_evaluation_reports_new_seen_and_empty_cohorts(model_data):
    frame, teacher, student, _ = model_data
    report = supervised.evaluate(frame, teacher, student, frame.filter("CustomerID < '2'"))
    assert report['all']['customers'] == 24
    assert report['new']['customers'] + report['seen']['customers'] == 24
    empty_new = supervised.evaluate(frame, teacher, student, frame)
    assert empty_new['new']['customers'] == 0 and empty_new['new']['macro_f1'] is None


def test_invalid_pipeline_rejected(model_data):
    _, _, student, info = model_data
    supervised.validate_student(student, info)
    with pytest.raises(ValueError):
        supervised.validate_student(PipelineModel(stages=student.stages[:2]), info)
    wrong = VectorAssembler(inputCols=['Monetary'], outputCol='raw_features')
    with pytest.raises(ValueError):
        supervised.validate_student(PipelineModel(stages=[wrong, *student.stages[1:]]), info)
    with pytest.raises(ValueError):
        supervised.validate_student(student, {**info, 'k': 3})
    with pytest.raises(ValueError):
        supervised.validate_info(info, '2020-03-15')


def test_selection_evaluates_test_once_before_refit(model_data, monkeypatch):
    frame, teacher, student, _ = model_data
    validation = frame.withColumn('CustomerID', F.concat(F.lit('new-'), F.col('CustomerID')))
    test, latest = object(), object()
    fits, evaluations = [], []
    def fit(data, *args, **kwargs):
        fits.append(data)
        return student
    def evaluate(data, *args):
        evaluations.append(data)
        if data is test:
            assert all(x is not latest for x in fits)
            return {'new': {'macro_f1': 0.0}}  # test must not gate/refit/select
        return {'new': {'macro_f1': .9}}
    monkeypatch.setattr(supervised, 'fit_student', fit)
    monkeypatch.setattr(supervised, 'evaluate', evaluate)
    _, info = supervised.select_and_refit(frame, validation, test, latest, teacher,
        train_date='2020-01-01', validation_date='2020-02-01', test_date='2020-03-01',
        refit_date='2020-03-15', trees=[4,8], depths=[2], minimum=.8)
    assert sum(x is test for x in evaluations) == 1
    assert fits[-1] is latest
    assert info['selected_params'] == dict(num_trees=4, max_depth=2)
    assert info['reports']['test']['new']['macro_f1'] == 0


def test_real_temporal_grid_and_refit(model_data):
    frame, teacher, _, _ = model_data
    validation = frame.withColumn('CustomerID', F.concat(F.lit('v-'), F.col('CustomerID')))
    test = frame.withColumn('CustomerID', F.concat(F.lit('t-'), F.col('CustomerID')))
    latest = frame.unionByName(validation).unionByName(test)
    deployed, info = supervised.select_and_refit(frame, validation, test, latest, teacher,
        train_date='2020-01-01', validation_date='2020-02-01', test_date='2020-03-01',
        refit_date='2020-03-15', trees=[2,4], depths=[3], minimum=.5)
    assert len(info['candidates']) == 2
    assert info['reports']['validation']['new']['customers'] == 24
    assert info['reports']['test']['new']['customers'] == 24
    assert info['fit_cutoff'] == '2020-03-15'
    assert info['model_id'] == deployed.uid != info['evaluated_model_id']
    supervised.validate_student(deployed, info)


@pytest.mark.parametrize('mismatch', ['date', 'snapshot', 'gate'])
def test_cli_provenance_and_gate_never_write(model_data, monkeypatch, mismatch):
    frame, teacher, _, _ = model_data
    args = training.parse_args(['--store-path','store','--teacher-path','teacher','--train-date','2020-01-01',
       '--validation-date','2020-02-01','--test-date','2020-03-01','--model-output','output'])
    metadata = dict(run_date='2019-01-01' if mismatch == 'date' else args.train_date, store_version='teacher')
    spark = Mock()
    spark.read.text.return_value.take.return_value = [{'value': json.dumps(metadata)}]
    monkeypatch.setattr(training, 'assert_separate_paths', lambda *a: None)
    monkeypatch.setattr(training, 'load_rfm_model', lambda *a: teacher)
    monkeypatch.setattr(training, 'read_store_rfm', lambda s,p,d: (frame, {'run_date':d,'version':'other' if mismatch == 'snapshot' else 'teacher'}))
    def reject(*a, **kw):
        raise ValueError('validation gate failed')
    monkeypatch.setattr(training, 'select_and_refit', reject)
    save = Mock()
    monkeypatch.setattr(training, 'save_bundle', save)
    with pytest.raises(ValueError):
        training.run_training(spark, args)
    save.assert_not_called()


@pytest.mark.parametrize('metadata_mismatch', [False, True])
def test_airflow_rfm_dispatch_uses_classifier(spark, model_data, tmp_path, monkeypatch, metadata_mismatch):
    import retail_pipeline as adapter
    import retail_models as registry
    _, _, model, info = model_data
    source = spark.createDataFrame([('a','i1',datetime(2020,3,31),2,10.),
                                    ('b','i2',datetime(2020,3,15),5,100.)],
                                   'CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double')
    manifest = dict(model_kind='classifier',model_version='v3',bundle_path='models/bundle',supervised_info=info)
    monkeypatch.setattr(adapter,'load_stage',lambda *a: {'outputs':{'transactions':'curated/input'}})
    monkeypatch.setattr(registry,'validate_binding',lambda *a,**kw: manifest)
    monkeypatch.setattr(registry,'read_model',lambda *a,**kw: manifest)
    monkeypatch.setattr(supervised,'load_bundle',lambda *a: (model, {**info, 'seed': -1} if metadata_mismatch else info))
    monkeypatch.setattr(PipelineModel,'load',lambda *a: pytest.fail('KMeans load reached in classifier branch'))
    written = {}
    monkeypatch.setattr(adapter,'parquet',lambda frame,path: written.update({path.name:frame.collect()}))
    reader = Mock()
    reader.read.parquet.return_value = source
    context = dict(kind='daily',model={},run_date='2020-04-01',source_version='v1',source_hash='h',
                   first_date='2019-01-01',churn_days=90,rfm_k=2)
    if metadata_mismatch:
        with pytest.raises(ValueError, match='metadata'):
            adapter.run_rfm(reader, context, tmp_path/'output', tmp_path)
        assert not written
        return
    metrics, outputs, _ = adapter.run_rfm(reader, context, tmp_path/'output', tmp_path)
    assert metrics['customers'] == 2 and outputs == ['customers','cluster_profile']
    assert {r.model_version for r in written['customers']} == {'v3'}
    assert {r.supervised_model_id for r in written['customers']} == {model.uid}


disk = pytest.mark.skipif(os.name == 'nt' and not os.environ.get('HADOOP_HOME'), reason='Windows Hadoop native filesystem unavailable; run persistence tests in Linux/Docker')


@disk
def test_bundle_roundtrip_and_cutoff(spark, model_data, tmp_path):
    frame, teacher, student, info = model_data
    path = str(tmp_path / 'bundle')
    supervised.save_bundle(spark, path, student, teacher, info)
    loaded, _ = supervised.load_bundle(spark, path, '2020-03-16')
    assert supervised.score(frame, loaded, 'v3').count() == 24
    with pytest.raises(ValueError):
        supervised.load_bundle(spark, path, '2020-03-15')


@disk
def test_classifier_output_partition_and_retry(spark, tmp_path):
    frame = spark.createDataFrame([('a',date(2020,1,10),datetime(2020,1,9),1,1,10.,0,'v3','model')],
        'CustomerID string, run_date date, LastPurchaseDate timestamp, Recency int, Frequency int, Monetary double, Cluster int, model_version string, supervised_model_id string')
    result = add_rfm_scores(frame)
    path = write_rfm(result, str(tmp_path), '2020-01-10', 2, 'v3')
    assert 'run_date' not in spark.read.parquet(path).columns
    assert write_rfm(result, str(tmp_path), '2020-01-10', 2, 'v3') == path
    with pytest.raises(ValueError, match='different state'):
        write_rfm(result.withColumn('Cluster', F.lit(1)), str(tmp_path), '2020-01-10', 2, 'v3')


@disk
def test_daily_main_classifier_path(spark, model_data, tmp_path, monkeypatch):
    import pyspark_rfm as job
    import sys
    frame, teacher, model, info = model_data
    bundle = str(tmp_path / 'bundle')
    supervised.save_bundle(spark, bundle, model, teacher, info)
    population = (frame.withColumn('run_date', F.lit('2020-04-01').cast('date'))
                  .withColumn('Recency', F.col('Recency').cast('int'))
                  .withColumn('LastPurchaseDate', F.date_sub(F.col('run_date'), F.col('Recency')).cast('timestamp')))
    monkeypatch.setattr(job, 'ingest_store', lambda *a: {'version': 'fixture'})
    monkeypatch.setattr(job, 'daily_rfm_population', lambda *a: add_rfm_scores(population))
    monkeypatch.setattr(job, 'load_rfm_model', lambda *a: pytest.fail('Daily must not load KMeans'))
    monkeypatch.setattr(spark, 'stop', lambda: None)
    monkeypatch.setattr(sys, 'argv', ['pyspark_rfm.py', '--input', str(tmp_path/'input'), '--batch-id','b1',
        '--store-path', str(tmp_path/'store'), '--run-date','2020-04-01', '--model-path', bundle,
        '--model-version','v3','--model-kind','classifier','--output',str(tmp_path/'output')])
    job.main()
    result = spark.read.parquet(str(tmp_path/'output/run_date=2020-04-01'))
    assert result.count() == 24 and 'Cluster' in result.columns
    assert 'run_date' not in result.columns
