"""Linux/Docker acceptance: bootstrap -> train -> activate -> daily classifier."""
import csv
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

import retail_contracts as contracts
import retail_models as models
from prepare_daily_landing import prepare
from retail_pipeline import run_stage
from retail_training import train_model


@pytest.mark.skipif(os.name == 'nt' and not os.environ.get('HADOOP_HOME'), reason='Requires Hadoop native persistence; run in project Docker')
def test_registry_classifier_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setenv('RETAIL_DATA_ROOT', str(tmp_path))
    raw = tmp_path/'raw.csv'
    with raw.open('w', encoding='cp1252', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(contracts.COLUMNS)
        for cohort, day in enumerate(['12/20/2019 10:00','1/15/2020 10:00','2/15/2020 10:00','3/15/2020 10:00']):
            for i in range(8):
                key = f'{cohort}-{i}'
                writer.writerow([key,'A','Product',1,day,float(10+i if i<4 else 1000+i*10),key,'UK'])
        writer.writerow(['daily','A','Product',1,'4/1/2020 10:00',12.,'0-0','UK'])
    prepare(raw,tmp_path,'fixture')
    params = dict(source_version='fixture',churn_days=90)
    bootstrap = contracts.build_context(tmp_path,'bootstrap','seed','2020-03-31',params,kind='bootstrap')
    run_stage('etl',bootstrap['context_path'])
    meta = models.build_training_context(tmp_path,'train','rf','2020-04-01',dict(
        source_version='fixture',rfm_k=2,effective_from='2020-04-02',model_kind='classifier',
        classifier_train_date='2020-01-01',classifier_validation_date='2020-02-01',classifier_test_date='2020-03-01',
        num_trees_grid=[4],max_depth_grid=[3],min_macro_f1=0))
    context = contracts.load_context(meta['context_path'],tmp_path)
    spark = SparkSession.builder.master('local[1]').appName('classifier-lifecycle').getOrCreate()
    try:
        candidate = train_model(spark,context,tmp_path)
    finally:
        spark.stop()
    models.publish_model(tmp_path,context)
    manifest = models.read_model(tmp_path,candidate,verify_artifact=True)
    assert manifest['model_kind']=='classifier'
    assert manifest['training_cutoff']=='2020-04-01'
    assert manifest['supervised_info']['test_scope']=='pre_refit_model_only'
    daily = contracts.build_context(tmp_path,'daily','rf','2020-04-01',params)
    monkeypatch.setattr(Pipeline,'fit',lambda *a,**k: pytest.fail('Daily must never train'))
    run_stage('etl',daily['context_path'])
    run_stage('rfm',daily['context_path'])
    record = contracts.read_json(Path(daily['context_path']).with_name('rfm.json'))
    assert record['metrics']['customers']==32
    assert record['model']['model_version']==manifest['model_version']
