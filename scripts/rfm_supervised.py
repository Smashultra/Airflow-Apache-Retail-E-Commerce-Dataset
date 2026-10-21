"""Temporal selection and direct numeric prediction of frozen KMeans pseudo-labels."""
import json
import math
from datetime import date

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from retail_contracts import check_dates, choose_candidate, confusion_metrics, parameter_grid


def fit_student(rfm, teacher, seed=42, *, num_trees=60, max_depth=8):
    from pyspark_rfm import prepare_kmeans_features
    parameter_grid([num_trees], [max_depth])
    labeled = teacher.transform(prepare_kmeans_features(rfm))
    k = teacher.stages[-1].getK()
    if {int(r.Cluster) for r in labeled.select('Cluster').distinct().collect()} != set(range(k)):
        raise ValueError('Student fit snapshot must represent every teacher cluster')
    classifier = RandomForestClassifier(
        labelCol='Cluster', featuresCol='features', predictionCol='prediction',
        numTrees=num_trees, maxDepth=max_depth, seed=seed,
    ).fit(labeled)
    return PipelineModel(stages=[*teacher.stages[:2], classifier])


def evaluate(rfm, teacher, student, training_ids=None):
    """One inference pass, reported by all/seen/new relative to training IDs."""
    from pyspark_rfm import prepare_kmeans_features
    labels = teacher.transform(prepare_kmeans_features(rfm)).select('CustomerID', F.col('Cluster').alias('label'))
    predicted = student.transform(prepare_kmeans_features(rfm)).select('CustomerID', F.col('prediction').cast('int').alias('predicted'))
    joined = predicted.join(labels, 'CustomerID')
    if training_ids is None:
        cells = [r.asDict() for r in joined.groupBy('label', 'predicted').count().collect()]
        return confusion_metrics(cells, teacher.stages[-1].getK())
    known = training_ids.select('CustomerID').distinct().withColumn('_seen', F.lit(True))
    joined = joined.join(known, 'CustomerID', 'left').withColumn('cohort', F.when(F.col('_seen'), 'seen').otherwise('new'))
    cells = [r.asDict() for r in joined.groupBy('cohort', 'label', 'predicted').count().collect()]
    return {name: confusion_metrics([r for r in cells if name == 'all' or r['cohort'] == name], teacher.stages[-1].getK())
            for name in ('all', 'seen', 'new')}


def select_and_refit(train, validation, test, latest, teacher, *, train_date, validation_date,
                     test_date, refit_date, trees=(60, 150), depths=(4, 8, 12), seed=42, minimum=0.8):
    """Validation selects/gates. Test is evaluated once BEFORE deployment refit."""
    check_dates(train_date, validation_date, test_date, refit_date)
    ids = train.select('CustomerID').distinct()
    if validation.select('CustomerID').join(ids, 'CustomerID', 'left_anti').isEmpty():
        raise ValueError('No new validation customers; cannot select hyperparameters')
    candidates, best_model, best_key = [], None, None
    for params in parameter_grid(list(trees), list(depths)):
        model = fit_student(train, teacher, seed, **params)
        report = evaluate(validation, teacher, model, ids)
        candidate = dict(params=params, validation=report)
        candidates.append(candidate)
        key = (-report['new']['macro_f1'], params['num_trees'], params['max_depth'])
        if best_key is None or key < best_key:
            best_key, best_model = key, model
    winner = choose_candidate(candidates, minimum)
    # No selection, gate, retry search or refit is allowed to use this report.
    test_report = evaluate(test, teacher, best_model, ids)
    deployment = fit_student(latest, teacher, seed, **winner['params'])
    info = dict(schema_version=3, target='frozen_kmeans_pseudo_label', k=teacher.stages[-1].getK(),
                model_id=deployment.uid, evaluated_model_id=best_model.uid,
                train_date=train_date, validation_date=validation_date, test_date=test_date,
                fit_cutoff=refit_date, teacher_cutoff=train_date, seed=seed,
                selected_params=winner['params'], selection_metric='validation.new.macro_f1',
                min_macro_f1=minimum, candidates=candidates,
                reports={'validation': winner['validation'], 'test': test_report},
                test_scope='pre_refit_model_only', refit_policy='student_latest_snapshot_frozen_teacher')
    return deployment, info


def validate_student(model, info):
    from pyspark_rfm import LOG_FEATURES
    stages = model.stages
    if (len(stages) != 3 or not isinstance(stages[0], VectorAssembler)
            or not isinstance(stages[1], StandardScalerModel)
            or not isinstance(stages[2], RandomForestClassificationModel)):
        raise ValueError('Expected assembler, fitted scaler and RandomForest classifier')
    assembler, scaler, classifier = stages
    if (assembler.getInputCols() != LOG_FEATURES or assembler.getOutputCol() != 'raw_features'
            or scaler.getInputCol() != 'raw_features' or scaler.getOutputCol() != 'features'
            or not scaler.getWithMean() or not scaler.getWithStd()
            or classifier.getFeaturesCol() != 'features' or classifier.getPredictionCol() != 'prediction'
            or classifier.getLabelCol() != 'Cluster' or classifier.getProbabilityCol() != 'probability'
            or classifier.getRawPredictionCol() != 'rawPrediction'
            or classifier.numClasses != info['k'] or classifier.numFeatures != len(LOG_FEATURES)
            or model.uid != info['model_id']):
        raise ValueError('Classifier feature/output/model identity contract mismatch')
    if (len(scaler.mean) != len(LOG_FEATURES) or len(scaler.std) != len(LOG_FEATURES)
            or not all(math.isfinite(float(v)) for v in [*scaler.mean, *scaler.std])
            or any(float(v) < 0 for v in scaler.std)):
        raise ValueError('Invalid fitted scaler')
    params = info['selected_params']
    if classifier.getNumTrees != params['num_trees'] or classifier.getMaxDepth() != params['max_depth']:
        raise ValueError('Classifier hyperparameters differ from metadata')


def validate_info(info, run_date=None):
    if info.get('schema_version') != 3 or info.get('target') != 'frozen_kmeans_pseudo_label':
        raise ValueError('Unsupported supervised metadata; retrain with current contract')
    check_dates(info['train_date'], info['validation_date'], info['test_date'], info['fit_cutoff'])
    if type(info['k']) is not int or info['k'] < 2:
        raise ValueError('Invalid number of classes')
    if run_date is not None and date.fromisoformat(run_date) <= date.fromisoformat(info['fit_cutoff']):
        raise ValueError('Daily cutoff must be after deployment fit cutoff')
    parameter_grid([info['selected_params']['num_trees']], [info['selected_params']['max_depth']])


def save_bundle(spark, path, student, teacher, info):
    validate_info(info)
    validate_student(student, info)
    student.write().save(path.rstrip('/') + '/student')
    teacher.write().save(path.rstrip('/') + '/teacher')
    restored = PipelineModel.load(path.rstrip('/') + '/student')
    validate_student(restored, info)
    # Readiness marker last; incomplete folders are not serving artifacts.
    spark.createDataFrame([(json.dumps(info, sort_keys=True),)], ['value']).coalesce(1).write.text(path.rstrip('/') + '/supervised_info')


def load_bundle(spark, path, run_date):
    from pyspark_rfm import _store_filesystem
    fs, marker = _store_filesystem(spark, path.rstrip('/') + '/supervised_info/_SUCCESS')
    if not fs.exists(marker):
        raise ValueError('Incomplete supervised metadata publication')
    rows = spark.read.text(path.rstrip('/') + '/supervised_info').take(2)
    if len(rows) != 1:
        raise ValueError('Expected one supervised metadata record')
    info = json.loads(rows[0]['value'])
    validate_info(info, run_date)
    model = PipelineModel.load(path.rstrip('/') + '/student')
    validate_student(model, info)
    return model, info


def score(rfm, student, version):
    from pyspark_rfm import prepare_kmeans_features, LOG_FEATURES
    if not version.strip():
        raise ValueError('model_version must not be blank')
    return (student.transform(prepare_kmeans_features(rfm))
            .withColumn('Cluster', F.col('prediction').cast('int'))
            .withColumn('confidence', F.array_max(vector_to_array('probability')))
            .withColumn('model_version', F.lit(version))
            .withColumn('supervised_model_id', F.lit(student.uid))
            .drop(*LOG_FEATURES, 'raw_features', 'features', 'prediction', 'rawPrediction', 'probability'))


def daily(spark, rfm, bundle_path, run_date, version):
    model, info = load_bundle(spark, bundle_path, run_date)
    return score(rfm, model, version), info['k']
