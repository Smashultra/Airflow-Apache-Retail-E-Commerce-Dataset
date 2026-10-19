"""Train a RandomForest to imitate a frozen KMeans teacher; never future churn."""
import argparse
import json

from pyspark.sql import SparkSession

from pyspark_rfm import assert_separate_paths, load_rfm_model, read_store_rfm
from rfm_supervised import check_dates, evaluate, fit_student


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('store-path', 'teacher-path', 'train-date', 'validation-date', 'test-date', 'model-output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--min-macro-f1', type=float, default=0.8)
    args = parser.parse_args()
    check_dates(args.train_date, args.validation_date, args.test_date)
    if not 0 <= args.min_macro_f1 <= 1:
        parser.error('Thresholds must be in [0,1]')
    spark = SparkSession.builder.appName('TrainRFMClassifier').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
    try:
        for path in (args.store_path, args.teacher_path):
            assert_separate_paths(spark, path, args.model_output)
        teacher = load_rfm_model(args.teacher_path)
        metadata_rows = spark.read.text(args.teacher_path.rstrip('/') + '/rfm_training_info').collect()
        if len(metadata_rows) != 1:
            raise ValueError('Teacher training provenance is required')
        teacher_info = json.loads(metadata_rows[0]['value'])
        if teacher_info['run_date'] != args.train_date:
            raise ValueError('Teacher cutoff must equal classifier training cutoff')
        k = teacher.stages[-1].getK()
        train, train_manifest = read_store_rfm(spark, args.store_path, args.train_date)
        if train_manifest['run_date'] != args.train_date:
            raise ValueError('Training requires a snapshot at the exact cutoff')
        if train_manifest['version'] != teacher_info['store_version']:
            raise ValueError('Teacher and student training snapshots differ')
        student = fit_student(train, teacher, args.seed)
        reports = {}
        snapshots = {'train': train_manifest['version']}
        for split, cutoff in [('validation', args.validation_date), ('test', args.test_date)]:
            rfm, manifest = read_store_rfm(spark, args.store_path, cutoff)
            if manifest['run_date'] != cutoff:
                raise ValueError('Evaluation requires a snapshot at the exact cutoff')
            reports[split] = evaluate(rfm, teacher, student)
            snapshots[split] = manifest['version']
        info = dict(schema_version=2, target='frozen_kmeans_pseudo_label', k=k, model_id=student.uid,
                    train_date=args.train_date, validation_date=args.validation_date,
                    test_date=args.test_date, snapshots=snapshots,
                    seed=args.seed, reports=reports,
                    min_macro_f1=args.min_macro_f1, teacher_path=args.teacher_path)
        print(json.dumps(info, ensure_ascii=False, sort_keys=True))
        if any(report['macro_f1'] < args.min_macro_f1 for report in reports.values()):
            raise ValueError('Temporal pseudo-label agreement gate failed; no bundle released')
        # New immutable directory. Metadata is the readiness marker written last.
        student.write().save(args.model_output.rstrip('/') + '/student')
        teacher.write().save(args.model_output.rstrip('/') + '/teacher')
        spark.createDataFrame([(json.dumps(info, sort_keys=True),)], ['value']).coalesce(1).write.text(
            args.model_output.rstrip('/') + '/supervised_info')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
