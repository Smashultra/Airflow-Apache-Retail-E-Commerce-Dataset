"""Tune on new validation customers, evaluate once, refit a deployment classifier."""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark_rfm import assert_separate_paths, load_rfm_model, read_store_rfm
from rfm_supervised import check_dates, parameter_grid, select_and_refit, save_bundle


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('store-path', 'teacher-path', 'train-date', 'validation-date', 'test-date', 'model-output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--refit-date', help='Latest snapshot for production fit; defaults to test-date')
    parser.add_argument('--num-trees-grid', nargs='+', type=int, default=[60, 150])
    parser.add_argument('--max-depth-grid', nargs='+', type=int, default=[4, 8, 12])
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--min-macro-f1', type=float, default=0.8)
    args = parser.parse_args(argv)
    args.refit_date = args.refit_date or args.test_date
    check_dates(args.train_date, args.validation_date, args.test_date, args.refit_date)
    parameter_grid(args.num_trees_grid, args.max_depth_grid)
    if not 0 <= args.min_macro_f1 <= 1:
        parser.error('Validation threshold must be in [0,1]')
    return args


def run_training(spark, args):
    for path in (args.store_path, args.teacher_path):
        assert_separate_paths(spark, path, args.model_output)
    teacher = load_rfm_model(args.teacher_path)
    rows = spark.read.text(args.teacher_path.rstrip('/') + '/rfm_training_info').take(2)
    if len(rows) != 1:
        raise ValueError('Teacher training provenance is required')
    teacher_info = json.loads(rows[0]['value'])
    if teacher_info['run_date'] != args.train_date:
        raise ValueError('Teacher cutoff must equal classifier training cutoff')
    frames, snapshots = {}, {}
    for split, cutoff in [('train', args.train_date), ('validation', args.validation_date),
                          ('test', args.test_date), ('refit', args.refit_date)]:
        frame, manifest = read_store_rfm(spark, args.store_path, cutoff)
        if manifest['run_date'] != cutoff:
            raise ValueError('Each split requires a snapshot at the exact cutoff')
        frames[split], snapshots[split] = frame, manifest['version']
    if snapshots['train'] != teacher_info['store_version']:
        raise ValueError('Teacher and student training snapshots differ')
    student, info = select_and_refit(frames['train'], frames['validation'], frames['test'], frames['refit'], teacher,
                                    train_date=args.train_date, validation_date=args.validation_date,
                                    test_date=args.test_date, refit_date=args.refit_date,
                                    trees=args.num_trees_grid, depths=args.max_depth_grid,
                                    seed=args.seed, minimum=args.min_macro_f1)
    info.update(snapshots=snapshots, teacher_path=args.teacher_path)
    save_bundle(spark, args.model_output, student, teacher, info)
    return info


def main(argv=None):
    args = parse_args(argv)
    spark = SparkSession.builder.appName('TrainRFMClassifier').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
    try:
        info = run_training(spark, args)
        print(json.dumps(info, sort_keys=True))
        return info
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
