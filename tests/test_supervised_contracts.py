"""Direct imports: hand-computed metrics, CLI and release contracts."""
import pytest
import retail_contracts as contracts
import retail_models as models
from retail_contracts import check_dates, confusion_metrics, choose_candidate, parameter_grid
from train_rfm_classifier import parse_args
from pyspark_rfm import parse_args as daily_args


def test_macro_excludes_unused_but_counts_false_positive_class():
    report = confusion_metrics([dict(label=0, predicted=0, count=8), dict(label=0, predicted=1, count=2)], 4)
    assert report['agreement'] == 0.8
    assert report['macro_f1'] == pytest.approx((16/18 + 0) / 2)
    assert report['macro_labels'] == [0, 1]
    assert report['per_class'][2]['f1'] is None
    perfect = confusion_metrics([dict(label=0, predicted=0, count=10)], 4)
    assert perfect['macro_f1'] == 1
    assert confusion_metrics([], 4)['macro_f1'] is None


def test_handcomputed_precision_recall():
    r = confusion_metrics([dict(label=0, predicted=0, count=3), dict(label=0, predicted=1, count=1),
                           dict(label=1, predicted=0, count=2), dict(label=1, predicted=1, count=4)], 2)
    assert r['per_class'][0]['precision'] == pytest.approx(3/5)
    assert r['per_class'][0]['recall'] == pytest.approx(3/4)
    assert r['macro_f1'] == pytest.approx((6/9 + 8/11)/2)


def candidate(n, d, new, all_score=1):
    return dict(params=dict(num_trees=n, max_depth=d), validation={'new': {'macro_f1': new}, 'all': {'macro_f1': all_score}})


def test_selection_uses_new_validation_only_and_smaller_tiebreak():
    choices = [candidate(60, 8, .8), candidate(150, 12, .9), candidate(150, 8, .9, .5)]
    assert choose_candidate(choices, .85)['params'] == dict(num_trees=150, max_depth=8)
    with pytest.raises(ValueError, match='gate failed'):
        choose_candidate(choices, .95)
    with pytest.raises(ValueError, match='No new'):
        choose_candidate([candidate(60, 8, None)], .8)


def test_training_cli_grid_and_refit_dates():
    base = ['--store-path','store','--teacher-path','teacher','--train-date','2020-01-01',
            '--validation-date','2020-02-01','--test-date','2020-03-01','--model-output','new']
    args = parse_args(base + ['--num-trees-grid','10','20','--max-depth-grid','3','5'])
    assert len(parameter_grid(args.num_trees_grid, args.max_depth_grid)) == 4
    assert args.refit_date == args.test_date
    with pytest.raises(ValueError):
        parse_args(base + ['--refit-date','2020-02-15'])
    with pytest.raises(ValueError):
        parameter_grid([0], [8])
    with pytest.raises(ValueError):
        check_dates('2020-02-01','2020-01-01','2020-03-01')


def test_daily_cli_selects_classifier_without_prior_labels():
    args = daily_args(['--input','batch','--batch-id','b1','--run-date','2020-04-01',
                       '--model-path','model/v3','--model-kind','classifier'])
    assert args.model_kind == 'classifier'


# Registry contracts.

def test_classifier_training_context_freezes_split_grid(tmp_path, monkeypatch):
    etl = {'outputs': {}, 'fixture': True}
    source = {'run_date': '2020-04-01', 'source_version': 'v1', 'source_hash': 'hash'}
    monkeypatch.setattr(models, 'read_training_etl', lambda *a: (etl, source))
    params = dict(source_version='v1', rfm_k=2, effective_from='2020-04-02', etl_manifest='etl/x.json',
                  model_kind='classifier', num_trees_grid=[4,8], max_depth_grid=[2,3])
    result = models.build_training_context(tmp_path, 'train', 'run', '2020-04-01', params)
    assert result['classifier_train_date'] == '2020-01-02'
    assert result['classifier_test_date'] == '2020-03-02'
    assert result['num_trees_grid'] == [4,8]
    with pytest.raises(ValueError, match='inputs changed'):
        models.build_training_context(tmp_path, 'train', 'run', '2020-04-01', {**params,'num_trees_grid':[20]})


def test_classifier_bundle_corruption_rejected(tmp_path):
    bundle = tmp_path/'models/bundle'
    (bundle/'student/metadata').mkdir(parents=True)
    (bundle/'student/metadata/part-00000').write_text('pipeline')
    (bundle/'supervised_info').mkdir()
    (bundle/'supervised_info/part-00000').write_text('metadata')
    model = dict(complete=True, model_kind='classifier',model_version='v1',feature_version=models.FEATURE_VERSION,
                 k=2,training_cutoff='2020-04-01',pipeline_path='models/bundle/student',bundle_path='models/bundle',
                 supervised_info=dict(schema_version=3,fit_cutoff='2020-04-01',k=2),
                 artifact_hashes=models.artifact_hashes(tmp_path,'models/bundle/student'),
                 bundle_hashes=models.bundle_hashes(tmp_path,'models/bundle'))
    contracts.atomic_json(tmp_path/'models/model.json',model)
    binding=dict(model_version='v1',model_manifest='models/model.json',model_manifest_hash=contracts.json_hash(model))
    assert models.read_model(tmp_path,binding,verify_artifact=True)['model_kind']=='classifier'
    (bundle/'supervised_info/part-00000').write_text('changed')
    with pytest.raises(ValueError,match='bundle hash'):
        models.read_model(tmp_path,binding,verify_artifact=True)
