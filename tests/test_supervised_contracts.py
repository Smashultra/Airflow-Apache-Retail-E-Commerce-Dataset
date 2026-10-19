"""Fast contracts without Spark: execute the actual CLI/date/daily functions."""
import argparse
import ast
import re
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def functions(filename, names, namespace):
    tree = ast.parse((ROOT / 'scripts' / filename).read_text(encoding='utf-8'))
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=body, type_ignores=[]), filename, 'exec'), namespace)
    return namespace

class DirectClassifierContracts(unittest.TestCase):
    def test_daily_cli_needs_no_previous_labels(self):
        ns = functions('pyspark_rfm.py', {'parse_args', 'add_store_arguments', 'validate_store_arguments'}, dict(argparse=argparse, re=re, date=date, Path=Path, DATA_ROOT=Path('data'), DEFAULT_NUM_BUCKETS=64))
        base = ['--input', 'batch', '--batch-id', 'b1', '--run-date', '2020-01-10', '--model-path', 'models/v2']
        args = ns['parse_args'](base + ['--model-kind', 'classifier'])
        self.assertEqual(args.model_kind, 'classifier')
        self.assertEqual(ns['parse_args'](base).model_kind, 'kmeans')
        self.assertFalse(hasattr(args, 'previous_output'))
        for removed in ['--bootstrap-segments', '--previous-output', '--vip-cluster']:
            with self.assertRaises(SystemExit): ns['parse_args'](base + [removed])

    def test_daily_returns_direct_score_without_prior_state(self):
        model, population, predicted = object(), object(), object()
        calls = []
        def load(spark, path, cutoff):
            calls.append((path, cutoff))
            return model, {'k': 3}
        def predict(frame, fitted, version):
            self.assertIs(frame, population)
            self.assertIs(fitted, model)
            self.assertEqual(version, 'v2')
            return predicted
        ns = functions('rfm_supervised.py', {'daily'}, dict(load_bundle=load, score=predict))
        result, k = ns['daily'](None, population, 'bundle', '2020-01-10', 'v2')
        self.assertIs(result, predicted)
        self.assertEqual(k, 3)
        self.assertEqual(calls, [('bundle', '2020-01-10')])

    def test_temporal_order_still_required(self):
        ns = functions('rfm_supervised.py', {'check_dates'}, dict(date=date))
        ns['check_dates']('2020-01-01', '2020-02-01', '2020-03-01')
        for dates in [('2020-02-01','2020-01-01','2020-03-01'), ('2020-01-01','2020-02-01','2020-02-01')]:
            with self.assertRaises(ValueError): ns['check_dates'](*dates)

if __name__ == '__main__':
    unittest.main()
