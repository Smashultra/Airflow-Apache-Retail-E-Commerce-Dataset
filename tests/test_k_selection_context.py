"""Context binding checks runnable without Airflow or a Spark JVM."""
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from retail_contracts import json_hash
from select_kmeans_k import parse_args, resolve_inputs, report_metadata


class KSelectionContextTests(unittest.TestCase):
    def test_standalone_and_ambiguous_arguments(self):
        args = parse_args(["--input", "curated", "--run-date", "2011-06-01",
                           "--output", "report"])
        self.assertIsNone(resolve_inputs(args))
        with self.assertRaises(SystemExit):
            parse_args(["--pipeline-context", "context.json", "--input", "other"])

    def test_classifier_teacher_cutoff_and_report_keep_training_cutoff(self):
        with TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            etl = {"outputs": {"transactions": "curated/frozen"}}
            context = dict(kind="training", run_key="abc", run_date="2011-11-01",
                           classifier_train_date="2011-08-03", etl_manifest="etl.json",
                           etl_hash=json_hash(etl), seed=42, rfm_k=2)
            with (patch("retail_contracts.data_root", return_value=root),
                  patch("retail_contracts.load_context", return_value=context),
                  patch("retail_models.read_training_etl", return_value=(etl, {"run_date": "2011-11-01"}))):
                args = parse_args(["--pipeline-context", "context.json"])
                resolve_inputs(args)
                self.assertEqual(args.run_date, "2011-08-03")
                report = report_metadata(context, args, 100, [(2, 10.0, 0.8)])
                self.assertEqual(report["training_cutoff"], "2011-11-01")
                self.assertEqual(report["teacher_cutoff"], "2011-08-03")
                self.assertEqual(report["etl_hash"], json_hash(etl))
                del context["classifier_train_date"]
                resolve_inputs(args)
                report = report_metadata(context, args, 100, [])
                self.assertEqual(report["training_cutoff"], report["teacher_cutoff"])

    def test_pinned_snapshot_cutoff_seed_and_report_path(self):
        with TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            etl = {"outputs": {"transactions": "curated/frozen"}}
            context = {"kind": "training", "run_key": "abc", "run_date": "2011-06-01",
                       "etl_manifest": "manifests/etl.json", "etl_hash": json_hash(etl),
                       "seed": 42, "rfm_k": 4}
            with (patch("retail_contracts.data_root", return_value=root),
                  patch("retail_contracts.load_context", return_value=context),
                  patch("retail_models.read_training_etl",
                        return_value=(etl, {"run_date": "2011-06-01"})) as reader):
                args = parse_args(["--pipeline-context", "context.json"])
                self.assertEqual(resolve_inputs(args), context)
                self.assertEqual(args.input, str(root / "curated/frozen"))
                self.assertEqual(args.run_date, "2011-06-01")
                self.assertEqual(args.output, str(root / "analytics/k_selection/abc"))
                self.assertEqual(args.seed, 42)
                self.assertEqual(context["rfm_k"], 4)
                reader.return_value = (etl, {"run_date": "2011-07-01"})
                with self.assertRaisesRegex(ValueError, "cutoff changed"):
                    resolve_inputs(args)
                reader.return_value = ({**etl, "changed": True}, {"run_date": "2011-06-01"})
                with self.assertRaisesRegex(ValueError, "generation"):
                    resolve_inputs(args)
                context["kind"] = "daily"
                with self.assertRaisesRegex(ValueError, "training context"):
                    resolve_inputs(args)


if __name__ == "__main__":
    unittest.main()
