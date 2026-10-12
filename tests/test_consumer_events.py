"""Pure event matching checks, runnable without Airflow or Spark installed."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from retail_support.consumer_events import publication_events  # noqa: E402


class Asset:
    def __init__(self, uri):
        self.uri = uri


class PublicationEventsTests(unittest.TestCase):
    def setUp(self):
        self.rfm = Asset("file:///rfm")
        self.audit = Asset("file:///audit")
        self.extra = {
            "run_date": "2011-12-10",
            "run_key": "run-a",
            "source_dag_id": "ecommerce_etl_dag",
            "source_run_id": "manual-a",
            "published_pointer": "/data/published/2011-12-10.json",
            "completion_manifest": "/data/runs/run-a/completion.json",
        }
        self.event = SimpleNamespace(extra=self.extra)

    def test_accepts_asset_and_uri_keys_and_deduplicates_retries(self):
        result = publication_events(
            {self.rfm: [self.event, self.event], self.audit.uri: [self.event]},
            [self.rfm],
        )
        self.assertEqual(result, [self.extra])

    def test_keeps_all_coalesced_events(self):
        other = dict(self.extra, run_key="run-b", source_run_id="manual-b")
        result = publication_events(
            {self.rfm.uri: [self.event, SimpleNamespace(extra=other)]},
            [self.rfm],
        )
        self.assertEqual(result, [self.extra, other])

    def test_rejects_manual_run_and_missing_asset(self):
        for events in (None, {}, {self.audit: [self.event]}):
            with self.subTest(events=events), self.assertRaises(ValueError):
                publication_events(events, [self.rfm])

    def test_rejects_incomplete_and_conflicting_metadata(self):
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            publication_events(
                {self.rfm: [SimpleNamespace(extra={})]}, [self.rfm]
            )
        other = SimpleNamespace(
            extra=dict(
                self.extra, completion_manifest="/different/completion.json"
            )
        )
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            publication_events({self.rfm: [self.event, other]}, [self.rfm])

    def early(self, **changes):
        value = {
            key: val
            for key, val in self.extra.items()
            if key not in {"published_pointer", "completion_manifest"}
        }
        return dict(
            value,
            publication_version=2,
            context_hash="context-a",
            etl_hash="etl-a",
            rfm_hash="rfm-a",
            rfm_manifest="/data/runs/run-a/rfm_publication.json",
            **changes,
        )

    def test_marketing_needs_no_final_completion(self):
        early = self.early()
        self.assertEqual(
            publication_events(
                {self.rfm: [SimpleNamespace(extra=early)]}, [self.rfm]
            ),
            [early],
        )


if __name__ == "__main__":
    unittest.main()
