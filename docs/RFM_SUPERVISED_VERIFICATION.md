# Verification - correction 2026-10-19

Scope: remove invented VIP naming, prior-label state, hysteresis and policy thresholds.
Daily now writes the classifier prediction directly to integer Cluster. Bundle contract v2
rejects old policy bundles. Temporal split, teacher provenance and evaluation remain.

Verified on this correction:
- python -B tests/test_supervised_contracts.py:
  3 passed. Executes actual extracted parser/date/daily functions without Spark imports;
  checks classifier needs no prior labels, removed flags are rejected, direct scoring
  delegation and temporal ordering. This is NOT Spark execution.
- Python AST checks for changed scripts/tests/DAG.
- Runtime regression test updated to compare Cluster with the fitted classifier prediction
  and verify absence of invented business labels. Not executed: current Python has no PySpark.

Historical 8 Spark / 23 CLI-context passes from 16 October exercised the old policy-bearing
version; they do NOT certify this corrected implementation.
No fresh full-data training, model persistence, Parquet retry or Airflow run verified.
Run tests/test_supervised_rfm.py and existing RFM/DAG tests in project Docker before release.

Remote branch codex/rfm-supervised-20261019 rolled back with explicit lease from d986dcf
to f671393. Main unchanged. The corrected version is prepared for the user-authorized review-branch push; main remains unchanged.
Shared workflow adaptation to upstream model registry is still required before deployment.

Publication checks: 3 contract tests passed again on the review checkout (0.052s); five Python files parsed. No Spark execution claimed.
