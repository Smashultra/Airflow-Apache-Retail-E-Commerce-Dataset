# RFM review correction

Owner Codex, current branch codex/rfm-supervised-20261019. No commit/push.

1. Select RF hyperparameters using new-customer validation macro-F1, report all/seen/new cohorts, define absent-label convention.
2. Keep test out of selection/release gate; evaluate once, then refit student at latest cutoff with fixed teacher. Separate evaluated vs deployed model provenance.
3. Validate complete pipeline/metadata contracts and normalize output partition schema/retry.
4. Integrate classifier with existing model registry, monthly training and daily dispatch; retire obsolete patch.
5. Replace AST tests with direct imports; add metric, temporal/provenance/gate, persistence/output, and workflow regression coverage.
6. Run available Spark tests; document platform limitations and distinguish peer-reported numbers from reproduced evidence.

Implementation complete in this review checkout. Available checks: 35 passed;
four filesystem/lifecycle tests explicitly skipped on Windows without native Hadoop.
Keep plan active for project Docker persistence/lifecycle and actual Airflow verification.
No commit/push or live model activation. See docs/RFM_SUPERVISED_VI.md.
