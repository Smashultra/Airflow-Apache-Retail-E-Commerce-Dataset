# Anomalies EDA — approved implementation plan

Owner: Codex. Branch: main. Scope: notebook only, no detector, cleaning, RFM or Airflow changes; no commits or data writes.

1. Create Vietnamese notebooks/EDA_Anomalies.ipynb. Read existing CSV and anomalies Parquet, fingerprint every input, normalize comparison types and reconcile content without regenerating data.
2. Compare missing/present CustomerID cohorts, invoice identity completeness, numeric distributions, month/country/product composition and signed versus positive sales values.
3. Add overlapping observed business flags and explicitly hypothetical description flags, pairwise intersections, and per-analysis eligibility counts.
4. Evaluate six business rules and product-level Quantity/UnitPrice/line-value upper IQR fences (1.5 and 3; minimum 30; skip zero IQR). Report applicability, missing-ID coverage, sensitivity and bounded examples.
5. Inspect invoice/product/customer and possible reversal context for important cases; distinguish observation, inference and uncertainty. End with evidence-grounded recommendations and a provisional detector output design.
6. Execute fresh kernel using nbclient/nbconvert; assertions cover cohort reconciliation, row preservation, IQR boundaries, missing fields/IDs and skipped groups. Verify input hashes unchanged, inspect charts and outputs, update TODO/HANDOFF and archive this plan.

Implementation notes: stay in the existing checkout to use local ignored datasets and preserve current .agent edits. No worktree or commits required for this notebook-only approved scope. Focused runnable checks live inside the notebook as authorized by the plan. No extra dependencies or exported datasets.

Verification: complete. Fresh-kernel nbconvert execution exited 0; all 10 code cells passed, no error outputs, input hashes unchanged; both figures visually inspected. Independent review fixes verified in final run. See HANDOFF for findings and command.
