# Repair validation — 2026-10-19

Nine Spark contract tests passed. Full local Spark 3.5.3 / Delta 3.2.1
execution also passed on the bundled 30,000-account dataset:

- First publication and replay each exposed 30,000 accounts / 180,000 account-months.
- Bronze replay did not duplicate source records.
- An all-invalid snapshot failed and preserved the previous successful publication.
- Independent pandas reconciliation matched monthly counts and monetary totals.

Fresh results are in `evidence/local_validation.json` and the adjacent CSVs.
The executed 2026-09-16 Databricks notebook was inspected and retains SUCCESS
output and integrity checks. No new Databricks workspace run was performed.

This container needed `SPARK_LOCAL_IP=127.0.0.1`. Maven's Java resolver could
not resolve its default host, so the documented `--jars` option was used with
Delta Spark 3.2.1 and Delta Storage 3.2.1 jars downloaded from Maven Central.
No source or transformation logic was changed to obtain the passing result.
