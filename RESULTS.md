# Executed results

Environment: local Apache Spark 3.5.3 and Delta Lake 3.2.1. Databricks workspace execution is pending.

## Verified pipeline behavior

- 30,000 real source accounts accepted; no source records rejected.
- 180,000 monthly repayment facts created across six historical months.
- Replaying the same source kept Bronze at 30,000 records and current reporting at 180,000 facts.
- An injected all-invalid batch failed; the previous successful run remained visible.
- Every monthly delayed-account count/rate and positive/delayed bill sum reconciled independently against pandas.

## Historical results

| Month | Accounts with status >= 2 | Share of all accounts | Positive bills (NTD) |
|---|---:|---:|---:|
| 2005-04-01 | 3,079 | 10.26% | 1,168,268,063 |
| 2005-05-01 | 2,968 | 9.89% | 1,210,412,763 |
| 2005-06-01 | 3,508 | 11.69% | 1,298,989,558 |
| 2005-07-01 | 4,209 | 14.03% | 1,411,355,065 |
| 2005-08-01 | 4,410 | 14.70% | 1,476,195,541 |
| 2005-09-01 | 3,130 | 10.43% | 1,537,381,257 |

The illustrative 1-percentage-point deterioration rule flags June (+1.80 pp) and July (+2.34 pp). This is a retrospective result on 2005 data, not a prediction or an estimate of current bank risk.

Codes -2 and 0 are counted as undocumented and included in the all-account denominator. Delayed-account rate uses only the explicitly positive status >= 2 criterion. This prevents undocumented codes from being silently relabeled as known current accounts.

## Evidence files

- `local_validation.json`: run IDs, counts, replay and failure checks.
- `test_results.txt`: adversarial Spark tests.
- `monthly_results.csv`, `segment_results.csv`, `alert_results.csv`: executed query outputs.

## What remains unverified

- Databricks notebook execution, catalog permissions and serverless runtime behavior.
- Databricks bundle validation/deployment and Lakeflow Job execution.
- Any live-data ingestion, measured business savings or credit-loss reduction.
