# CreditWatch — Databricks Credit Repayment Risk Lakehouse

CreditWatch addresses a practical risk-analytics problem: repayment records need consistent definitions, duplicate handling and quality checks before analysts can trust portfolio reports. The implementation converts raw records into monthly repayment facts, credit-limit segments and deterioration alerts. A failed batch does not replace the last successfully published dataset.

Built with **PySpark, SQL, Delta Lake and Databricks-compatible notebooks**. This is a working historical-data demonstration. It is not connected to a bank, a live feed, or Mastercard.

## Start here

1. Extract this project ZIP on your computer.
2. In Databricks **Workspace**, import `notebooks/CreditWatch_Databricks.ipynb` (the `.py` source alternative is also included).
3. Attach Python serverless compute. Run the setup cell to choose your catalog and create the `creditwatch_portfolio_v1` schema and `inbound` volume.
4. In **Catalog**, open that volume and upload `data/uci_credit.csv`. Keep the file unchanged.
5. Run all notebook cells. The notebook checks the source hash, executes the pipeline, validates the output grain and displays results.
6. Run the pipeline cell again to verify replay. Expect **30,000 current accounts and 180,000 current account-months**, not twice those values.

The code does not require an OAuth client ID, API token or a ChatGPT connector when you run it directly inside Databricks. Your account needs permission to create schemas, volumes, tables and views. Use an existing writable catalog if the default is restricted.

**Execution status:** locally executed with Spark 3.5.3 and Delta Lake 3.2.1; workspace execution has not been performed. See `evidence/local_validation.json`, `evidence/test_results.txt` and `evidence/RESULTS.md`. Databricks permissions, serverless compatibility and job execution must be established by the first workspace run.

## What the project implements

- Raw string ingestion with source provenance and content hashes.
- Delta MERGE for replay-safe bronze ingestion.
- Exact duplicate removal; all conflicting rows for an account are quarantined.
- Integer, domain, credit-limit and payment validation, with explicit reasons.
- A configurable rejection-rate gate; empty usable data cannot publish.
- Account dimension and six monthly repayment facts per accepted account.
- Append-only run versions and an audit-based publication pointer.
- Monthly and credit-limit-band SQL reporting views.
- Month-over-month deterioration flags, expressed in percentage points.
- Run audits, row-count reconciliation, referential/grain checks and failure tests.

## Data and scope

The included CSV derives from **Yeh, I. (2009), Default of Credit Card Clients, UCI**, DOI https://doi.org/10.24432/C55S3H. License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). It contains 30,000 Taiwanese credit-card accounts and six months of repayment history (April–September 2005). All monetary amounts are **NTD**, not USD. `data/provenance.json` records original/download URLs, checksums, attribution and the conversion performed.

The CSV contains the original fields; its outcome header was renamed to `default_next_month`. No synthetic customers or invented outcomes were added. Invalid data used by tests is deliberately injected into disposable test tables and is not blended into the source CSV.

### Metric definitions

| Metric | Definition | Interpretation |
|---|---|---|
| Accounts | Distinct accepted account-month records | One row per account each month |
| Delayed account rate | Count with repayment status >= 2 / all accepted accounts that month | Source-reported delay of at least two months, not exact 60+ DPD |
| Positive bill amount | Sum of max(bill amount, 0) | Observed positive statement balances, not regulatory EAD |
| Delayed bill share | Positive bills on status >= 2 accounts / all positive bills | Null when denominator is zero |
| Undocumented status accounts | Count with status -2 or 0 | Codes appear in the file but are not defined in the cited source description |
| Deterioration alert | Current delayed account rate minus prior month >= 1 percentage point by default | Illustrative review threshold, not a bank-approved rule |

Negative statement balances are valid credit balances, so they remain in silver. Codes -2 and 0 are preserved and reported; they are not assigned invented meanings. They remain in the all-account denominator. The supplied credit limit is one snapshot value and is used only for segmentation; it is not treated as a changing monthly limit. Never sum monthly balances across periods to describe portfolio exposure.

The future-default label is excluded from operational reporting. Demographics are retained only in raw/quarantine records, not used for segment selection or alerts. The project makes no lending decisions, forecasts, compliance determinations or loss-reduction claims.

## Pipeline layout

| Layer | Object | Grain / purpose |
|---|---|---|
| Bronze | `bronze_records` | Source SHA + distinct row hash; raw lineage |
| Quality | `quarantine_records` | Rejected row per run, with reason |
| Versions | `account_versions` | Run + account |
| Versions | `repayment_versions` | Run + account + month |
| Publication | `pipeline_runs` | Run status and reconciled counts |
| Silver | `silver_accounts`, `silver_repayments` | Latest successful run only |
| Gold | `gold_monthly` | Month |
| Gold | `gold_segments` | Month + limit band |
| Gold | `gold_alerts` | Month and prior-month comparison |

This is a **full-snapshot batch pipeline**. Bronze is merged rather than appended blindly. Silver versions intentionally accumulate across runs so failed and successful processing can be audited; current reporting reads only the most recent SUCCESS run. It does not claim CDC, streaming, or multi-table transactional writes. Under a single writer and unchanged view definitions, the final success marker prevents partial data runs becoming visible. A schema/code migration needs separate deployment controls.

## Orchestration in Databricks

Create a **Lakeflow Job** with one notebook task, choose this notebook, use serverless compute and set **maximum concurrent runs to 1**. Parameters are `catalog`, `schema`, `source_path`, `max_reject_rate` and `alert_threshold_pp`. Run on demand; scheduling a static dataset daily adds no new business information.

The included `databricks.yml` is an optional declarative deployment definition. With an authenticated Databricks CLI and a supported workspace, set its variables and run:

```bash
databricks bundle validate --var catalog=workspace --var schema=creditwatch_portfolio_v1
databricks bundle deploy --var catalog=workspace --var schema=creditwatch_portfolio_v1
databricks bundle run creditwatch_job --var catalog=workspace --var schema=creditwatch_portfolio_v1
```

Upload the CSV to the configured volume before running the job. The CLI deployment has not been executed here. The browser notebook route is the simplest first run.

## Reproduce local verification

Python 3.10+ and Java 17 are required for the pinned local Spark version.

```bash
python -m pip install -r requirements-local.txt
python -m pytest tests -q
python run_local.py
```

`run_local.py` resolves Delta jars through Maven unless `--jars` supplies local paths. It creates a disposable local warehouse, publishes real data, replays it, attempts an all-invalid snapshot, and checks that the previous successful run remains visible. It independently reconciles monthly results using pandas. CSV results and JSON evidence are saved under `evidence/`.

The test suite covers malformed input, invalid money values, exact/conflicting duplicates, negative bill balances, undocumented source codes, label exclusion, month ordering and failure gates. Local execution does not prove Databricks deployment.

## Operating guide

- **Bad source checksum:** upload the supplied CSV unchanged. For a new data source, update contract, month mapping and provenance together.
- **Quality gate failure:** inspect `pipeline_runs` and `quarantine_records`; correct the source and rerun. Do not simply raise the threshold to hide failures.
- **Partial write / infrastructure failure:** rerun after fixing the cause. Uncommitted silver versions do not appear in reporting views. Bronze MERGE remains replay-safe.
- **Permissions error:** use a writable catalog/schema or have the workspace administrator grant the necessary access. No broad permission changes are made by this project.
- **Serverless quota exceeded:** resume after capacity is available. The dataset is small, but workspace quotas still apply.
- **History retention:** no automatic deletion is included. Before repeated production use, define retention for raw data, failed versions and audit logs.

## Resume use

Before workspace execution, describe this as a **Databricks-targeted PySpark/Delta project tested locally**. After a successful workspace run, retain notebook/job output and report the actual verified rows, tables and tests. Do not claim real production ownership, cost savings or reduced defaults from this demonstration.

## Documentation sources

- Dataset and license: https://archive.ics.uci.edu/dataset/350/default+of+credit+card+clients
- Delta MERGE: https://docs.databricks.com/aws/en/delta/merge
- Free Edition: https://docs.databricks.com/aws/en/getting-started/free-edition
- Serverless limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
- Unity Catalog volumes: https://docs.databricks.com/aws/en/volumes/files
