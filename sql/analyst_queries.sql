-- Replace workspace with your catalog if different.
USE CATALOG workspace;
USE SCHEMA creditwatch_portfolio_v1;

-- Chart: month on X; delayed_account_rate on Y, formatted as a percentage.
SELECT month, accounts, delayed_accounts, delayed_account_rate,
       positive_bill_ntd, delayed_bill_ntd, delayed_bill_share,
       undocumented_status_accounts
FROM gold_monthly ORDER BY month;

-- Latest-month concentration by limit band. These are snapshot limits, not historical limits.
SELECT limit_band, accounts, delayed_accounts, delayed_account_rate, positive_bill_ntd
FROM gold_segments
WHERE month = (SELECT MAX(month) FROM gold_monthly)
ORDER BY delayed_account_rate DESC;

-- Deterioration alerts. Percentage points are already scaled by 100.
SELECT * FROM gold_alerts WHERE alert_status = 'REVIEW' ORDER BY month;

-- Operational status: distinguish source data period from processing time.
SELECT run_id, completed_at, status, raw_rows, exact_duplicates,
       rejected_rows, accepted_accounts, monthly_rows, message
FROM pipeline_runs ORDER BY completed_at DESC LIMIT 20;

-- Rejection triage. Inspect details only with authorized raw-data access.
SELECT run_id, rejection_reason, COUNT(*) AS rejected_rows
FROM quarantine_records GROUP BY run_id, rejection_reason;

-- Grain checks should return no rows.
SELECT account_id, month, COUNT(*) AS copies
FROM silver_repayments GROUP BY account_id, month HAVING COUNT(*) > 1;
