# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # CreditWatch — Credit Repayment Risk Lakehouse
# MAGIC ## Goal
# MAGIC Turn an archived credit-card dataset into auditable monthly portfolio reporting using **Databricks, PySpark, Delta Lake and SQL**. Analysts can inspect delinquency concentration and month-over-month deterioration, while bad input is quarantined and failed runs do not replace published data.
# MAGIC 
# MAGIC **Execution status:** the shared engine is tested locally with Spark/Delta. This notebook has not yet run in a Databricks workspace. Run all cells to establish that evidence.
# MAGIC 
# MAGIC **Source:** [Yeh (2009), UCI Default of Credit Card Clients](https://doi.org/10.24432/C55S3H), CC BY 4.0. 30,000 accounts; April–September 2005. Currency: NTD. This is a historical portfolio demonstration, not a live feed or a credit approval system.
# MAGIC 
# MAGIC ### Key assumptions
# MAGIC - `repayment_status >= 2` means the source reports a payment delay of at least two months. It is not an exact days-past-due measure.
# MAGIC - Source codes `-2` and `0` are preserved and counted as undocumented, not assigned invented meanings. They stay in the account-rate denominator.
# MAGIC - Positive bill amounts are `max(bill, 0)`; negative credit balances remain in silver. Credit limit bands use the single supplied limit, not a historical monthly limit.
# MAGIC - The 1-percentage-point alert threshold is illustrative and editable. Future default labels and demographics do not enter operational silver/gold tables.
# MAGIC - Each input is a complete snapshot. The job is single-writer. This project does not implement CDC or streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 1. Import this `.ipynb` into Databricks Workspace and attach Python serverless compute.
# MAGIC 2. Run the next two cells to select your catalog/schema and create an inbound volume.
# MAGIC 3. Upload the bundled `data/uci_credit.csv` into the displayed volume through **Catalog**.
# MAGIC 4. Run all cells. Use the packaged CSV unchanged; its checksum is verified.
# MAGIC 
# MAGIC Free Edition is a serverless, quota-limited environment. No paid classic cluster, GPU, external cloud account or secret is required by this project. Your catalog must permit schema, volume, and table creation. If volume creation is restricted, use an existing writable volume and change `source_path`.

# COMMAND ----------

import re
catalog_default = spark.sql("SELECT current_catalog()").first()[0]
dbutils.widgets.text("catalog", catalog_default)
dbutils.widgets.text("schema", "creditwatch_portfolio_v1")
dbutils.widgets.text("max_reject_rate", "0.01")
dbutils.widgets.text("alert_threshold_pp", "1.0")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
for name in [catalog, schema]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Catalog/schema must be simple identifiers")
namespace = f"{catalog}.{schema}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.inbound")
volume_path = f"/Volumes/{catalog}/{schema}/inbound"
dbutils.widgets.text("source_path", volume_path + "/uci_credit.csv")
print("Upload data/uci_credit.csv to:", volume_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Load the shared pipeline engine
# MAGIC The following cells are copied directly from `src/pipeline.py`. They define the same functions exercised by the local tests.

# COMMAND ----------

"""CreditWatch: shared PySpark transformations and Delta publication workflow.
No credentials or Databricks-only imports. The notebook embeds this exact module.
"""
import re
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

COLUMNS = ['ID', 'LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE', 'AGE',
           'PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6'] + \
          [f'BILL_AMT{i}' for i in range(1, 7)] + \
          [f'PAY_AMT{i}' for i in range(1, 7)] + ['default_next_month']
STATUS = ['PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6']
AUDIT_SCHEMA = '''run_id string, completed_at timestamp, status string,
    source_sha256 string, raw_rows long, exact_duplicates long,
    rejected_rows long, accepted_accounts long, monthly_rows long, message string'''



# COMMAND ----------

def read_input(spark, path):
    frame = spark.read.option('header', True).option('inferSchema', False).csv(path)
    if set(frame.columns) != set(COLUMNS):
        raise ValueError('Input schema mismatch; expected the packaged UCI CSV columns')
    return frame.select(*COLUMNS)

# COMMAND ----------

def prepare(raw, source_sha256):
    """Preserve raw strings, deduplicate exact rows, quarantine every conflicting ID."""
    if set(raw.columns) != set(COLUMNS):
        raise ValueError('Unexpected input schema')
    raw = raw.select(*[F.col(c).cast('string').alias(c) for c in COLUMNS])
    tagged = raw.withColumn('record_hash', F.sha2(F.to_json(F.struct(*COLUMNS)), 256))
    unique = tagged.dropDuplicates(['record_hash'])
    bronze = (unique.withColumn('source_sha256', F.lit(source_sha256))
              .withColumn('ingested_at', F.current_timestamp()))
    typed = unique.select('record_hash', *[
        F.expr(f'try_cast(`{c}` AS BIGINT)').alias(c) for c in COLUMNS])
    # IDs that differ only in textual formatting still conflict after casting.
    conflicts = typed.groupBy('ID').count().where('count > 1').select('ID')
    typed = typed.join(conflicts.withColumn('_conflict', F.lit(True)), 'ID', 'left')
    invalid_integer = F.lit(False)
    for c in COLUMNS:
        invalid_integer = invalid_integer | F.col(c).isNull()
    rules = [
        F.when(invalid_integer, F.lit('missing_or_noninteger')),
        F.when(F.col('ID') <= 0, F.lit('invalid_account_id')),
        F.when(F.col('LIMIT_BAL') <= 0, F.lit('nonpositive_credit_limit')),
        F.when(~F.col('AGE').between(18, 120), F.lit('age_out_of_range')),
        F.when(~F.col('SEX').isin(1, 2), F.lit('invalid_sex_code')),
        F.when(~F.col('EDUCATION').between(0, 6), F.lit('education_code_out_of_range')),
        F.when(~F.col('MARRIAGE').between(0, 3), F.lit('marriage_code_out_of_range')),
        F.when(~F.col('default_next_month').isin(0, 1), F.lit('invalid_outcome')),
        F.when(F.col('_conflict'), F.lit('conflicting_account_id')),
    ]
    for c in STATUS:
        rules.append(F.when(~F.col(c).between(-2, 9), F.lit(f'invalid_{c}')))
    for i in range(1, 7):
        rules.append(F.when(F.col(f'PAY_AMT{i}') < 0, F.lit(f'negative_PAY_AMT{i}')))
    checked = typed.withColumn('rejection_reason', F.concat_ws('|', *rules))
    rejected = checked.where("rejection_reason != ''").select('record_hash', 'rejection_reason')
    quarantine = bronze.join(rejected, 'record_hash', 'inner')
    accepted = checked.where("rejection_reason = ''").drop('_conflict', 'rejection_reason')
    return bronze, accepted, quarantine

# COMMAND ----------

def build_silver(accepted, run_id):
    # Demographics and future default outcome never enter operational tables.
    dim = accepted.select(F.col('ID').alias('account_id'),
                          F.col('LIMIT_BAL').alias('credit_limit_ntd'))
    dim = dim.withColumn('limit_band', F.when(F.col('credit_limit_ntd') < 100000, 'below_100k')
                         .when(F.col('credit_limit_ntd') < 300000, '100k_to_299k')
                         .otherwise('300k_plus')).withColumn('run_id', F.lit(run_id))
    months = []
    for i, status in enumerate(STATUS, start=1):
        month = 10 - i
        months.append(F.struct(
            F.to_date(F.lit(f'2005-{month:02d}-01')).alias('month'),
            F.col(status).alias('repayment_status'),
            F.col(f'BILL_AMT{i}').alias('bill_ntd'),
            F.col(f'PAY_AMT{i}').alias('payment_ntd')))
    fact = accepted.select(F.col('ID').alias('account_id'),
                           F.explode(F.array(*months)).alias('m')).select('account_id', 'm.*')
    fact = (fact.withColumn('positive_bill_ntd', F.greatest(F.col('bill_ntd'), F.lit(0)))
            .withColumn('delayed_2plus', F.col('repayment_status') >= 2)
            .withColumn('status_not_documented', F.col('repayment_status').isin(-2, 0))
            .withColumn('run_id', F.lit(run_id)))
    return dim, fact

# COMMAND ----------

def monthly_sql(fact):
    return f'''SELECT month, COUNT(*) AS accounts,
        SUM(CASE WHEN delayed_2plus THEN 1 ELSE 0 END) AS delayed_accounts,
        AVG(CASE WHEN delayed_2plus THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END) AS delayed_account_rate,
        SUM(positive_bill_ntd) AS positive_bill_ntd,
        SUM(CASE WHEN delayed_2plus THEN positive_bill_ntd ELSE 0 END) AS delayed_bill_ntd,
        CASE WHEN SUM(positive_bill_ntd) > 0 THEN
          CAST(SUM(CASE WHEN delayed_2plus THEN positive_bill_ntd ELSE 0 END) AS DOUBLE) /
          SUM(positive_bill_ntd) END AS delayed_bill_share,
        SUM(payment_ntd) AS payments_ntd,
        SUM(CASE WHEN status_not_documented THEN 1 ELSE 0 END) AS undocumented_status_accounts
        FROM {fact} GROUP BY month'''

# COMMAND ----------

def segment_sql(fact, dim):
    return f'''SELECT f.month, d.limit_band, COUNT(*) AS accounts,
        SUM(f.positive_bill_ntd) AS positive_bill_ntd,
        SUM(CASE WHEN f.delayed_2plus THEN 1 ELSE 0 END) AS delayed_accounts,
        AVG(CASE WHEN f.delayed_2plus THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END) AS delayed_account_rate
        FROM {fact} f JOIN {dim} d ON f.account_id = d.account_id AND f.run_id = d.run_id
        GROUP BY f.month, d.limit_band'''

# COMMAND ----------

def alerts_sql(monthly, threshold_pp=1.0):
    # Threshold is a configurable demonstration rule, not a calibrated credit policy.
    return f'''WITH history AS (
        SELECT *, LAG(delayed_account_rate) OVER (ORDER BY month) AS previous_rate
        FROM {monthly})
        SELECT month, delayed_account_rate, previous_rate,
          100.0 * (delayed_account_rate - previous_rate) AS change_percentage_points,
          {float(threshold_pp)} AS threshold_percentage_points,
          CASE WHEN previous_rate IS NULL THEN 'BASELINE'
               WHEN 100.0 * (delayed_account_rate - previous_rate) >= {float(threshold_pp)}
               THEN 'REVIEW' ELSE 'WITHIN_THRESHOLD' END AS alert_status
        FROM history'''

# COMMAND ----------

def check_gate(raw_rows, unique_rows, accepted_rows, rejected_rows, max_reject_rate):
    if not 0 <= max_reject_rate <= 1:
        raise ValueError('max_reject_rate must be in [0, 1]')
    if raw_rows == 0 or accepted_rows == 0:
        raise ValueError('No usable input rows; publication blocked')
    if accepted_rows + rejected_rows != unique_rows:
        raise ValueError('Input reconciliation failed')
    if rejected_rows / unique_rows > max_reject_rate:
        raise ValueError('Quarantine rate exceeds threshold; publication blocked')

# COMMAND ----------

def publish(spark, raw, namespace, source_sha256, max_reject_rate=0.01, threshold_pp=1.0):
    """Full-snapshot batch, append-only run versions, Delta bronze merge.
    A SUCCESS audit record makes a complete run visible to reporting views.
    Use a single writer (job max concurrent runs = 1).
    """
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?', namespace):
        raise ValueError('Use a simple schema or catalog.schema identifier')
    if not re.fullmatch(r'[0-9a-f]{64}', source_sha256):
        raise ValueError('Expected a SHA-256 hex digest')
    if threshold_pp < 0:
        raise ValueError('Alert threshold must be nonnegative')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
    audit = f'{namespace}.pipeline_runs'
    spark.createDataFrame([], AUDIT_SCHEMA).write.format('delta').mode('ignore').saveAsTable(audit)
    run_id = uuid.uuid4().hex
    counts = dict(raw_rows=0, exact_duplicates=0, rejected_rows=0, accepted_accounts=0, monthly_rows=0)

    def record(status, message):
        row = (run_id, datetime.now(timezone.utc).replace(tzinfo=None), status, source_sha256,
               counts['raw_rows'], counts['exact_duplicates'], counts['rejected_rows'],
               counts['accepted_accounts'], counts['monthly_rows'], message[:1000])
        spark.createDataFrame([row], AUDIT_SCHEMA).write.format('delta').mode('append').saveAsTable(audit)

    try:
        bronze, accepted, quarantine = prepare(raw, source_sha256)
        counts['raw_rows'] = raw.count()
        unique_rows = bronze.count()
        counts['exact_duplicates'] = counts['raw_rows'] - unique_rows
        counts['accepted_accounts'] = accepted.count()
        counts['rejected_rows'] = quarantine.count()
        bronze_table = f'{namespace}.bronze_records'
        bronze.limit(0).write.format('delta').mode('ignore').saveAsTable(bronze_table)
        bronze.createOrReplaceTempView('_cw_bronze_batch')
        spark.sql(f'''MERGE INTO {bronze_table} t USING _cw_bronze_batch s
          ON t.record_hash = s.record_hash AND t.source_sha256 = s.source_sha256
          WHEN NOT MATCHED THEN INSERT *''')
        quarantine.withColumn('run_id', F.lit(run_id)).write.format('delta').mode('append').saveAsTable(
            f'{namespace}.quarantine_records')
        check_gate(counts['raw_rows'], unique_rows, counts['accepted_accounts'],
                   counts['rejected_rows'], max_reject_rate)
        dim, fact = build_silver(accepted, run_id)
        counts['monthly_rows'] = fact.count()
        if counts['monthly_rows'] != counts['accepted_accounts'] * 6:
            raise ValueError('Account-month reconciliation failed')
        dim.write.format('delta').mode('append').saveAsTable(f'{namespace}.account_versions')
        fact.write.format('delta').mode('append').saveAsTable(f'{namespace}.repayment_versions')
        latest = f"SELECT run_id FROM {audit} WHERE status = 'SUCCESS' ORDER BY completed_at DESC, run_id DESC LIMIT 1"
        for view, table in [('silver_accounts', 'account_versions'), ('silver_repayments', 'repayment_versions')]:
            spark.sql(f'''CREATE OR REPLACE VIEW {namespace}.{view} AS
              SELECT * FROM {namespace}.{table} WHERE run_id = ({latest})''')
        spark.sql(f'CREATE OR REPLACE VIEW {namespace}.gold_monthly AS ' + monthly_sql(f'{namespace}.silver_repayments'))
        spark.sql(f'CREATE OR REPLACE VIEW {namespace}.gold_segments AS ' +
                  segment_sql(f'{namespace}.silver_repayments', f'{namespace}.silver_accounts'))
        spark.sql(f'CREATE OR REPLACE VIEW {namespace}.gold_alerts AS ' +
                  alerts_sql(f'{namespace}.gold_monthly', threshold_pp))
        # Publication is last; failed/incomplete runs remain invisible through silver/gold views.
        record('SUCCESS', 'Full snapshot published; no credit decisions automated')
    except Exception as exc:
        record('FAILED', str(exc))
        raise
    return {'run_id': run_id, 'status': 'SUCCESS', **counts}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Verify the source and read raw strings
# MAGIC This checksum binds the demonstration to the supplied real dataset. To adapt the project to a different source, revise the input contract, month mapping and provenance together.

# COMMAND ----------

import hashlib
source_path = dbutils.widgets.get("source_path")
if not source_path.startswith("/Volumes/"):
    raise ValueError("Use an uploaded CSV in a Unity Catalog volume")
try:
    with open(source_path, "rb") as source_file:
        source_sha256 = hashlib.file_digest(source_file, "sha256").hexdigest()
except FileNotFoundError:
    raise FileNotFoundError(f"Upload the bundled uci_credit.csv to {source_path}, then rerun")
expected_sha256 = "84ba892a2a55a0d711259f30084753f22b14b0c56303baf25ced4e7600230248"
if source_sha256 != expected_sha256:
    raise ValueError("Source checksum differs from the packaged dataset; check the uploaded file")
raw = read_input(spark, source_path)
print("Source rows:", raw.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Execute the Delta pipeline
# MAGIC Bronze MERGE avoids duplicate ingestion of the same snapshot. Validation quarantines bad records. A successful audit entry publishes the complete run to reporting views. Reruns preserve run history while current reporting stays at one record per account/month.

# COMMAND ----------

result = publish(spark, raw, namespace, source_sha256,
                 max_reject_rate=float(dbutils.widgets.get("max_reject_rate")),
                 threshold_pp=float(dbutils.widgets.get("alert_threshold_pp")))
print(result)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC Validate grain and reconciliation before reviewing results. Run the pipeline cell twice to test replay: Bronze should remain at 30,000 rows for this source. Historical version tables grow deliberately; reporting views should remain at 30,000 accounts and 180,000 account-months.

# COMMAND ----------

assert spark.table(f"{namespace}.silver_accounts").count() == 30000
assert spark.table(f"{namespace}.silver_repayments").count() == 180000
assert spark.table(f"{namespace}.bronze_records").where(
    F.col("source_sha256") == source_sha256).count() == 30000
assert spark.sql(f"SELECT account_id, month FROM {namespace}.silver_repayments GROUP BY account_id, month HAVING COUNT(*) > 1").count() == 0
assert spark.sql(f"SELECT * FROM {namespace}.silver_repayments f LEFT ANTI JOIN {namespace}.silver_accounts d ON f.account_id=d.account_id AND f.run_id=d.run_id").count() == 0
print("Grain, replay row count, and referential integrity checks passed")
display(spark.table(f"{namespace}.pipeline_runs").orderBy(F.desc("completed_at")).limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Inspect portfolio metrics
# MAGIC Use `gold_monthly` for a monthly delayed-account-rate chart and `gold_segments` for a credit-limit-band comparison. Rate columns are fractions; multiply by 100 or choose percentage formatting. Do not sum monthly balances across months.

# COMMAND ----------

display(spark.table(f"{namespace}.gold_monthly").orderBy("month"))
display(spark.table(f"{namespace}.gold_segments").orderBy("month", "limit_band"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Inspect deterioration alerts and quarantined records
# MAGIC Alerts are stored query results for analyst review. They do not send messages or change credit limits.

# COMMAND ----------

display(spark.table(f"{namespace}.gold_alerts").orderBy("month"))
display(spark.table(f"{namespace}.quarantine_records").where(F.col("run_id")==result["run_id"]).select("ID", "rejection_reason").limit(20))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC - Save the notebook output after the first successful Databricks run. That is your deployment evidence.
# MAGIC - To orchestrate it, create a Lakeflow Job with one notebook task using serverless compute and **maximum concurrent runs = 1**. Pass the same widget parameters. Run on demand: the source is historical and does not refresh.
# MAGIC - Use `sql/analyst_queries.sql` for SQL exploration. SQL access to gold can be granted separately from raw data by your workspace administrator.
# MAGIC - Before adapting to real operational data, define the source contract, historical limit semantics, access policy, ingestion frequency and business-approved alert threshold.
# MAGIC - Do not claim loss reductions, live production use or regulatory-reporting compliance from this demonstration.