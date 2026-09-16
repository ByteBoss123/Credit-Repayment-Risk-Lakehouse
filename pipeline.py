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


def read_input(spark, path):
    frame = spark.read.option('header', True).option('inferSchema', False).csv(path)
    if set(frame.columns) != set(COLUMNS):
        raise ValueError('Input schema mismatch; expected the packaged UCI CSV columns')
    return frame.select(*COLUMNS)


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


def segment_sql(fact, dim):
    return f'''SELECT f.month, d.limit_band, COUNT(*) AS accounts,
        SUM(f.positive_bill_ntd) AS positive_bill_ntd,
        SUM(CASE WHEN f.delayed_2plus THEN 1 ELSE 0 END) AS delayed_accounts,
        AVG(CASE WHEN f.delayed_2plus THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END) AS delayed_account_rate
        FROM {fact} f JOIN {dim} d ON f.account_id = d.account_id AND f.run_id = d.run_id
        GROUP BY f.month, d.limit_band'''


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


def check_gate(raw_rows, unique_rows, accepted_rows, rejected_rows, max_reject_rate):
    if not 0 <= max_reject_rate <= 1:
        raise ValueError('max_reject_rate must be in [0, 1]')
    if raw_rows == 0 or accepted_rows == 0:
        raise ValueError('No usable input rows; publication blocked')
    if accepted_rows + rejected_rows != unique_rows:
        raise ValueError('Input reconciliation failed')
    if rejected_rows / unique_rows > max_reject_rate:
        raise ValueError('Quarantine rate exceeds threshold; publication blocked')


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
