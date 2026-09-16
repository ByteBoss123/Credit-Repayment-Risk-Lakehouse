"""Real-data execution, Delta replay and failed-publication verification.
Use --jars only if jars have already been downloaded; otherwise Maven resolves them.
"""
import argparse,hashlib,json,os,tempfile
from pathlib import Path
from pyspark.sql import SparkSession,functions as F
from src.pipeline import read_input,publish

p=argparse.ArgumentParser();p.add_argument('--jars');args=p.parse_args()
root=Path(__file__).resolve().parent
work=tempfile.mkdtemp(prefix='creditwatch-')
b=(SparkSession.builder.master('local[2]').appName('CreditWatch local Delta validation')
   .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
   .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
   .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
   .config('spark.databricks.delta.snapshotPartitions','2')
   .config('spark.sql.warehouse.dir',work))
if args.jars: b=b.config('spark.jars',args.jars)
else:
    from delta import configure_spark_with_delta_pip
    b=configure_spark_with_delta_pip(b)
spark=b.getOrCreate();spark.sparkContext.setLogLevel('ERROR')
path=root/'data/uci_credit.csv'
sha=hashlib.sha256(path.read_bytes()).hexdigest()
raw=read_input(spark,str(path))
namespace='creditwatch_validation'
first=publish(spark,raw,namespace,sha)
bronze_count=spark.table(namespace+'.bronze_records').count()
second=publish(spark,raw,namespace,sha)
assert spark.table(namespace+'.bronze_records').count()==bronze_count==30000
assert spark.table(namespace+'.silver_accounts').count()==30000
assert spark.table(namespace+'.silver_repayments').count()==180000
# A failed full snapshot must not displace the preceding published run.
bad=raw.withColumn('LIMIT_BAL',F.lit('-1'))
try:
    publish(spark,bad,namespace,'f'*64)
    raise AssertionError('Expected validation failure')
except ValueError as e:
    assert 'No usable input' in str(e)
assert spark.table(namespace+'.silver_accounts').select('run_id').first()[0]==second['run_id']
assert spark.table(namespace+'.pipeline_runs').where("status='FAILED'").count()==1
monthly=spark.table(namespace+'.gold_monthly').orderBy('month').toPandas()
segments=spark.table(namespace+'.gold_segments').orderBy('month','limit_band').toPandas()
alerts=spark.table(namespace+'.gold_alerts').orderBy('month').toPandas()
# Independent pandas reconciliation of every monthly account count/rate and bill total.
import pandas as pd
source=pd.read_csv(path)
for i,status in enumerate(['PAY_0','PAY_2','PAY_3','PAY_4','PAY_5','PAY_6'],1):
    r=monthly.loc[monthly.month.astype(str)==f'2005-{10-i:02d}-01'].iloc[0]
    assert r.accounts==len(source)
    assert r.delayed_accounts==int((source[status]>=2).sum())
    assert abs(float(r.delayed_account_rate)-float((source[status]>=2).mean()))<1e-10
    assert r.positive_bill_ntd==int(source[f'BILL_AMT{i}'].clip(lower=0).sum())
    assert r.delayed_bill_ntd==int(source.loc[source[status]>=2,f'BILL_AMT{i}'].clip(lower=0).sum())
out=root/'evidence';out.mkdir(exist_ok=True)
monthly.to_csv(out/'monthly_results.csv',index=False)
segments.to_csv(out/'segment_results.csv',index=False)
alerts.to_csv(out/'alert_results.csv',index=False)
result={'execution_environment':'Local Apache Spark 3.5.3 + Delta Lake 3.2.1; NOT Databricks',
        'source_sha256':sha,'first_run':first,'replay_run':second,
        'checks':{'bronze_replay_no_duplicate':True,'current_accounts_30000':True,
                  'current_months_180000':True,'failed_run_keeps_previous_publication':True,
                  'pandas_monthly_reconciliation':True},'databricks_execution':'NOT RUN: no workspace connector available'}
(out/'local_validation.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2));print(monthly.to_string(index=False));print(alerts.to_string(index=False))
spark.stop()
