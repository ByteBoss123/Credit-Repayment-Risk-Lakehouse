"""Adversarial contract tests using the same Spark code as the Databricks notebook."""
import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.pipeline import COLUMNS, prepare, build_silver, check_gate, monthly_sql, alerts_sql

@pytest.fixture(scope='session')
def spark():
    s = (SparkSession.builder.master('local[2]').appName('creditwatch-tests')
         .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions','2').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()

def row(**changes):
    d = {c: '0' for c in COLUMNS}
    d.update(ID='1', LIMIT_BAL='100000', SEX='1', EDUCATION='2', MARRIAGE='1', AGE='30')
    d.update(changes)
    return tuple(d[c] for c in COLUMNS)

def frame(spark, rows):
    return spark.createDataFrame(rows, ','.join(c+' string' for c in COLUMNS))

def test_exact_replay_and_conflict(spark):
    raw=frame(spark,[row(),row(),row(ID='2'),row(ID='2', LIMIT_BAL='200000')])
    bronze,good,bad=prepare(raw,'a'*64)
    assert bronze.count()==3
    assert good.count()==1
    assert bad.count()==2
    assert {r.rejection_reason for r in bad.select('rejection_reason').collect()} == {'conflicting_account_id'}

def test_invalid_money_and_nulls(spark):
    raw=frame(spark,[row(LIMIT_BAL='0'),row(ID='2',PAY_AMT1='-1'),row(ID='3',AGE=None),
                     row(ID='4',PAY_0='10'),row(ID='5',LIMIT_BAL='oops')])
    _,good,bad=prepare(raw,'b'*64)
    assert good.count()==0
    assert bad.count()==5

def test_credit_balances_and_undocumented_codes(spark):
    _,good,bad=prepare(frame(spark,[row(BILL_AMT1='-100',PAY_0='-2')]),'c'*64)
    assert bad.count()==0
    dim,fact=build_silver(good,'r')
    latest=fact.where("month = '2005-09-01'").first()
    assert latest.bill_ntd==-100 and latest.positive_bill_ntd==0
    assert latest.status_not_documented and not latest.delayed_2plus
    assert 'default_next_month' not in fact.columns+dim.columns
    assert not {'SEX','EDUCATION','MARRIAGE','AGE'} & set(fact.columns+dim.columns)

def test_month_mapping_and_alert_units(spark):
    _,good,_=prepare(frame(spark,[row(PAY_0='2',PAY_2='1',BILL_AMT1='100')]),'d'*64)
    _,fact=build_silver(good,'r')
    assert fact.count()==6
    fact.createOrReplaceTempView('test_months')
    monthly=spark.sql(monthly_sql('test_months'))
    monthly.createOrReplaceTempView('test_monthly')
    alerts=spark.sql(alerts_sql('test_monthly',1.0))
    assert alerts.where("month='2005-09-01'").first().change_percentage_points==100
    assert alerts.where("month='2005-09-01'").first().alert_status=='REVIEW'
    assert alerts.where("month='2005-04-01'").first().alert_status=='BASELINE'

@pytest.mark.parametrize('counts',[(0,0,0,0),(10,10,0,10),(100,100,98,2),(100,100,99,0)])
def test_quality_gate_blocks(counts):
    with pytest.raises(ValueError): check_gate(*counts,0.01)

def test_quality_gate_boundary():
    check_gate(101,100,99,1,0.01)
