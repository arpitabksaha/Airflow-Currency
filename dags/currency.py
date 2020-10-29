from datetime import datetime, time, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import airflow.operators.sqlite_operator
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.hooks.dbapi_hook import DbApiHook
import sqlite3
import pandas as pd
import os, sys
import os.path
from pyspark.sql import *

#Default parameter
conn_id = 'sqlite_default'
BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
loggenerator = os.path.join(BASE_DIR, "LogGenerationScript.py")


def createdb():

    try:
        dest = SqliteHook(sqlite_conn_id='sqlite2')
        dest_conn = dest.get_conn()
        dest_conn.execute('''CREATE TABLE if not exists Currency(USD text, JPY text, CAD text, GBP text, NZD text, INR text, Date_of_rate date)''')
    except:
        print("SQLite Connection Failed")

def extract_data():
    import requests
    import json
    import csv
    import pandas as pd
    
    BASE_DIR = os.path.dirname(os.path.abspath("__file__"))

    response = requests.get('https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=2020-01-01')

    try:
        response.status_code == 200
        json_parsed = json.loads(response.text)
        df = pd.DataFrame(columns=[['USD', 'JPY', 'CAD', 'GBP', 'NZD','INR'],'Date'])
        df = df.append(pd.DataFrame([[[json_parsed['rates'][x][value] for value in ['USD', 'JPY', 'CAD', 'GBP', 'NZD','INR']],x] for x in json_parsed['rates']],columns=[['USD', 'JPY', 'CAD', 'GBP', 'NZD','INR'],'Date']))
        df.to_csv(os.path.join(BASE_DIR, "InputFile.txt"),encoding='utf-8',index= False)
    except:
        print("Error in API call")

def transform_data():
    import pyspark
    import pandas as pd
    from pyspark.sql import SQLContext
    from pyspark import SparkContext
    
    dest = SqliteHook(sqlite_conn_id='sqlite2')
    dest_conn = dest.get_conn()
    BASE_DIR = os.path.dirname(os.path.abspath("__file__"))

    sc = pyspark.SparkContext()
    sqlContext = SQLContext(sc)

    df = sc.textFile(os.path.join(BASE_DIR, "InputFile.txt"))
    df.collect()
    sparkDF = df.map(lambda x: str(x).translate({ord(c): None for c in '][""'})).map(lambda w: w.split(',')).toDF()
    pdDF = sparkDF.toPandas()
    
    sqlUpdate = 'INSERT OR REPLACE INTO Currency(USD, JPY, CAD, GBP, NZD, INR, Date_of_rate) VALUES (?, ?, ?, ?, ?, ?, ?)'
    data = pdDF.values
    dest_conn.executemany(sqlUpdate,data)
    dest_conn.commit()
    sc.stop()

		
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['arpita.b.saha@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('Currency', 
	default_args=default_args,
	schedule_interval="@once") 

	
CreateDb = PythonOperator(
	task_id='create_db',
    python_callable=createdb,
    dag=dag)
	
GetData = PythonOperator(
	task_id='api_call',
	python_callable=extract_data,
	dag=dag)
	
Transform_data = PythonOperator(
	task_id='TransformLoad',
	python_callable=transform_data,
	dag=dag)


dag.doc_md = __doc__


templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

CreateDb  >> GetData >> Transform_data
