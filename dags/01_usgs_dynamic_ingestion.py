from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def fetch_fdsnws_api(ti, ts_nodash, **kwargs):
    dag_run = kwargs.get('dag_run')
    is_manual = False
    
    if dag_run and dag_run.conf and 'start_time' in dag_run.conf:
        start_time = dag_run.conf['start_time']
        end_time = dag_run.conf['end_time']
        is_manual = True
    else:
        end_dt = kwargs['logical_date']
        start_dt = end_dt - timedelta(hours=24)
        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_time = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

    ti.xcom_push(key='is_manual', value=is_manual)

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}&minmagnitude=2.5"
    response = requests.get(url)

    raw_dir = "/opt/airflow/data/raw"
    os.makedirs(raw_dir, exist_ok=True)
    filepath = os.path.join(raw_dir, f"raw_{ts_nodash}.json")

    if response.status_code in [204, 404]:
        data = {"type": "FeatureCollection", "features": []}
    else:
        response.raise_for_status()
        data = response.json()

    with open(filepath, 'w') as f:
        json.dump(data, f)

with DAG(
    '01_usgs_dynamic_ingestion',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 9, 6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='fetch_fdsnws_api',
        python_callable=fetch_fdsnws_api
    )

    t2 = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='02_clean_and_parse',
        conf={
            "ts_nodash": "{{ ts_nodash }}", 
            "is_manual": "{{ ti.xcom_pull(task_ids='fetch_fdsnws_api', key='is_manual') }}"
        },
        wait_for_completion=False
    )

    t1 >> t2