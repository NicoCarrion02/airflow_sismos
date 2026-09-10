import os
import json
import requests
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def extract_data(**context):
    run_id = context['dag_run'].run_id.replace(':', '_')
    conf = context["dag_run"].conf or {}
    end_time = conf.get("end_date") or context["data_interval_end"].isoformat()
    start_time = conf.get("start_date") or (context["data_interval_end"] - timedelta(days=1)).isoformat()
    
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    os.makedirs('/opt/airflow/data', exist_ok=True)
    with open(f"/opt/airflow/data/raw_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(response.json(), f)

with DAG("01_extract_data", start_date=days_ago(1), schedule_interval="*/5 * * * *", catchup=False) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract_data)
    t2 = TriggerDagRunOperator(task_id="trigger_dag2", trigger_dag_id="02_clean_data", conf={"source_run_id": "{{ run_id }}"})
    t1 >> t2