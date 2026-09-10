import os
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def extract_data(**context):
    run_id = context["dag_run"].run_id.replace(":", "_")
    conf = context["dag_run"].conf or {}

    if conf.get("start_date") and conf.get("end_date"):
        start_time = datetime.fromisoformat(conf["start_date"].replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(conf["end_date"].replace("Z", "+00:00"))
    else:
        end_time = context["data_interval_end"]
        start_time = end_time - timedelta(days=1)

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    events = []

    while start_time < end_time:
        chunk_end = min(start_time + timedelta(days=7), end_time)
        params = {
            "format": "geojson",
            "starttime": start_time.isoformat(),
            "endtime": chunk_end.isoformat(),
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        events.extend(response.json()["features"])
        start_time = chunk_end

    os.makedirs("/opt/airflow/data", exist_ok=True)
    with open(f"/opt/airflow/data/raw_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": events}, f)

with DAG("01_extract_data", start_date=days_ago(1), schedule_interval="*/5 * * * *", catchup=False) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract_data)
    t2 = TriggerDagRunOperator(task_id="trigger_dag2", trigger_dag_id="02_clean_data", conf={"source_run_id": "{{ run_id }}"}, trigger_rule=TriggerRule.ALL_DONE)
    t1 >> t2