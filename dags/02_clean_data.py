import json
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def clean_data(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    
    with open(f"/opt/airflow/data/raw_{source_run_id}.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    cleaned = []
    for feature in raw_data.get('features', []):
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [0, 0, 0])
        
        timestamp_ms = props.get("time") or 0
        dt_iso = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        
        cleaned.append({
            "id": feature.get("id"),
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "time": dt_iso,
            "url": props.get("url"),
            "longitude": coords[0],
            "latitude": coords[1],
            "depth": coords[2]
        })
        
    with open(f"/opt/airflow/data/clean_{source_run_id}.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f)

with DAG("02_clean_data", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="clean", python_callable=clean_data)
    t2 = TriggerDagRunOperator(task_id="trigger_dag3", trigger_dag_id="03_enrich_and_ingest", conf={"source_run_id": "{{ dag_run.conf.get('source_run_id') }}"}, trigger_rule=TriggerRule.ALL_DONE)
    t1 >> t2