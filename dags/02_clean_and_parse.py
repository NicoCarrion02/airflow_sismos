from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def clean_and_parse_json(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    raw_filepath = f"/opt/airflow/data/raw/raw_{target_ts}.json"
    
    with open(raw_filepath, 'r') as f:
        data = json.load(f)
        
    features = data.get('features', [])
    processed_dir = "/opt/airflow/data/processed"
    os.makedirs(processed_dir, exist_ok=True)
    
    if not features:
        df = pd.DataFrame(columns=['id', 'time', 'latitude', 'longitude', 'depth', 'mag', 'place'])
    else:
        rows = [{
            'id': feat.get('id'),
            'time': feat.get('properties', {}).get('time'),
            'longitude': feat.get('geometry', {}).get('coordinates', [None]*3)[0],
            'latitude': feat.get('geometry', {}).get('coordinates', [None]*3)[1],
            'depth': feat.get('geometry', {}).get('coordinates', [None]*3)[2],
            'mag': feat.get('properties', {}).get('mag'),
            'place': feat.get('properties', {}).get('place')
        } for feat in features]
            
        df = pd.DataFrame(rows).dropna(subset=['latitude', 'longitude', 'mag'])
        df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
        df[['mag', 'depth']] = df[['mag', 'depth']].astype(float)

        history_file = os.path.join(processed_dir, "seen_earthquake_ids.json")
        seen_ids = set()
        
        if os.path.exists(history_file):
            with open(history_file, 'r') as hf:
                seen_ids = set(json.load(hf))
                
        df = df[~df['id'].isin(seen_ids)].copy()
        
        if not df.empty:
            with open(history_file, 'w') as hf:
                json.dump(list(seen_ids.union(df['id'].tolist())), hf)

    processed_filepath = os.path.join(processed_dir, f"processed_{target_ts}.csv")
    df.to_csv(processed_filepath, index=False)

with DAG(
    '02_clean_and_parse',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='clean_and_parse_json',
        python_callable=clean_and_parse_json
    )

    t2 = TriggerDagRunOperator(
        task_id='trigger_dag_3',
        trigger_dag_id='03_spatial_haversine',
        conf={
            "ts_nodash": "{{ dag_run.conf.get('ts_nodash', ts_nodash) }}",
            "is_manual": "{{ dag_run.conf.get('is_manual', False) }}"
        },
        wait_for_completion=False
    )

    t1 >> t2