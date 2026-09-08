from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0)**2
    return R * (2.0 * np.arcsin(np.sqrt(a)))

def calculate_spatial_metrics(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    processed_filepath = f"/opt/airflow/data/processed/processed_{target_ts}.csv"
        
    df = pd.read_csv(processed_filepath)
    
    if df.empty:
        df['distance_to_quito_km'] = []
        df['global_alert'] = []
        df['local_alert'] = []
    else:
        quito_lat, quito_lon = -0.1805, -78.4678
        df['distance_to_quito_km'] = haversine_distance(df['latitude'], df['longitude'], quito_lat, quito_lon)
        df['global_alert'] = df['mag'] >= 6.0
        df['local_alert'] = (df['distance_to_quito_km'] <= 300.0) & (df['mag'] >= 4.0)

    df.to_csv(processed_filepath, index=False)

with DAG(
    '03_spatial_haversine',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='calculate_spatial_metrics',
        python_callable=calculate_spatial_metrics
    )

    t2 = TriggerDagRunOperator(
        task_id='trigger_dag_4',
        trigger_dag_id='04_metrics_and_routing',
        conf={
            "ts_nodash": "{{ dag_run.conf.get('ts_nodash', ts_nodash) }}",
            "is_manual": "{{ dag_run.conf.get('is_manual', False) }}"
        },
        wait_for_completion=False
    )

    t1 >> t2