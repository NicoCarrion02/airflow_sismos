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

def process_metrics_and_routing(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    is_manual = dag_run.conf.get('is_manual', False) if dag_run and dag_run.conf else False
        
    processed_filepath = f"/opt/airflow/data/processed/processed_{target_ts}.csv"
    df = pd.read_csv(processed_filepath)
    
    if df.empty:
        summary = {
            "total": 0,
            "max_mag": 0.0,
            "avg_depth": 0.0,
            "global_alerts": 0,
            "local_alerts": 0,
            "top_global": [],
            "top_quito": []
        }
    else:
        summary = {
            "total": len(df),
            "max_mag": float(df['mag'].max()),
            "avg_depth": round(float(df['depth'].mean()), 2),
            "global_alerts": int(df['global_alert'].sum()),
            "local_alerts": int(df['local_alert'].sum()),
            "top_global": df.nlargest(5, 'mag')[['id', 'time', 'mag', 'place', 'distance_to_quito_km']].to_dict('records'),
            "top_quito": df.nsmallest(5, 'distance_to_quito_km')[['id', 'time', 'mag', 'place', 'distance_to_quito_km']].to_dict('records')
        }

    send_telegram = False if is_manual else (summary["global_alerts"] > 0 or summary["local_alerts"] > 0)
    
    reports_dir = "/opt/airflow/data/reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    route_config = {
        "is_manual": is_manual,
        "send_telegram": send_telegram,
        "send_email": True,
        "summary": summary
    }
    
    with open(os.path.join(reports_dir, f"summary_{target_ts}.json"), 'w') as f:
        json.dump(route_config, f, indent=4)

with DAG(
    '04_metrics_and_routing',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='process_metrics_and_routing',
        python_callable=process_metrics_and_routing
    )

    t2 = TriggerDagRunOperator(
        task_id='trigger_dag_5',
        trigger_dag_id='05_alerting_and_cleanup',
        conf={
            "ts_nodash": "{{ dag_run.conf.get('ts_nodash', ts_nodash) }}",
            "is_manual": "{{ dag_run.conf.get('is_manual', False) }}"
        },
        wait_for_completion=False
    )

    t1 >> t2