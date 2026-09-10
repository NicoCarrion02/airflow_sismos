import json
import math
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def enrich_and_ingest(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    
    with open(f"/opt/airflow/data/clean_{source_run_id}.json", "r", encoding="utf-8") as f:
        quakes = json.load(f)

    conn = psycopg2.connect("host=postgres dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_history (
            id VARCHAR PRIMARY KEY,
            magnitude FLOAT,
            place VARCHAR,
            time VARCHAR,
            url VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            depth FLOAT,
            is_high_mag BOOLEAN,
            is_near_quito BOOLEAN
        )
    """)
    
    cur.execute("SELECT id FROM earthquake_history")
    existing_ids = set(row[0] for row in cur.fetchall())
    
    new_alerts = []
    quito_lat, quito_lon = -0.1807, -78.4678
    
    for q in quakes:
        if q["id"] in existing_ids:
            continue
            
        mag = q["magnitude"] if q["magnitude"] is not None else 0.0
        dist = math.sqrt((q["latitude"] - quito_lat)**2 + (q["longitude"] - quito_lon)**2) * 111
        
        q["is_high_mag"] = mag >= 6.0
        q["is_near_quito"] = dist <= 300
        
        cur.execute("""
            INSERT INTO earthquake_history (id, magnitude, place, time, url, latitude, longitude, depth, is_high_mag, is_near_quito)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (q["id"], q["magnitude"], q["place"], q["time"], q["url"], q["latitude"], q["longitude"], q["depth"], q["is_high_mag"], q["is_near_quito"]))
        
        if q["is_high_mag"] or q["is_near_quito"]:
            q["alert_reason"] = "💥 Magnitud Alta" if q["is_high_mag"] else "🇪🇨 Cerca de Quito"
            new_alerts.append(q)
            
    conn.commit()
    cur.close()
    conn.close()
    
    with open(f"/opt/airflow/data/alerts_{source_run_id}.json", "w", encoding="utf-8") as f:
        json.dump(new_alerts, f)

with DAG("03_enrich_and_ingest", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="enrich_ingest", python_callable=enrich_and_ingest)
    t2 = TriggerDagRunOperator(task_id="trigger_dag4", trigger_dag_id="04_format_alerts", conf={"source_run_id": "{{ dag_run.conf.get('source_run_id') }}"}, trigger_rule=TriggerRule.ALL_DONE)
    t1 >> t2