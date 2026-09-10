import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def format_alerts(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    
    with open(f"/opt/airflow/data/alerts_{source_run_id}.json", "r", encoding="utf-8") as f:
        alerts = json.load(f)
        
    formatted = {"telegram": [], "email": ""}
    
    if alerts:
        email_lines = ["<h3>Sismos Relevantes Detectados (Nuevos):</h3><ul>"]
        for q in alerts:
            formatted["telegram"].append(
                f"🚨 **¡ALERTA SÍSMICA DETECTADA!** 🚨\n"
                f"📌 *Criterio:* {q['alert_reason']}\n\n"
                f"📍 **Ubicación:** {q['place']}\n"
                f"📊 **Magnitud:** {q['magnitude']}\n"
                f"🌊 **Profundidad:** {q['depth']} km\n"
                f"🕒 **Fecha/Hora:** {q['time']}\n"
                f"🔗 [Ver en USGS]({q['url']})"
            )
            email_lines.append(
                f"<li><b>{q['alert_reason']}</b> - Mag {q['magnitude']} | Prof. {q['depth']}km | {q['place']} | {q['time']} | <a href='{q['url']}'>Enlace</a></li>"
            )
        email_lines.append("</ul>")
        formatted["email"] = "".join(email_lines)

    with open(f"/opt/airflow/data/formatted_{source_run_id}.json", "w", encoding="utf-8") as f:
        json.dump(formatted, f)

with DAG("04_format_alerts", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="format", python_callable=format_alerts)
    t2 = TriggerDagRunOperator(task_id="trigger_dag5", trigger_dag_id="05_send_notifications", conf={"source_run_id": "{{ dag_run.conf.get('source_run_id') }}"}, trigger_rule=TriggerRule.ALL_DONE)
    t1 >> t2