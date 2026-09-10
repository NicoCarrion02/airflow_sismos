import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

def send_telegram(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    file_path = f"/opt/airflow/data/formatted_{source_run_id}.json"
    
    if not os.path.exists(file_path):
        return

    with open(file_path, "r", encoding="utf-8") as f:
        formatted = json.load(f)
        
    if formatted.get("telegram"):
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        for msg in formatted["telegram"]:
            requests.post(tg_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})

def send_email(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    file_path = f"/opt/airflow/data/formatted_{source_run_id}.json"
    
    if not os.path.exists(file_path):
        return

    with open(file_path, "r", encoding="utf-8") as f:
        formatted = json.load(f)
        
    if formatted.get('email'):
        api_key = os.environ.get("BREVO_API_KEY")
        sender_email = os.environ.get("BREVO_SENDER_EMAIL")
        alert_emails = [e.strip() for e in os.environ.get("ALERT_EMAIL", "").split(",") if e.strip()]

        response = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={
                "accept": "application/json",
                "api-key": api_key,
                "content-type": "application/json"
            },
            json={
                "sender": {"email": sender_email},
                "to": [{"email": email} for email in alert_emails],
                "subject": "Alerta: Nuevos Sismos Relevantes (USGS)",
                "htmlContent": formatted["email"]
            },
            timeout=30
        )
        response.raise_for_status()

def cleanup_files(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    for prefix in ["raw", "clean", "alerts", "formatted"]:
        file_path = f"/opt/airflow/data/{prefix}_{source_run_id}.json"
        if os.path.exists(file_path):
            os.remove(file_path)

with DAG("05_send_notifications", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    
    telegram_task = PythonOperator(
        task_id="send_telegram",
        python_callable=send_telegram
    )
    
    email_task = PythonOperator(
        task_id="send_email",
        python_callable=send_email
    )
    
    clean_task = PythonOperator(
        task_id="cleanup_files",
        python_callable=cleanup_files,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    [telegram_task, email_task] >> clean_task