import os
import json
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

def send_notifications(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    file_path = f"/opt/airflow/data/formatted_{source_run_id}.json"
    
    if not os.path.exists(file_path):
        print("No se encontró el archivo de alertas. Omitiendo envío.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        formatted = json.load(f)
        
    if formatted.get("telegram"):
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        for msg in formatted["telegram"]:
            requests.post(tg_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
            
        smtp_server = os.environ.get('SMTP_SERVER', 'smtp-relay.brevo.com')
        smtp_port = int(os.environ.get('SMTP_PORT', 587))
        smtp_user = os.environ.get('SMTP_USER')
        smtp_pass = os.environ.get('SMTP_PASSWORD')
        alert_emails = os.environ.get('ALERT_EMAIL', '').split(',')
        
        email_msg = MIMEMultipart()
        email_msg['From'] = smtp_user
        email_msg['To'] = ", ".join(alert_emails)
        email_msg['Subject'] = "Alerta: Nuevos Sismos Relevantes (USGS)"
        email_msg.attach(MIMEText(formatted["email"], 'html'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(email_msg)

def cleanup_files(**context):
    source_run_id = context['dag_run'].conf.get('source_run_id', '').replace(':', '_')
    for prefix in ["raw", "clean", "alerts", "formatted"]:
        file_path = f"/opt/airflow/data/{prefix}_{source_run_id}.json"
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Eliminado: {file_path}")

with DAG("05_send_notifications", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    
    notify_task = PythonOperator(
        task_id="send_notifications",
        python_callable=send_notifications
    )
    
    clean_task = PythonOperator(
        task_id="cleanup_files",
        python_callable=cleanup_files,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    notify_task >> clean_task