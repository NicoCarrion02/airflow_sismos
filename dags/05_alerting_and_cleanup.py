from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os
import shutil

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def send_telegram_alert(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    summary_filepath = f"/opt/airflow/data/reports/summary_{target_ts}.json"
    
    with open(summary_filepath, 'r') as f:
        route_data = json.load(f)
        
    if not route_data.get("send_telegram"):
        return
        
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        raise ValueError("Credenciales de Telegram no encontradas en el entorno.")
        
    summary = route_data["summary"]
    text = (
        "🚨 *ALERTA SÍSMICA CRÍTICA* 🚨\n\n"
        f"🌍 Sismos Globales (Mag ≥ 6): {summary['global_alerts']}\n"
        f"📍 Sismos Locales (Quito, Mag ≥ 4): {summary['local_alerts']}\n"
        f"📈 Magnitud Máxima: {summary['max_mag']}\n\n"
        "Revisar el reporte completo en el correo."
    )
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

def send_email_report(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    summary_filepath = f"/opt/airflow/data/reports/summary_{target_ts}.json"
    
    with open(summary_filepath, 'r') as f:
        route_data = json.load(f)
        
    if not route_data.get("send_email"):
        return
        
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    alert_email = os.environ.get("ALERT_EMAIL")
    
    if not all([smtp_user, smtp_pass, alert_email]):
        raise ValueError("Credenciales SMTP no encontradas en el entorno.")
        
    summary = route_data["summary"]
    mode = "Histórico (Manual)" if route_data.get("is_manual") else "Monitoreo (Automático)"
    
    html_content = f"""
    <html>
      <body>
        <h2>Reporte Sísmico - {mode}</h2>
        <p><strong>Total de eventos procesados:</strong> {summary['total']}</p>
        <p><strong>Magnitud Máxima:</strong> {summary['max_mag']}</p>
        <p><strong>Profundidad Promedio:</strong> {summary['avg_depth']} km</p>
        
        <h3>Top 5 - Sismos Globales</h3>
        <table border="1" cellpadding="5">
          <tr><th>Magnitud</th><th>Lugar</th><th>Distancia a Quito (km)</th></tr>
          {"".join(f"<tr><td>{q['mag']}</td><td>{q['place']}</td><td>{round(q['distance_to_quito_km'], 2)}</td></tr>" for q in summary['top_global'])}
        </table>
        
        <h3>Top 5 - Sismos Cercanos a Quito</h3>
        <table border="1" cellpadding="5">
          <tr><th>Magnitud</th><th>Lugar</th><th>Distancia a Quito (km)</th></tr>
          {"".join(f"<tr><td>{q['mag']}</td><td>{q['place']}</td><td>{round(q['distance_to_quito_km'], 2)}</td></tr>" for q in summary['top_quito'])}
        </table>
      </body>
    </html>
    """
    
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = alert_email
    msg['Subject'] = f"Reporte Sísmico USGS - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    msg.attach(MIMEText(html_content, 'html'))
    
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)

def archive_and_clean(dag_run, ts_nodash, **kwargs):
    target_ts = dag_run.conf.get('ts_nodash', ts_nodash) if dag_run and dag_run.conf else ts_nodash
    
    processed_file = f"/opt/airflow/data/processed/processed_{target_ts}.csv"
    raw_file = f"/opt/airflow/data/raw/raw_{target_ts}.json"
    archive_dir = "/opt/airflow/data/archive"
    
    os.makedirs(archive_dir, exist_ok=True)
    
    if os.path.exists(processed_file):
        shutil.move(processed_file, os.path.join(archive_dir, f"archived_{target_ts}.csv"))
        
    if os.path.exists(raw_file):
        os.remove(raw_file)

with DAG(
    '05_alerting_and_cleanup',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='send_telegram_alert',
        python_callable=send_telegram_alert
    )

    t2 = PythonOperator(
        task_id='send_email_report',
        python_callable=send_email_report
    )

    t3 = PythonOperator(
        task_id='archive_and_clean',
        python_callable=archive_and_clean
    )

    [t1, t2] >> t3