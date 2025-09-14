from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import warnings
import sys
import os

# Adiciona a pasta scripts no path para importar seus módulos
sys.path.append('/opt/airflow/dags/scripts')

import extract
import transform
import validate
import load
import analise_gera_csv
import gera_relatorio_pdf
import send_report

default_args = {
    'owner': 'jose',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a pasta temporária que será usada para os arquivos CSV, gráficos e PDF
TEMP_DIR = '/opt/airflow/temp'
os.makedirs(TEMP_DIR, exist_ok=True)

with DAG(
    'pipeline_ecommerce',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
) as dag:

    def ignore_warnings():
        warnings.filterwarnings("ignore")

    task_ignore_warnings = PythonOperator(
        task_id='ignore_warnings',
        python_callable=ignore_warnings
    )

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract.main
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform.main,
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate.main,
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load.main
    )

    # Wrapper para rodar a análise e salvar arquivos na pasta temp
    def run_analise_wrapper():
        analise_gera_csv.run_analysis(temp_dir=TEMP_DIR)

    task_analise = PythonOperator(
        task_id='analise_gera_csv',
        python_callable=run_analise_wrapper
    )

    # Wrapper para gerar o PDF no temp_dir
    def run_pdf_wrapper():
        gera_relatorio_pdf.generate_pdf_report(temp_dir=TEMP_DIR)

    task_pdf = PythonOperator(
        task_id='gera_relatorio_pdf',
        python_callable=run_pdf_wrapper
    )

    # Wrapper para enviar o email usando a pasta temp
    def run_send_email_wrapper():
        send_report.send_report(temp_dir=TEMP_DIR)

    task_send_email = PythonOperator(
        task_id='send_report',
        python_callable=run_send_email_wrapper
    )

    # Define a ordem das tarefas
    task_ignore_warnings >> task_extract >> task_transform >> task_validate >> task_load >> task_analise >> task_pdf >> task_send_email