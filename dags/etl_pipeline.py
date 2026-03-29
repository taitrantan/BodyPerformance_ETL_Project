import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.load_raw import load_raw
from scripts.clean_data import clean_data
from scripts.load_dw import load_dw
from scripts.etl_log import setup_tables, validate_data, log_step, RUN_ID

default_args = {
    'owner': 'Tai Tran', 
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG( 
    dag_id= "Body_Performance_Pipeline",
    default_args=default_args,
    description="Pipeline tự dộng tính toán phân loại mức độ sức khỏe của con người dựa trên các chỉ số như BMI, sức mạnh và sức bền.", 
    schedule_interval="@daily",  # Lịch chạy: Chạy mỗi ngày 1 Lăn Lúc 00:00 [cite: 161]
    start_date=datetime(2023, 1, 1), 
    catchup=False, 
    tags=["body_performance", "reporting",  "BMI"],
) as dag:
    
    task_start = PythonOperator(
        task_id='load_raw',
        python_callable = load_raw
    )
    
    task_run_etl = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    task_end = PythonOperator(
        task_id="load_dw",
        python_callable=load_dw
    )

    task_start >> task_run_etl >> task_end
