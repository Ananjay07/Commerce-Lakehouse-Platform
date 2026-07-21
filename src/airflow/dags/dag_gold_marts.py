from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'cartco_de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dag_gold_marts',
    default_args=default_args,
    description='Aggregate conformed Silver tables into Gold business marts',
    schedule_interval='*/20 * * * *', # Run every 20 minutes
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['gold', 'marts', 'analytics'],
) as dag:

    compute_gold_marts = BashOperator(
        task_id='compute_gold_marts',
        bash_command='python /opt/airflow/src/jobs/gold/refine_gold.py',
        env={
            'MINIO_ENDPOINT': 'http://minio:9000',
            'NESSIE_ENDPOINT': 'http://nessie:19120/api/v1',
            'AWS_ACCESS_KEY_ID': 'admin',
            'AWS_SECRET_ACCESS_KEY': 'password123',
            'OPENLINEAGE_URL': 'http://marquez:5000',
            'OPENLINEAGE_NAMESPACE': 'cartco-lakehouse'
        }
    )

    compute_gold_marts
