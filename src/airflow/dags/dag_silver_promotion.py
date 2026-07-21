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
    'dag_silver_promotion',
    default_args=default_args,
    description='Upsert conformed Silver tables from Bronze layers and run data quality suites',
    schedule_interval='*/15 * * * *', # Run every 15 minutes (after Bronze ingestions)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['silver', 'transformation', 'quality'],
) as dag:

    refine_silver = BashOperator(
        task_id='refine_silver',
        bash_command='python /opt/airflow/src/jobs/silver/refine_silver.py',
        env={
            'MINIO_ENDPOINT': 'http://minio:9000',
            'NESSIE_ENDPOINT': 'http://nessie:19120/api/v1',
            'AWS_ACCESS_KEY_ID': 'admin',
            'AWS_SECRET_ACCESS_KEY': 'password123',
            'OPENLINEAGE_URL': 'http://marquez:5000',
            'OPENLINEAGE_NAMESPACE': 'cartco-lakehouse'
        }
    )

    validate_silver = BashOperator(
        task_id='validate_silver',
        bash_command='python /opt/airflow/src/jobs/silver/validate_silver.py',
        env={
            'MINIO_ENDPOINT': 'http://minio:9000',
            'NESSIE_ENDPOINT': 'http://nessie:19120/api/v1',
            'AWS_ACCESS_KEY_ID': 'admin',
            'AWS_SECRET_ACCESS_KEY': 'password123',
            'OPENLINEAGE_URL': 'http://marquez:5000',
            'OPENLINEAGE_NAMESPACE': 'cartco-lakehouse'
        }
    )

    refine_silver >> validate_silver
