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
    'dag_distributor_bronze',
    default_args=default_args,
    description='Simulate distributor CSV drops and ingest to Bronze Iceberg table',
    schedule_interval='*/15 * * * *', # Run every 15 minutes
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['bronze', 'distributor'],
) as dag:

    generate_mock_data = BashOperator(
        task_id='generate_mock_data',
        bash_command='python /opt/airflow/src/generators/distributor_csv_generator.py',
    )

    ingest_distributor_bronze = BashOperator(
        task_id='ingest_distributor_bronze',
        bash_command='python /opt/airflow/src/jobs/bronze/ingest_csv.py',
        env={
            'MINIO_ENDPOINT': 'http://minio:9000',
            'NESSIE_ENDPOINT': 'http://nessie:19120/api/v1',
            'AWS_ACCESS_KEY_ID': 'admin',
            'AWS_SECRET_ACCESS_KEY': 'password123',
            'OPENLINEAGE_URL': 'http://marquez:5000',
            'OPENLINEAGE_NAMESPACE': 'cartco-lakehouse'
        }
    )

    generate_mock_data >> ingest_distributor_bronze
