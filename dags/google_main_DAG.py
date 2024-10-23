import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pipelines.customer_reviews_google.process import process_data as process_customer_reviews_google
from pipelines.company_profiles_google_maps.process import process_data as process_company_profiles_google_maps


default_args = {
    'owner': 'leo.mattos',
    'depends_on_past': False,
}

def load_config(pipeline_name):
    config_path = os.path.join(
        os.path.dirname(__file__), 
        'pipelines', 
        pipeline_name, 
        'config.yaml'
    )
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def run_pipeline(pipeline_name, process_function, **kwargs):
    config = load_config(pipeline_name)
    process_function(config)

with DAG(
    'customer_reviews_google_dag',
    tags=['GOOGLE', 'S3', 'POSTGRESQL'],
    default_args=default_args,
    description='DAG to process Google reviews and companies data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    start_task = EmptyOperator(task_id='Start')
    finish_task = EmptyOperator(task_id='Finish')
    
    process_customer_reviews_google_task = PythonOperator(
        task_id='process_customer_reviews_google',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'customer_reviews_google',
            'process_function': process_customer_reviews_google
        },
    )

    process_company_profiles_google_maps_task = PythonOperator(
        task_id='process_company_profiles_google_maps',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'company_profiles_google_maps',
            'process_function': process_company_profiles_google_maps
        },
    )
    
    start_task >> process_customer_reviews_google_task >> process_company_profiles_google_maps_task >> finish_task
