import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pipelines.fmcsa_companies.process import process_pipeline as pipeline_fmcsa_companies
from pipelines.fmcsa_company_snapshot.process import process_pipeline as pipeline_fmcsa_company_snapshot
from pipelines.fmcsa_complaints.process import process_pipeline as pipeline_fmcsa_complaints
from pipelines.fmcsa_safer_data.process import process_pipeline as pipeline_fmcsa_safer_data

default_args = {
    'owner': 'leo.mattos',
    'depends_on_past': False
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
    'fmcsa_main_dag',
    tags=['FMCSA', 'S3', 'POSTGRESQL'],
    default_args=default_args,
    description='DAG to process FMCSA data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    start_task = EmptyOperator(task_id='Start', dag=dag)
    finish_task = EmptyOperator(task_id='Finish', dag=dag)

    process_fmcsa_companies_task = PythonOperator(
        task_id='process_fmcsa_companies',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'fmcsa_companies',
            'process_function': pipeline_fmcsa_companies
        },
    )

    process_fmcsa_company_snapshot_task = PythonOperator(
        task_id='process_fmcsa_company_snapshot',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'fmcsa_company_snapshot',
            'process_function': pipeline_fmcsa_company_snapshot
        },
    )

    process_fmcsa_complaints_task = PythonOperator(
        task_id='process_fmcsa_complaints',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'fmcsa_complaints',
            'process_function': pipeline_fmcsa_complaints
        },
    )

    process_fmcsa_safer_data_task = PythonOperator(
        task_id='process_fmcsa_safer_data',
        python_callable=run_pipeline,
        op_kwargs={
            'pipeline_name': 'fmcsa_safer_data',
            'process_function': pipeline_fmcsa_safer_data
        },
    )

    start_task >> (
    process_fmcsa_companies_task,
    process_fmcsa_company_snapshot_task,
    process_fmcsa_complaints_task,
    process_fmcsa_safer_data_task) >> finish_task
