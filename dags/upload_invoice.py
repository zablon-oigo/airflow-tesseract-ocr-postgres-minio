from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='upload_data_to_minio',
    default_args=default_args,
    description='Upload files from local data folder to MinIO',
    schedule=None,   
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    def upload_folder_to_minio():
        hook = S3Hook(aws_conn_id='minio_default')
        local_data_dir = Path('/opt/airflow/data')
        
        for root, dirs, files in os.walk(local_data_dir):
            for file in files:
                local_file_path = Path(root) / file
                relative_path = local_file_path.relative_to(local_data_dir)
                s3_key = f"data/{relative_path}" 
                
                print(f"Uploading {local_file_path}  s3://warehouse/{s3_key}")
                
                hook.load_file(
                    filename=str(local_file_path),
                    key=s3_key,
                    bucket_name='warehouse',
                    replace=True
                )

    upload_task = PythonOperator(
        task_id='upload_all_files_from_data_folder',
        python_callable=upload_folder_to_minio,
    )