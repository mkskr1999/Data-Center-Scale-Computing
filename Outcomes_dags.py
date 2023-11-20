from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import sys
from etl_scripts.transform import transform_data
from etl_scripts.ExtractDataFromAPItoGCS import main

# SOURCE_URL="https://data.austintexas.gov/api/views/9t4d-g238/rows.csv"
# AIRFLOW_HOME=os.environ.get('AIRFLOW_HOME','/opt/airflow')
# CSV_TARGET_DIR=AIRFLOW_HOME + '/data/ {{ ds }} /downloads'
# CSV_TARGET_FILE=CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'

default_args = {
    "owner": "Sai Maddula",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="outcomes_dag",
    start_date=datetime(2023,11,1),
    schedule_interval='@daily'
) as dag:
    
    start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        # copy_creds = BashOperator(task_id = "COPY_CREDS", bash_command = "echo start")

        extract_api_data_to_gcs =  PythonOperator(task_id = "EXTRACT_API_DATA_TO_GCS",
                                                  python_callable = main,)

        transform_data_step = PythonOperator(task_id="TRANSFORM_DATA",
                                             python_callable=transform_data,)

        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_api_data_to_gcs >> transform_data_step >> end

    # extract = BashOperator(
    #     task_id="extract",
    #     bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    # )
    # step2 = BashOperator(
    #     task_id="step2",
    #     bash_command=f"ls {CSV_TARGET_DIR}",
    # )
    # extract >> step2