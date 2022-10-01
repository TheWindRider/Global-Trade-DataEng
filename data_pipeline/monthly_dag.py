import os
import configparser
config = configparser.RawConfigParser()
config.read(os.path.join(
    os.path.dirname(__file__), 
    '../data_access.cfg'
))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from data_tasks.api_to_json import TaskApiJson
from data_tasks.html_to_json import TaskHtmlJson
from data_tasks.json_to_db import TaskJsonMongoDB

events_eye_task_1 = TaskHtmlJson(config['EVENTS_EYE'])
events_eye_task_2 = TaskJsonMongoDB(config['MONGO_DB'])
fred_prices_task_1 = TaskApiJson(config['FRED_STLOUIS'])
fred_prices_task_2 = TaskJsonMongoDB(config['MONGO_DB'])

def events_eye_op_1(**kwargs):
    execution_date = kwargs["next_execution_date"]
    events_eye_task_1.events_eye_to_json(execution_date)

def events_eye_op_2(**kwargs):
    execution_month_str = kwargs["next_execution_date"].strftime('%Y-%m')
    events_eye_task_2.events_eye_json_to_mongodb(execution_month_str)

def fred_prices_op_1(**kwargs):
    execution_month_str = kwargs["next_execution_date"].strftime('%Y-%m')
    fred_prices_task_1.fred_api_to_json(execution_month_str)

def fred_prices_op_2(**kwargs):
    execution_month_str = kwargs["next_execution_date"].strftime('%Y-%m')
    fred_prices_task_2.fred_json_to_mongodb(execution_month_str)

with DAG(
    'monthly',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Monthly data processing of project data pipeline',
    start_date=datetime(2022, 8, 1),
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id='events_eye_json',
        python_callable=events_eye_op_1,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='events_eye_db',
        python_callable=events_eye_op_2,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='fred_prices_json',
        python_callable=fred_prices_op_1,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id='fred_prices_db',
        python_callable=fred_prices_op_2,
        provide_context=True,
    )

    recycle = BashOperator(
        task_id='old_files_cleanup',
        # delete files with modification time older than 60 days
        bash_command='find ${AIRFLOW_HOME}/dags/data_files/*.json -type f -mtime +60 -delete',
    )

    t1 >> t2
    t3 >> t4
    recycle
