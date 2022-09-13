import os
import configparser
config = configparser.RawConfigParser()
config.read(os.path.join(
    os.path.dirname(__file__), 
    '../data_access.cfg'
))

from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
from data_tasks.html_to_json import TaskHtmlJson
from data_tasks.json_to_db import TaskJsonMongoDB

events_eye_task_1 = TaskHtmlJson(config['EVENTS_EYE'])
events_eye_task_2 = TaskJsonMongoDB(config['MONGO_DB'])

def events_eye_op_1(**kwargs):
    execution_date = kwargs["next_execution_date"]
    events_eye_task_1.events_eye_to_json(execution_date)

def events_eye_op_2(**kwargs):
    execution_month_str = kwargs["next_execution_date"].strftime('%Y-%m')
    events_eye_task_2.events_eye_json_to_mongodb(execution_month_str)

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

    t1 >> t2
