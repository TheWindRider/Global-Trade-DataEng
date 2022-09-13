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
from data_tasks.api_to_json import TaskApiJson
from data_tasks.json_to_db import TaskJsonMongoDB


ita_event_task_1 = TaskApiJson(config['ITA_TRADE_EVENTS'])
ita_event_task_2 = TaskJsonMongoDB(config['MONGO_DB'])

def ita_event_op_1(**kwargs):
    date_from = kwargs["execution_date"] + timedelta(weeks=kwargs["week_lead"])
    date_to = date_from + timedelta(weeks=kwargs["week_duration"])
    date_from_str = date_from.strftime('%Y-%m-%d')
    date_to_str = date_to.strftime('%Y-%m-%d')
    ita_event_task_1.ita_api_to_json([date_from_str, date_to_str])

def ita_event_op_2(**kwargs):
    date_from = kwargs["execution_date"] + timedelta(weeks=kwargs["week_lead"])
    date_to = date_from + timedelta(weeks=kwargs["week_duration"])
    date_from_str = date_from.strftime('%Y-%m-%d')
    date_to_str = date_to.strftime('%Y-%m-%d')
    ita_event_task_2.ita_json_to_mongodb([date_from_str, date_to_str])

with DAG(
    'weekly',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Weekly data processing of project data pipeline',
    start_date=datetime(2022, 8, 1),
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='ita_event_json',
        python_callable=ita_event_op_1,
        op_kwargs={'week_lead': 1, 'week_duration': 4},
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='ita_event_db',
        python_callable=ita_event_op_2,
        op_kwargs={'week_lead': 1, 'week_duration': 4},
        provide_context=True,
    )

    t1 >> t2
