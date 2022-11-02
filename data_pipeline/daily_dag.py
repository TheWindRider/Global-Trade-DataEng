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
from data_tasks.api_to_db import TaskApiMongoDB, TaskApiElasticSearch

commodity_price_task_1 = TaskApiMongoDB(config['COMMODITY_PRICE'], config['MONGO_DB'])
trade_news_task_2 = TaskApiElasticSearch(config['NEWS_DATA_IO'], config['ELASTIC_SEARCH'])

with DAG(
    'daily',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Weekly data processing of project data pipeline',
    start_date=datetime(2022, 8, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='commodity_price',
        python_callable=commodity_price_task_1.commodity_api_to_mongodb,
    )

    t2 = PythonOperator(
        task_id='trade_news_db',
        python_callable=trade_news_task_2.news_api_to_es_dataio,
    )
