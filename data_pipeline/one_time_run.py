import os, sys
sys.path.append(os.getcwd())
import configparser
config = configparser.RawConfigParser()
config.read('data_access.cfg')

from datetime import date, timedelta
from data_tasks.api_to_json import TaskApiJson
from data_tasks.api_to_db import TaskApiMongoDB, TaskApiElasticSearch
from data_tasks.html_to_json import TaskHtmlJson
from data_tasks.json_to_db import TaskJsonMongoDB

date_curr = date.today()
date_next = date_curr + timedelta(weeks=4)
date_prev = date_curr - timedelta(days=7)
date_curr_str = date_curr.strftime('%Y-%m-%d')
date_next_str = date_next.strftime('%Y-%m-%d')
date_prev_str = date_prev.strftime('%Y-%m-%d')
month_curr_str = date_curr.strftime('%Y-%m')

one_time_task = TaskApiJson(config['ITA_TRADE_EVENTS'])
one_time_task.ita_api_to_json([date_curr_str, date_next_str])
one_time_task = TaskApiJson(config['FRED_STLOUIS'])
one_time_task.fred_api_to_json(month_curr_str)

one_time_task = TaskHtmlJson(config['EVENTS_EYE'])
one_time_task.events_eye_to_json(date_curr)

one_time_task = TaskJsonMongoDB(config['MONGO_DB'])
one_time_task.ita_json_to_mongodb([date_curr_str, date_next_str])
one_time_task.events_eye_json_to_mongodb(month_curr_str)
one_time_task.fred_json_to_mongodb(month_curr_str)

one_time_task = TaskApiMongoDB(config['COMMODITY_PRICE'], config['MONGO_DB'])
one_time_task.commodity_api_to_mongodb()
one_time_task = TaskApiElasticSearch(config['NEWS'], config['ELASTIC_SEARCH'])
one_time_task.news_api_to_es([date_prev_str, date_curr_str])
one_time_task = TaskApiElasticSearch(config['NEWS_DATA_IO'], config['ELASTIC_SEARCH'])
one_time_task.news_api_to_es_dataio()
