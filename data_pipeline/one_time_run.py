import os, sys
sys.path.append(os.getcwd())
import configparser
config = configparser.RawConfigParser()
config.read('data_access.cfg')

from datetime import date, timedelta
from data_tasks.api_to_json import TaskApiJson
from data_tasks.html_to_json import TaskHtmlJson
from data_tasks.json_to_db import TaskJsonMongoDB

date_from = date.today() + timedelta(weeks=1)
date_to = date_from + timedelta(weeks=4)
date_from_str = date_from.strftime('%Y-%m-%d')
date_to_str = date_to.strftime('%Y-%m-%d')

one_time_task = TaskApiJson(config['ITA_TRADE_EVENTS'])
one_time_task.ita_api_to_json([date_from_str, date_to_str])

one_time_task = TaskJsonMongoDB(config['MONGO_DB'])
one_time_task.ita_json_to_mongodb([date_from_str, date_to_str])

one_time_task = TaskHtmlJson(config['EVENTS_EYE'])
one_time_task.events_eye_to_json(date.today())
