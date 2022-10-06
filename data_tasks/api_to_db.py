import os
import json
import pymongo
import requests
from typing import List
from elasticsearch import Elasticsearch, helpers

class TaskApiMongoDB:
    """
    https://commodities-api.com
    """
    def __init__(self, api_config, db_config):
        self.base_url = api_config['API_URL']
        self.access_token = api_config['API_TOKEN']
        self.client = pymongo.MongoClient(db_config["CONN_URI"], serverSelectionTimeoutMS=5000)

    def commodity_api_to_mongodb(self, 
        items_code: List[str] = ['BRENTOIL', 'NG', 'XCU', 'SOYBEAN', 'COTTON', 'RUBBER'],
        items_name: List[str] = ['Crude Oil', 'Natural Gas', 'Copper', 'Soy Bean', 'Cotton', 'Rubber']
    ):
        items_symbol = ','.join(items_code)
        items_dict = dict(zip(items_code, items_name))

        request_url = (
            f'{self.base_url}'
            f'?access_key={self.access_token}'
            f'&symbols={items_symbol}'
        )
        response = requests.get(request_url)
        if response.status_code != 200:
            return {"error_msg": f"not able to request data from {request_url}"}
        
        database = self.client["globa-trade"]
        collection = database["prices"]
        json_data_price = response.json()["data"]
        
        result = collection.bulk_write(
            [
                pymongo.UpdateOne(
                    {"_id": symbol},
                    {"$setOnInsert": {
                        "_id": symbol,
                        "name": items_dict[symbol],
                        "prices": [],
                    }},
                    upsert=True
                )
                for symbol, price in json_data_price["rates"].items() if symbol != "USD"
            ] + [
                pymongo.UpdateOne(
                    {"_id": symbol},
                    {"$push": {
                        "prices": {"date": json_data_price["date"], "value": price}
                    }}
                )
                for symbol, price in json_data_price["rates"].items() if symbol != "USD"
            ]
        )
        print(result.bulk_api_result)

class TaskApiElasticSearch:
    """
    https://newsapi.org
    """
    def __init__(self, api_config, db_config):
        self.api_base_url = api_config['API_URL']
        self.api_token = api_config['API_TOKEN']
        self.db_host = db_config["HOST"]
        self.db_port = db_config["PORT"]
        self.db_user = db_config["ACCESS_KEY"]
        self.db_password = db_config["ACCESS_SECRET"]
        self.db_client = Elasticsearch([{
            'host': self.db_host,
            'port': self.db_port,
            'use_ssl': True,
            'http_auth': (self.db_user, self.db_password)
        }])

    def news_api_to_es(self, date_range: [str, str], 
        query: str = '"international trade" OR "global trade"'
    ):
        date_begin, date_end = date_range

        request_url = (
            f'{self.api_base_url}'
            f'?apiKey={self.api_token}'
            f'&q={query}&from={date_begin}&to={date_end}'
            f'&sortBy=relevancy'
        )
        response = requests.get(request_url)
        if response.status_code != 200:
            return {"error_msg": f"not able to request data from {request_url}"}

        if not self.db_client.indices.exists(index='news_global_trade'):
            self.db_client.indices.create(index='news_global_trade')

        result = helpers.bulk(
            self.db_client, 
            [
                {'_index': 'news_global_trade', '_source': news}
                for news in response.json()["articles"]
            ]
        )
        print(result)
