import os
import json
import pymongo
import requests
from typing import List

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
        collection = database["commodity_price"]
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
                        "prices": {"date": json_data_price["date"], "price": price}
                    }}
                )
                for symbol, price in json_data_price["rates"].items() if symbol != "USD"
            ]
        )
        print(result.bulk_api_result)
