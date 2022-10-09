import os
import json
import certifi
import pymongo

class TaskJsonMongoDB:
    def __init__(self, config):
        ca = certifi.where()
        self.conn_url = config["CONN_URI"]
        self.client = pymongo.MongoClient(
            self.conn_url, 
            serverSelectionTimeoutMS=5000, 
            tlsCAFile=ca
        )

    def ita_json_to_mongodb(self, event_start_dates: [str,str]):
        date_begin, date_end = event_start_dates

        source_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/ITA_events_{date_begin}_{date_end}.json'
        )
        if not os.path.exists(source_file):
            return {"error_msg": f"{source_file} not exists"}

        with open(source_file) as json_file:
            json_data = json.load(json_file)
            json_data_events = [
                doc for doc in json_data["results"] 
                if "id" in doc and "source" in doc
            ]
        # set primary key to avoid duplicates
        for doc in json_data_events:
            doc.update({"_id": {
                "event_id": doc["id"], 
                "source": doc["source"]
            }})

        database = self.client["globa-trade"]
        collection = database["trade_events"]
        result = collection.bulk_write([
            pymongo.ReplaceOne({"_id": {
                "event_id": doc["id"], 
                "source": doc["source"]
            }}, doc, upsert=True)
            for doc in json_data_events
        ])
        print(result.bulk_api_result)

    def events_eye_json_to_mongodb(self, execute_month: str):
        source_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/Events_Eye_{execute_month}.json'
        )
        if not os.path.exists(source_file):
            return {"error_msg": f"{source_file} not exists"}

        with open(source_file) as json_file:
            json_data = json.load(json_file)
            json_data_events = [doc for doc in json_data if "event_id" in doc]
        # set primary key to avoid duplicates
        for doc in json_data_events:
            doc.update({"_id": {
                "event_id": doc["event_id"], 
                "source": "Events_Eye"
            }})

        database = self.client["globa-trade"]
        collection = database["trade_events"]
        result = collection.bulk_write([
            pymongo.ReplaceOne({"_id": {
                "event_id": doc["event_id"], 
                "source": "Events_Eye"
            }}, doc, upsert=True)
            for doc in json_data_events
        ])
        result_dict = result.bulk_api_result
        del result_dict["upserted"]
        print(result_dict)

    def fred_json_to_mongodb(self, execute_month: str):
        source_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/FRED_prices_{execute_month}.json'
        )
        if not os.path.exists(source_file):
            return {"error_msg": f"{source_file} not exists"}

        with open(source_file) as json_file:
            json_data = json.load(json_file)

        database = self.client["globa-trade"]
        collection = database["prices"]
        result = collection.bulk_write(
            [
                pymongo.UpdateOne(
                    {"_id": series["series_code"]},
                    {"$setOnInsert": {
                        "_id": series["series_code"],
                        "name": series["series_name"],
                        "prices": [],
                    }},
                    upsert=True
                )
                for series in json_data
            ] + [
                pymongo.UpdateOne(
                    {"_id": series["series_code"]},
                    {"$push": {
                        "prices": {"$each": series["observations"]}
                    }}
                )
                for series in json_data
            ]
        )
        print(result.bulk_api_result)
