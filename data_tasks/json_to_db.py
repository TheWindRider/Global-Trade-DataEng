import os
import json
import pymongo

class TaskJsonMongoDB:
    def __init__(self, config):
        self.conn_url = config["CONN_URI"]

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
                if "event_id" in doc and "source" in doc
            ]
        # set primary key to avoid duplicates
        for doc in json_data_events:
            doc.pop("id")
            doc.update({"_id": {
                "event_id": doc["event_id"], 
                "source": doc["source"]
            }})

        client = pymongo.MongoClient(self.conn_url, serverSelectionTimeoutMS=5000)
        database = client["globa-trade"]
        collection = database["trade_events"]
        result = collection.bulk_write([
            pymongo.ReplaceOne({"_id": {
                "event_id": doc["event_id"], 
                "source": doc["source"]
            }}, doc, upsert=True)
            for doc in json_data_events
        ])
        print(result.bulk_api_result)
