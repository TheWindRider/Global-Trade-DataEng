import os
import json
import requests
from typing import List

class TaskApiJson:
    """
    https://api.trade.gov/apps/store/ita/resources
    """
    def __init__(self, config):
        self.base_url = config['API_URL']
        self.access_token = config['API_TOKEN']

    def ita_api_to_json(self, event_start_dates: [str,str]):
        date_begin, date_end = event_start_dates
        dest_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/ITA_events_{date_begin}_{date_end}.json'
        )

        request_url = (
            f'{self.base_url}/search?'
            f'start_date_range%5Bfrom%5D={date_begin}&start_date_range%5Bto%5D={date_end}'
            f'&size=50'
        )
        request_header = {'subscription-key': f'{self.access_token}'}
        response = requests.get(
            request_url,
            headers=request_header
        )
        if response.status_code != 200:
            return {"error_msg": f"not able to request data from {request_url}"}
        
        events_agg = response.json()["results"]
        # page through all results iteratively
        while "next_offset" in response.json():
            response = requests.get(
                request_url + f'&offset={response.json()["next_offset"]}',
                headers=request_header
            )
            events_agg.extend(response.json()["results"])
        
        with open(dest_file, 'w') as json_file:
            json.dump(events_agg, json_file, indent=4)
        
        print(response.headers)

    def fred_api_to_json(self, curr_date: str, 
        price_code: List[str] = [
            'PCU484121484121', 
            'PCU4831114831115', 
            'IC131', 
            'IS231'
        ],
        price_name: List[str] = [
            'Index_Trucking_Freight', 
            'Index_Sea_Freight', 
            'Index_Air_Freight_Inbound', 
            'Index_Air_Freight_Outbound'
        ]
    ):
        dest_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/FRED_prices_{curr_date}.json'
        )
        series_all = []
        for series_id, series_name in zip(price_code, price_name):
            request_url = (
                f'{self.base_url}/observations?series_id={series_id}&api_key={self.access_token}'
                f'&sort_order=desc&limit=1&file_type=json'
            )
            response = requests.get(request_url)
            print(response.headers)
            if response.status_code == 200:
                series_json = response.json()
                series_json["series_code"] = series_id
                series_json["series_name"] = series_name
                series_all.append(series_json)
        
        with open(dest_file, 'w') as json_file:
            json.dump(series_all, json_file, indent=4)
