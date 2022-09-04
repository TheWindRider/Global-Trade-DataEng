import os
import json
import requests

class TaskApiJson:
    """
    https://api.trade.gov/apps/store/ita/resources
    """
    def __init__(self, config):
        self.base_url = config['API_URL']
        self.access_token = config['API_TOKEN']

    def ita_api_to_json(self, event_start_dates: [str,str]):
        date_begin, date_end = event_start_dates

        request_url = f'{self.base_url}/search?start_date={date_begin}%20TO%20{date_end}'
        request_header = {'Authorization': f'Bearer {self.access_token}'}
        dest_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/ITA_events_{date_begin}_{date_end}.json'
        )

        response = requests.get(
            request_url,
            headers=request_header
        )
        if response.status_code == 200:
            with open(dest_file, 'w') as json_file:
                json.dump(response.json(), json_file, indent=4)
        
        print(response.headers)
