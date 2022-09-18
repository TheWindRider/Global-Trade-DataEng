import os
import json
import requests
import pandas as pd

from datetime import date
from bs4 import BeautifulSoup

class TaskHtmlJson:
    """
    https://www.eventseye.com/
    """
    def __init__(self, config, max_page=50):
        self.template_url = config['HTML_URL']
        self.max_page = max_page

    def process_html_table_header(self, html_table):
        html_table_header = html_table.thead.find("tr")
        html_table_cols = [th_item.text for th_item in html_table_header.find_all("th")]
        # additional field, value being a list of URLs
        html_table_cols.append("links")
        return html_table_cols

    def process_html_table_body(self, html_table):
        html_table_body = html_table.tbody.find_all("tr")
        html_table_rows = []
        for html_table_row in html_table_body:
            html_table_row_data = html_table_row.find_all("td")
            html_table_row_links = html_table_row.find_all("a")
            html_table_row_text = [cell.get_text('; ', strip=True) for cell in html_table_row_data]
            # additional field, value being a list of URLs
            html_table_row_text.append([link.get("href") for link in html_table_row_links])
            html_table_rows.append(html_table_row_text)
        return html_table_rows

    def events_eye_to_json(self, execute_date: date):
        month_id = execute_date.strftime('%Y-%m')
        month_name = execute_date.strftime("%B")

        dest_file = os.path.join(
            os.path.dirname(__file__),
            f'../data_files/Events_Eye_{month_id}.json'
        )
        event_tables = []

        for page in range(self.max_page):
            curr_page = '' if page == 0 else '_'+str(page)
            curr_url = self.template_url.format(month=str.lower(month_name), page=curr_page)

            try:
                html_text = requests.get(curr_url).text
                html_data = BeautifulSoup(html_text, "lxml")
                html_table = html_data.find_all("table")
                if len(html_table) == 0:
                    continue
                html_table_cols = self.process_html_table_header(html_table[0])
                html_table_rows = self.process_html_table_body(html_table[0])
                data_table = pd.DataFrame.from_records(html_table_rows, columns=html_table_cols)
                event_tables.append(data_table)
            
            except requests.HTTPError as e:
                break

        event_table = pd.concat(event_tables)
        event_table.reset_index(drop=True, inplace=True)
        event_table["event_id"] = event_table.index
        event_table["event_id"] = event_table["event_id"].apply(lambda x: month_id + '-' + str(x))
        event_table["start_date"] = event_table["Date"].str.split(r'[;\(?\)]').str[0]
        event_table["event_duration"] = event_table["Date"].str.split(r'[;\(?\)]').str[-1]
        event_table.drop(columns=["Date"], inplace=True)
        event_table.rename(columns={
            "Exhibition Name": "event_name",
            "Cycle": "cycle",
            "Venue": "venue",
        }, inplace=True)
        
        print(event_table.info())
        with open(dest_file, 'w') as json_file:
            json.dump(json.loads(
                event_table.to_json(orient='records')
            ), json_file, indent=4)
