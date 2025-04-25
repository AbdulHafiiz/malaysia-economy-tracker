import json
import logging
import grequests
import pandas as pd
from pathlib import Path
from random import random
from itertools import islice
from time import perf_counter, sleep
from typing import Dict, Union, List, AnyStr

logger = logging.getLogger(__name__)
logging.basicConfig(filename="bond_data_scraper.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
ROOT_DIR = Path(__file__).parents[1]
BOND_FILES = ['government_investment_issues.json', 'malaysian_government_securities.json', 'short_terms_bills.json']


def get_latest_datapoint(file_list) -> Union[Dict, pd.Timestamp]:
    latest_datapoint = ''
    current_data = dict()
    for name in file_list:
        clean_name = name.replace('.json', '')
        try:
            with open(f'data/investment_vehicles/{name}', 'r') as f:
                current_data[clean_name] = json.loads(f.read())
                latest_file_date = current_data[clean_name]['data'][-1]['trading_date']

            if latest_datapoint < latest_file_date:
                latest_datapoint = latest_file_date

        except FileNotFoundError:
            logging.error(f'Missing file {name}. Please check if the file is in the correct folder.')
            continue

        except IndexError:
            logging.warning(f'File is empty, do you wish to re-scrape the data?')
            latest_datapoint = '2006-09-28'

    return current_data, pd.to_datetime(latest_datapoint)


def scrape_api(latest_datapoint:pd.Timestamp) -> Dict:
    headers = {'Accept': 'application/vnd.BNM.API.v1+json'}
    current_date = pd.to_datetime('now').strftime('%Y-%m-%d')
    start_date = latest_datapoint.strftime('%Y-%m-%d')
    max_year, min_year = pd.to_datetime('now').year, latest_datapoint.year

    for year in range(max_year, min_year-1, -1):
        date_range = reversed(pd.date_range(max(f'{year}-01-01', start_date), min(f'{year}-12-31', current_date), freq='1d'))
        url_iter = iter([
            'https://api.bnm.gov.my/public/gov-sec-yield?date={}'.format(date.strftime('%Y-%m-%d'))
            for date in date_range
        ])
        
        responses = []
        
        t1 = perf_counter()
        while True:
            url_batch = list(islice(url_iter, 10))
            if not url_batch:
                break
            responses.extend([*grequests.imap([grequests.get(url, headers=headers) for url in url_batch], size=5)])
            sleep(1+random())
        t2 = perf_counter() - t1

        logging.info(f'Scraped {len(responses)} rows of data from {year} in {t2:.3f} seconds')

        bonds_data = [
            {
                'data': res.json().get('data'),
                'meta': res.json().get('meta'),
                'status': res.status_code if res != None else 400
            }
            for res in responses
        ]
        if year > min_year:
            print('Sleeping for 1 minute')
            sleep(60)

    bonds_data = sorted(bonds_data, key=lambda x: x['meta']['last_updated'])

    return bonds_data


def append_data(current_data:Dict, fresh_data:Dict, file_list:List) -> Dict:
    current_date = pd.to_datetime('now').strftime('%Y-%m-%d')
    bonds_filtered_data = [row for row in fresh_data if row['data']['malaysian_government_securities'] != []]
    bonds_filtered_data = sorted(bonds_filtered_data, key=lambda x: x['data']['malaysian_government_securities'][0]['trading_date'])
    bonds_filtered_data

    joined_data = current_data
    data_names = [name.replace('.json', '') for name in file_list]

    for row in bonds_filtered_data:
        for name in data_names:
            if daily_data := row['data'].get(name):
                joined_data[name]['data'].extend([{**{k.strip(): v.strip() for k,v in cell.items()}, 'securities_type': 'malaysian_government_securities'} for cell in daily_data])

    for name in data_names:
        joined_data[name]['meta']['data_sources'][0]['date_collected'] = current_date
        joined_data[name]['meta']['data_sources'][0]['notes'] = f'Data from 2006-09-29 to {current_date}'

    return joined_data


def save_data(latest_data:Dict, save_dir:Path='') -> None:
    for filename, data in latest_data.items():
        with open(save_dir / f'{filename}.json', 'w') as f:
            f.write(json.dumps(data, indent=4))
            logger.info(f'Saved data for file: {filename}.json in directory: {save_dir}')

if __name__ == '__main__':
    current_data, latest_datapoint = get_latest_datapoint(BOND_FILES)
    bonds_data = scrape_api(latest_datapoint)
    latest_data = append_data(current_data, bonds_data, BOND_FILES)
    save_data(latest_data, ROOT_DIR / 'data' / 'investment_vehicles')