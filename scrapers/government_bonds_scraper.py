import json
import logging
import grequests
from random import random
from itertools import islice
from time import perf_counter, sleep
from pandas import to_datetime, date_range

logger = logging.getLogger(__name__)
logging.basicConfig(filename="process_data.log", filemode="w", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOND_FILES = ['government_investment_issues.json', 'malaysian_government_securities.json', 'short_terms_bills.json']


def get_latest_datapoint(file_list):
    latest_datapoint = ''
    for name in file_list:
        clean_name = name.replace('.json', '')
        try:
            with open(f'data/investment_vehicles/{name}', 'r') as f:
                latest_file_date = json.loads(f.read())['data'][-1]['trading_date']
            if latest_datapoint < latest_file_date:
                latest_datapoint = latest_file_date
        except FileNotFoundError:
            logging.error(f'Missing file {name}. Please check if the file is in the correct folder.')
            continue
        except IndexError:
            logging.warning(f'File is empty, do you wish to re-scrape the data?')
            latest_datapoint = '2006-09-28'

    return to_datetime(latest_datapoint)


def scrape_api(latest_datapoint):
    headers = {'Accept': 'application/vnd.BNM.API.v1+json'}
    current_date = to_datetime('now').strftime('%Y-%m-%d')
    start_date = latest_datapoint.strftime('%Y-%m-%d')
    max_year, min_year = to_datetime('now').year, latest_datapoint.year

    for year in range(max_year, min_year-1, -1):
        date_range = reversed(date_range(max(f'{year}-01-01', start_date), min(f'{year}-12-31', current_date), freq='1d'))
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
        bonds_data = sorted(bonds_data, key=lambda x: x['meta']['last_updated'])
        with open(f'data/json/bonds_data_{year}.json', 'w') as f:
            f.write(json.dumps(bonds_data, indent=4))

        if year > min_year:
            print('Sleeping for 1 minute')
            sleep(60)

    return bonds_data


if __name__ == '__main__':
    latest_datapoint = get_latest_datapoint(BOND_FILES)
    bonds_data = scrape_api(latest_datapoint)