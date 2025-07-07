import os
import pandas as pd
from pathlib import Path
from typing import Literal
from dotenv import load_dotenv
from google.cloud import bigquery


FILEPATH = Path(__file__).parents[1]
load_dotenv(FILEPATH / 'secrets/.env', override=True)

GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_NAME')
GCP_DATASET_NAME = os.getenv('GCP_DATASET_NAME')

client = bigquery.Client.from_service_account_json(FILEPATH / 'secrets' / os.getenv('SERVICE_ACCOUNT_FILE'))
dataset = client.dataset(GCP_DATASET_NAME)


def sync_district_table(period:Literal['weekly', 'monthly']):
    # Clear current period's data and reinsert the data (since there's no easy way to update the median)
    current_week = (pd.to_datetime('now') - pd.to_timedelta(pd.to_datetime('now').weekday())).date().strftime('%Y-%m-%d')
    partial_data_query = f'''
    DELETE `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.stats_{period}_district_pricecatcher_transactions`
    WHERE {period_str.lower()}_start = {current_week}
    '''
    partial_res = client.query_and_wait(partial_data_query)
    print(partial_res)

    period_str = 'WEEK' if period == 'weekly' else 'monthly'
    query = f'''
    INSERT INTO `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.stats_{period}_district_pricecatcher_transactions` (
        {period_str.lower()}_start, state, district, premise_type, item_code, min_price, max_price, median_price, mean_price
    )
    WITH unmoved_data AS (
        SELECT *
        FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.pricecatcher_transactional_record`
        WHERE TIMESTAMP_TRUNC(date, {period_str}) NOT IN (
            SELECT DISTINCT {period_str.lower()}_start
            FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.stats_{period}_district_pricecatcher_transactions`
        )
    )
    SELECT
        TIMESTAMP_TRUNC(date, {period_str}) AS {period_str.lower()}_start, ppl.state, ppl.district, ppl.premise_type, unmoved_data.item_code,
        MIN(price) AS min_price, MAX(price) AS max_price,
        APPROX_QUANTILES(price, 2)[OFFSET(1)] AS median_price,
        ROUND(AVG(price), 2) AS average_price
    FROM unmoved_data
    LEFT JOIN `{GCP_DATASET_NAME}.pricecatcher_premise_lookup` AS ppl
        ON ppl.premise_code = unmoved_data.premise_code
    WHERE
        1=1
        AND ppl.district IS NOT NULL
    GROUP BY TIMESTAMP_TRUNC(date, {period_str}), ppl.state, ppl.district, ppl.premise_type, unmoved_data.item_code
    ORDER BY TIMESTAMP_TRUNC(date, {period_str}), ppl.state, ppl.district, ppl.premise_type, unmoved_data.item_code
    '''
    res = client.query_and_wait(query)
    print(res)

if __name__ == '__main__':
    sync_district_table('weekly')
    sync_district_table('monthly')