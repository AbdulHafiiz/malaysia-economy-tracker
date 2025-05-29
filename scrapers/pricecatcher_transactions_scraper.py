import os
import io
import pandas as pd
from pathlib import Path
from textwrap import dedent
from typing import Generator
from datetime import datetime
from zoneinfo import ZoneInfo
from time import perf_counter
from dotenv import load_dotenv
from google.cloud import bigquery
from urllib.error import HTTPError

FILEPATH = Path(__file__).parents[1]
load_dotenv(FILEPATH / 'secrets/.env', override=True)

GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_NAME')
GCP_DATASET_NAME = os.getenv('GCP_DATASET_NAME')

write_client = bigquery.Client.from_service_account_json(FILEPATH / 'secrets' / os.getenv('SERVICE_ACCOUNT_FILE'))
dataset = write_client.dataset(GCP_DATASET_NAME)

def get_latest_date() -> str:
    meta_query = dedent(f'''
        SELECT *
        FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.meta_last_updated_date`
        WHERE tablename = 'pricecatcher_transactional_record';
    ''').strip()

    query_res = write_client.query_and_wait(meta_query)
    meta_df = query_res.to_dataframe()
    latest_date = meta_df.loc[0, 'latest_date']

    return latest_date


def batch_slice(df, batch_size) -> Generator[pd.DataFrame, None, None]:
    n = df.shape[0]
    for i in range(0, n//batch_size + 1):
        yield df.loc[batch_size*i:batch_size*(i+1)-1, :]


def upload_scraped_data(latest_date: str) -> None:
    updated_date = latest_date
    current_date = datetime.now(tz=ZoneInfo('Asia/Kuala_Lumpur')).strftime('%Y-%m-%d')
    if latest_date >= current_date:
        print('Data is up to date')
        return

    for month in pd.period_range(latest_date, current_date, freq='M').strftime('%Y-%m'):
        try:
            temp_df = pd.read_parquet(f'https://storage.data.gov.my/pricecatcher/pricecatcher_{month}.parquet')
        except HTTPError as err:
            print(f'Unable to extract pricecatcher transaction data for {month}. See error: {err}')
            continue

        temp_df['date'] = pd.to_datetime(temp_df['date'])
        transaction_df = temp_df.drop_duplicates(subset=['date', 'premise_code', 'item_code'])
        transaction_df = transaction_df[transaction_df['date'] > latest_date]
        transaction_df = transaction_df.reset_index(drop=True).astype({'date': 'datetime64[s]'})
        if transaction_df.empty:
            print(f'No data to scrape')
            return

        for idx, df_slice in enumerate(batch_slice(transaction_df, 1_000_000)):
            buf_t = perf_counter()
            buf = io.BytesIO()
            df_slice.to_parquet(buf, index=False)
            buf.seek(0)
            buf_t2 = perf_counter() - buf_t
            print(f'Loaded data to buffer in {buf_t2:.3f} seconds')

            table_ref = dataset.table('pricecatcher_transactional_record')
            initial_row_count = write_client.get_table(table_ref).num_rows

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.source_format = bigquery.SourceFormat.PARQUET
            load_job = write_client.load_table_from_file(
                buf, table_ref, job_config=job_config
            )
            print(f'Loading data into table {table_ref.table_id}...')

            load_t = perf_counter()
            load_job.result()
            load_t2 = perf_counter() - load_t
            loaded_rows = write_client.get_table(table_ref).num_rows - initial_row_count

            updated_date = df_slice['date'].max().strftime('%Y-%m-%d')

            print(f'Batch {idx+1} finished')
            print(f'Data loaded for {df_slice["date"].unique()}')
            print(f'Successfully loaded {loaded_rows} rows into the table after {load_t2:.3f} seconds')
    
    print('Finished loading data into table')

    meta_query = dedent(f'''
        UPDATE `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.meta_last_updated_date`
        SET latest_date = '{updated_date}'
        WHERE tablename = 'pricecatcher_transactional_record';
    ''').strip()

    write_client.query_and_wait(meta_query)
    print(f'Updated meta tables latest_date for tablename: `pricecatcher_transactional_record` with value {updated_date}')
    return


def pricecatcher_transaction_scraper() -> None:
    latest_date = get_latest_date()
    upload_scraped_data(latest_date=latest_date)

if __name__ == '__main__':
    pricecatcher_transaction_scraper()