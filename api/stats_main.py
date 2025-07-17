import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException
from api_utils import query_builder
from models.stats_models import PremiseSearchOptions, ItemSearchOptions, PricecatcherStatsSearch


FILEPATH = Path(__file__).parents[1]
load_dotenv(FILEPATH / 'secrets/.env', override=True)

GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_NAME')
GCP_DATASET_NAME = os.getenv('GCP_DATASET_NAME')

client = bigquery.Client.from_service_account_json(FILEPATH / 'secrets' / os.getenv('SERVICE_ACCOUNT_FILE'))

app = FastAPI()

@app.get('/hello')
async def test():
    return {'message': 'Hello World!'}


@app.post('/pricecatcher/item/search')
async def search_item(search_options: ItemSearchOptions):
    query_filter, query_params = query_builder(search_options, ['limit'])

    if search_options.limit:
        query_filter.append('\nLIMIT @limit')
        query_params.append(bigquery.ScalarQueryParameter('limit', 'INTEGER', getattr(search_options, 'limit')))

    query = ''.join([f'SELECT * FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.pricecatcher_item_lookup`\nWHERE 1=1', *query_filter])
    query_config = bigquery.QueryJobConfig(query_parameters=query_params)

    item_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not item_list:
        raise HTTPException(status_code=204)

    return {'name': 'item_search', 'data': item_list}


@app.post('/pricecatcher/premise/search')
async def search_premise(search_options: PremiseSearchOptions):
    query_filter, query_params = query_builder(search_options, ['limit'])
    if search_options.limit:
        query_filter.append('\nLIMIT @limit')
        query_params.append(bigquery.ScalarQueryParameter('limit', 'INTEGER', getattr(search_options, 'limit')))

    query = ''.join([f'SELECT * FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.pricecatcher_premise_lookup` WHERE 1=1', *query_filter])
    query_config = bigquery.QueryJobConfig(query_parameters=query_params)
    premise_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not premise_list:
        raise HTTPException(status_code=204)

    return {'name': 'premise_search', 'data': premise_list}


@app.post('/pricecatcher/stats/search')
async def search_stats(search_options: PricecatcherStatsSearch):
    special_fields = ['limit', 'month_start']
    query_filter = []
    query_params = []

    if month_start := getattr(search_options, 'month_start', False):
        query_filter.append('\nAND month_start IN UNNEST(@month_start)')
        query_params.append(bigquery.ArrayQueryParameter('month_start', 'TIMESTAMP', [dt.strftime('%Y-%m-01') for dt in month_start]))

    query_body = query_builder(search_options, special_fields)
    query_filter.extend(query_body[0])
    query_params.extend(query_body[1])

    if limit_val := getattr(search_options, 'limit', False):
        query_filter.append('\nLIMIT @limit')
        query_params.append(bigquery.ScalarQueryParameter('limit', 'INTEGER', limit_val))

    query = ''.join([f'SELECT* FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.stats_monthly_district_pricecatcher_transactions`\nWHERE 1=1', *query_filter])
    query_config = bigquery.QueryJobConfig(query_parameters=query_params)

    stats_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not stats_list:
        raise HTTPException(status_code=204)

    return {'name': 'monthly_stats_search', 'data': stats_list}