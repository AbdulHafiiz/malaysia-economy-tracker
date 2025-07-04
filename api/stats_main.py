import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException
from models.stats_models import PremiseSearchOptions, ItemSearchOptions


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
    query_filter = [
        f'\nAND {search_field} IN UNNEST(@{search_field})' if getattr(search_options, search_field, False) else f'\nAND {search_field} IS NOT NULL'
        for search_field in ItemSearchOptions.model_fields.keys()
        if search_field != 'limit'
    ]
    if search_options.limit:
        query_filter.append('\nLIMIT @limit')

    query = ''.join([f'SELECT * FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.pricecatcher_item_lookup` WHERE 1=1', *query_filter])
    params = [
        *[
            bigquery.ArrayQueryParameter(search_field, 'STRING', getattr(search_options, search_field))
            for search_field in ItemSearchOptions.model_fields.keys()
            if search_field not in ['limit', 'item_code'] and getattr(search_options, search_field)
        ],
        bigquery.ScalarQueryParameter('limit', 'INTEGER', search_options.limit),
    ]
    if (item_code := getattr(search_options, 'item_code')):
        params.append(bigquery.ArrayQueryParameter('item_code', 'INTEGER', item_code))
    query_config = bigquery.QueryJobConfig(query_parameters=params)

    print(client.query(query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=True, query_parameters=params)).query)
    item_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not item_list:
        raise HTTPException(status_code=204)

    return {'name': 'item_search', 'data': item_list}


@app.post('/pricecatcher/premise/search/')
async def search_premise(search_options: PremiseSearchOptions):
    query_filter = [
        f'\nAND {search_field} IN UNNEST(@{search_field})' if getattr(search_options, search_field, False) else f'\nAND {search_field} IS NOT NULL'
        for search_field in PremiseSearchOptions.model_fields.keys()
        if search_field != 'limit'
    ]
    if search_options.limit:
        query_filter.append('\nLIMIT @limit')

    query = ''.join([f'SELECT * FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.pricecatcher_premise_lookup` WHERE 1=1', *query_filter])
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            *[
                bigquery.ArrayQueryParameter(search_field, 'STRING', getattr(search_options, search_field))
                for search_field in PremiseSearchOptions.model_fields.keys()
                if search_field != 'limit' and getattr(search_options, search_field)
            ],
            bigquery.ScalarQueryParameter('limit', 'INTEGER', search_options.limit),
        ]
    )
    premise_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not premise_list:
        raise HTTPException(status_code=204)

    return {'name': 'premise_search', 'data': premise_list}