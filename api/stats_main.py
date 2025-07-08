import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException
from api_utils import infer_model_datatypes
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


@app.post('/pricecatcher/stats/search')
async def search_stats(search_options: PricecatcherStatsSearch):
    datatype_dict = infer_model_datatypes(search_options)
    special_fields = ['limit', 'month_start']
    query_filter = []
    query_params = []

    for search_field, datatype in datatype_dict.items():
        if search_field in special_fields:
            continue

        if 'list' in datatype:
            search_data = getattr(search_options, search_field, False)
            if len(datatype[1:]) > 1:
                print('Warning: mixed datatypes in input, attempting to coerce data into strings')
                search_data = [str(i) for i in search_data]
                datatype = ['list', 'STRING']

            query_filter.append(
                f'\nAND {search_field} IN UNNEST(@{search_field})' if search_data else f'\nAND {search_field} IS NOT NULL'
            )
            if search_data:
                query_params.append(
                    bigquery.ArrayQueryParameter(search_field, datatype[1], search_data)
                )

        elif 'tuple' in datatype:
            try:
                search_start, search_end = getattr(search_options, search_field, (0, 0))
            except (AttributeError, TypeError):
                query_filter.append(f'\nAND {search_field} IS NOT NULL')

            if search_start and search_end:
                query_filter.append(f'\nAND {search_field} BETWEEN @{search_field}_start AND @{search_field}_end')
                query_params.extend([
                    bigquery.ScalarQueryParameter(f'{search_field}_start', datatype[1], search_start),
                    bigquery.ScalarQueryParameter(f'{search_field}_end', datatype[1], search_end),
                ])
            elif search_start and not search_end:
                query_filter.append(f'\nAND {search_field} >= @{search_field}_start')
                query_params.append(bigquery.ScalarQueryParameter(f'{search_field}_start', datatype[1], search_start))
            elif search_end and not search_start:
                query_filter.append(f'\nAND {search_field} <= @{search_field}_end')
                query_params.append(bigquery.ScalarQueryParameter(f'{search_field}_end', datatype[1], search_end))
            else:
                query_filter.append(f'\nAND {search_field} IS NOT NULL')

        elif len(datatype) < 2:
            search_data = getattr(search_options, search_field, False)
            query_filter.append(f'\nAND {search_field} = @{search_field}' if search_data else f'\nAND {search_field} IS NOT NULL')
            query_params.append(bigquery.ScalarQueryParameter(search_field, datatype[1], search_data))

    if month_start := getattr(search_options, 'month_start', False):
        query_filter.append('\nAND month_start IN UNNEST(@month_start)')
        query_params.append(bigquery.ArrayQueryParameter('month_start', 'TIMESTAMP', [dt.strftime('%Y-%m-01') for dt in month_start]))

    if limit_val := getattr(search_options, 'limit', False):
        query_filter.append('\nLIMIT @limit')
        query_params.append(bigquery.ScalarQueryParameter('limit', 'INTEGER', limit_val))


    query = ''.join([f'SELECT* FROM `{GCP_PROJECT_NAME}.{GCP_DATASET_NAME}.stats_monthly_district_pricecatcher_transactions`\nWHERE 1=1', *query_filter])
    print(70*'=', query, 70*'=', sep='\n')
    print(70*'=', *query_params, 70*'=', sep='\n')
    query_config = bigquery.QueryJobConfig(query_parameters=query_params)

    stats_list = [dict(row) for row in client.query_and_wait(query=query, job_config=query_config)]

    if not stats_list:
        raise HTTPException(status_code=204)

    return {'name': 'monthly_stats_search', 'data': stats_list}