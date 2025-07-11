import os
import re
import logging
from io import StringIO
from pathlib import Path
from ast import literal_eval
from dotenv import load_dotenv
from pydantic import BaseModel
from google.cloud import bigquery
from collections import defaultdict

DATATYPE_MATCHING = {
    'int': 'INTEGER',
    'str': 'STRING',
    'datetime.date': 'TIMESTAMP',
}

FILEPATH = Path(__file__).parents[1]
print(os.environ.keys())
if load_dotenv(FILEPATH / 'secrets/.env', override=True):
    print('Loaded .env file via dotenv.')
elif env_file := os.getenv('ENV_FILE'):
    load_dotenv(stream=StringIO(env_file))
    print('Loaded .env file via StringIO.')
else:
    raise ValueError('Failed to load .env variables')

client = bigquery.Client.from_service_account_json(FILEPATH / 'secrets' / os.getenv('SERVICE_ACCOUNT_FILE'))
logger = logging.getLogger(__name__)
logging.basicConfig(filename="logs/api_utils.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def infer_model_datatypes(model: BaseModel):
    # Iterates over Pydantic model fields to match them with 
    datatype_dict = defaultdict(list)
    for name, datatype in model.model_fields.items():
        field_annotation = str(datatype.annotation)
        if arr_type := re.search(r'(list|tuple)(?=\[.*?\])', field_annotation):
            datatype_dict[name].append(arr_type.group(0))

        if data_type := re.search(r'(?<=\[)[\w\.]*?(?=\])', field_annotation):
            datatype_dict[name].append(DATATYPE_MATCHING.get(data_type.group(0), 'NULL'))

        if literal_type := re.search(r'(?<=typing\.Literal)\[.*?\]', field_annotation):
            datatype_dict[name].extend([*{
                DATATYPE_MATCHING.get(type(obj).__name__, 'NULL')
                for obj in literal_eval(literal_type.group(0))
            }])
    return datatype_dict


def query_builder(search_options: BaseModel, special_fields: list):
    query_filter = []
    query_params = []
    for search_field, datatype in infer_model_datatypes(search_options).items():
        if search_field in special_fields:
            continue

        search_data = getattr(search_options, search_field, False)
        if 'list' in datatype and search_data:
            if len(datatype[1:]) > 1:
                logging.warning('Mixed datatypes detected in input, attempting to coerce data into string.')
                search_data = [str(i) for i in search_data]
                datatype = ['list', 'STRING']

            query_filter.append(
                f'\nAND {search_field} IN UNNEST(@{search_field})' if search_data else f'\nAND {search_field} IS NOT NULL'
            )
            query_params.append(
                bigquery.ArrayQueryParameter(search_field, datatype[1], search_data)
            )

        elif 'tuple' in datatype:
            search_start, search_end = None, None
            try:
                search_start, search_end = search_data
                assert isinstance(search_start, int)
                assert isinstance(search_end, int)
            except (ValueError, TypeError):
                logging.info(f'Omitting {search_field} from filter')
                continue
            except AssertionError:
                logging.warning('Non-integral datatype detected in input, attempting to coerce data into integer.')
                try:
                    search_start = int(search_data[0])
                    search_end = int(search_data[1])
                except TypeError:
                    logging.error(f'Failed to convert inputs into integers, skipping {search_field}.')

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
            query_filter.append(f'\nAND {search_field} = @{search_field}' if search_data else f'\nAND {search_field} IS NOT NULL')
            query_params.append(bigquery.ScalarQueryParameter(search_field, datatype[1], search_data))

        else:
            logger.warning(f'Unknown datatype {datatype} for {search_field}, skipping {search_field} filter.')

    return query_filter, query_params


if __name__ == '__main__':
    pass