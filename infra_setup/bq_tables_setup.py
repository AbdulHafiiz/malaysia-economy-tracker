import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

FILEPATH = Path(__file__).parents[1]
load_dotenv(FILEPATH / 'secrets/.env', override=True)

GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_NAME')
GCP_DATASET_NAME = os.getenv('GCP_DATASET_NAME')

BIGQUERY_TIME_PARTITIONS = {
    'HOUR': bigquery.TimePartitioningType.HOUR,
    'DAY': bigquery.TimePartitioningType.DAY,
    'MONTH': bigquery.TimePartitioningType.MONTH,
    'YEAR': bigquery.TimePartitioningType.YEAR,
}

logger = logging.getLogger(__name__)
logging.basicConfig(filename="logs/bq_table_setup.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_schema(schema_path:str|Path) -> list:
    with open(schema_path, 'r') as f:
        json_data = json.loads(f.read())
        schema_json = json_data['columns']
        partition_json = json_data['partitions']

    table_schema = [
        bigquery.SchemaField(**field)
        for field in schema_json
    ]
    table_partition = None

    if partition_json['type'] == 'TIME':
        table_partition = bigquery.TimePartitioning(
            type_=BIGQUERY_TIME_PARTITIONS[partition_json['range']],
            field=partition_json['field']
        )
    elif partition_json['type'] == 'INTEGER':
        table_partition = bigquery.TimePartitioning(
            range_=bigquery.PartitionRange(interval=partition_json['range']),
            field=partition_json['field']
        )
    else:
        logging.warning(f'Partition type {partition_json["type"]} does not match "TIME" or "INTEGER". Select one of the two to partition the table. Defaulting to no partition.')

    return table_schema, table_partition


def check_table_exists(client:bigquery.Client, table_name:str) -> bool:
    try:
        client.get_table(table_name)
        return True
    except NotFound:
        return False


def create_datasets(dir_path:str|Path = FILEPATH / 'infra_setup/schemas') -> None:
    client = bigquery.Client.from_service_account_json(FILEPATH / 'secrets' / os.getenv('SERVICE_ACCOUNT_FILE'))
    dataset = client.dataset(GCP_DATASET_NAME)
    table_status = {
        'failed': [],
        'successful': [],
        'exists': []
    }

    table_datafiles = os.scandir(dir_path)
    if not table_datafiles:
        logging.warning('Target filepath has no schema json files')
        return

    for filepath in table_datafiles:
        if not filepath.name.endswith('_schema.json') or 'template' in filepath.name:
            continue

        table_name = filepath.name.replace('_schema.json', '')
        table_ref = dataset.table(table_name)
        try:
            if check_table_exists(client, table_ref):
                logging.info(f'Table {table_name} already exists, skipping')
                table_status['exists'].append(table_name)
                continue

            table_schema, table_partition = parse_schema(schema_path=filepath.path)
            logging.info(f'Parsed schema for table {table_name}')

            table = bigquery.Table(table_ref, table_schema)
            if isinstance(table_partition, bigquery.TimePartitioning):
                table.time_partitioning = table_partition
                logging.info(f'Adding `Time Partitioning` to table on column: {table_partition.field}')
            elif isinstance(table_partition, bigquery.RangePartitioning):
                table.range_partitioning = table_partition
                logging.info(f'Adding `Range Partitioning` to table on column: {table_partition.field}')
            else:
                logging.info('Creating table without partition')
                pass
            
            table = client.create_table(table)

            table_status['successful'].append(table_name)
            logging.info(f'Created Bigquery table {table_name}')

        except Exception as err:
            table_status['failed'].append(table_name)
            logging.error(f'Warning - failed to create table {table_name}. Skipping table. {err}')

    logging.info(f'Succesfully created {len(table_status["successful"])} table{"s" if len(table_status["successful"]) > 0 else ""}: {", ".join(table_status["successful"])}')
    logging.info(f'Failed to create {len(table_status["failed"])} table{"s" if len(table_status["failed"]) > 0 else ""}: {", ".join(table_status["failed"])}')
    logging.info(f'{len(table_status["exists"])} table{"s" if len(table_status["exists"]) > 0 else ""} already exist: {", ".join(table_status["exists"])}')


if __name__ == '__main__':
    create_datasets()