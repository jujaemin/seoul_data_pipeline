from airflow.models import Variable
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from plugins.utils import RequestTool, FileManager
from plugins.s3 import S3Helper
import requests
import json
import csv
import os
import logging
import fcntl
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_date': '{{ds}}'
}

bucket_name = Variable.get('bucket_name')
s3_key_path = 'raw_data/seoul_air/'
base_url = 'http://openAPI.seoul.go.kr:8088'

req_params = {
        "KEY": Variable.get('api_key_seoul'),
        "TYPE": 'json',
        "SERVICE": 'DailyAverageCityAir',
        "START_INDEX": 1,
        "END_INDEX": 1000,
        "MSRDT_DE": '{{ ds_nodash }}'
    }

@dag(
    schedule_interval='@daily',
    catchup=True,
    default_args=default_args
)
def etl_seoul_air():
    @task()
    def extract(req_params: dict):
        verify=False
        json_result = RequestTool.api_request(base_url, verify, req_params)
        logging.info('JSON data has been extracted.')
        return json_result

    @task()
    def transform(json_extracted, execution_date: str):
        # Extract data from the JSON
        records = json_extracted[req_params["SERVICE"]]["row"]

        # Prepare a list of dictionaries for CSV conversion
        data_for_csv = []
        for record in records:
            data_for_csv.append({
                'MSRDT_DE': record.get('MSRDT_DE', ''),
                'MSRRGN_NM': record.get('MSRRGN_NM', ''),
                'MSRSTE_NM': record.get('MSRSTE_NM', ''),
                'PM10': record.get('PM10', ''),
                'PM25': record.get('PM25', ''),
                'O3': record.get('O3', ''),
                'NO2': record.get('NO2', ''),
                'CO': record.get('CO', ''),
                'SO2': record.get('SO2', '')
            })

        # Convert the data to a DataFrame
        df = pd.DataFrame(data_for_csv)

        # Define the filename for the CSV file
        path = 'temp/seoul_air'
        filename = f'{path}/{execution_date}.csv'

        # Save the DataFrame as CSV
        FileManager.mkdir(path)
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        logging.info(f'Data has been transformed to CSV. The filename is {filename}')
        return filename

    @task()
    def load(filename: str, execution_date: str, **context):
        s3_conn_id = 'aws_conn_id'
        bucket_name = "de-team5-s3-01"
        key = s3_key_path + f'{execution_date}.csv'
        replace = True

        try:
            # Upload to S3
            S3Helper.upload(s3_conn_id, bucket_name, key, filename, replace)
        except Exception as e:
            logging.error(f'Error occured during loading to S3: {str(e)}')
            raise
        logging.info('CSV file has been loaded to S3.')
        # Remove local file
        FileManager.remove(filename)

    #req_params = prepare()
    json = extract(req_params)
    filename = transform(json)
    load(filename)

# DAG instance
etl_seoul_air_dag = etl_seoul_air()