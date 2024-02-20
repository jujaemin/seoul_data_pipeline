from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from plugins.utils import FileManager, RequestTool
from plugins.s3 import S3Helper

import requests
import pandas as pd
import datetime
import os
import logging

req_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'SPOP_DAILYSUM_JACHI',
    "START_INDEX": 1,
    "END_INDEX": 1000,
    "MSRDT_DE": '{{ ds_nodash }}'
    }

@task
def extract(req_params: dict):
    verify=False
    result = RequestTool.api_request(base_url, verify, req_params)

    logging.info(f'Success : life_people_extract ({date})')

    return result

@task
def transform(response):

    data = response
    try:
        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        logging.info(f'Success : life_people_transform ({date})')

        return life_people_data
    
    except:

        logging.error(f'no data found : {date}')

        return None

@task
def load(record):

    try:
        data = record

        file_path = 'temp/Seoul_pop/'
        file_name = '{}.csv'.format(execution_date)

        FileManager.mkdir(file_path)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False, encoding='utf-8-sig')

        logging.info(f'Success : life_people_load ({date})')

        return local_file
    
    except TypeError:
        logging.error('no data found')
        return None

    
@task
def upload(file):
    
    try:
        local_file = file
        file_name = execution_date

        s3_key = key + str(file_name)

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local_file, True)

        FileManager.remove(local_file)

        logging.info(f'Success : life_people_upload ({file_name})')
    
    except:
        logging.error('no data found')
        pass


with DAG(
    dag_id = 'Seoul_Population',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'execution_date': '{{ds}}',
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'
    key = 'raw_data/seoul_pop/'
    base_url = 'http://openAPI.seoul.go.kr:8088'

    records = transform(extract(url))

    upload(load(records))
