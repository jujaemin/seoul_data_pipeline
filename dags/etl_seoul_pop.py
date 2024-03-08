from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from plugins.utils import RequestTool, FileManager
from plugins.s3 import S3Helper

import pandas as pd
import datetime
import logging

default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'execution_date': '{{  macros.ds_add(ds, -4) }}',
    }

req_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'SPOP_DAILYSUM_JACHI',
    "START_INDEX": 1,
    "END_INDEX": 1000,
    "MSRDT_DE": 'execution_date'.strftime("%Y-%m-%d").replace('-','')
}

bucket_name = Variable.get('bucket_name')
s3_key_path = 'raw_data/seoul_pop/'
base_url = 'http://openAPI.seoul.go.kr:8088'

@task
def extract(req_params: dict):
    
    verify = False
    
    json_result = RequestTool.api_request(base_url, verify, req_params)
    
    logging.info('Success : pop_extract')


    return json_result

@task
def transform(json_extracted, execution_date: str):

    try:

        df = pd.DataFrame(json_extracted['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        path = 'temp/seoul_pop'
        filename = f'{path}/{execution_date}.csv'

        FileManager.mkdir(path)
        life_people_data.to_csv(filename, index=False, header = False, encoding="utf-8-sig")

        logging.info('Success : pop_transform')

        return filename
    
    except Exception as e:

        logging.info('no data found')
        logging.info(e)

        return None

@task
def load(filename: str, execution_date: str, **context):
    s3_conn_id = 'aws_conn_id'
    key = s3_key_path + f'{execution_date}.csv'
    replace = True

    try:
        S3Helper.upload(s3_conn_id, bucket_name, key, filename, replace)
        
    except Exception as e:
        logging.error(f'Error occurred during loading to S3: {str(e)}')
        raise
    logging.info('CSV file has been loaded to S3.')
    FileManager.remove(filename)


with DAG(
    dag_id = 'etl_seoul_population',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = default_args
) as dag:
    aws_conn_id='aws_default'

    json = extract(req_params)
    filename = transform(json)
    load(filename)
