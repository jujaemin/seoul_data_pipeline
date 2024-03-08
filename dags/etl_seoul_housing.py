from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from plugins.utils import RequestTool, FileManager
from plugins.s3 import S3Helper

import requests
import pandas as pd
import datetime
import logging


req_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'tbLnOpendataRtmsV',
    "START_INDEX": 1,
    "END_INDEX": 1000,
    "MSRDT_DE": '{{ ds_nodash }}'
}

bucket_name = Variable.get('bucket_name')
s3_key_path = 'raw_data/seoul_housing/'
base_url = 'http://openAPI.seoul.go.kr:8088'

@task
def extract(req_params: dict):

    result = []
    verify = False

    start_date = datetime.datetime(2024,1,1).date()
    end_date = datetime.datetime.today().date()
    current_date = start_date

    while current_date <= end_date:
        date = current_date.strftime("%Y-%m-%d").replace('-','')
        try:
            json_result = RequestTool.api_request(base_url, verify, req_params)
        
            result.append(json_result)
            current_date += timedelta(days=1)
        except:
            current_date += timedelta(days=1)

    logging.info('Success : housing_extract')

    return result

@task
def transform(json_extracted, execution_date: str):
    
    for response in json_extracted:

        try:
            df = pd.DataFrame(response['tbLnOpendataRtmsV']['row'])

            housing_data = df[['DEAL_YMD', 'SGG_NM', 'BLDG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]
            result.append([housing_data, date])
             path = 'temp/seoul_housing'
            filename = f'{path}/{execution_date}.csv'

            FileManager.mkdir(path)
            df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        except Exception as e:
            logging.info(e)
            pass

    logging.info('Success : housing_transform')
        
    return filename

@task
def load(filename: str, execution_date: str, **context):
    
    s3_conn_id = 'aws_conn_id'
    key = s3_key_path + f'{execution_date}.csv'
    replace = True
    
    for record in filename:
        try:
            S3Helper.upload(s3_conn_id, bucket_name, key, record, replace)

            FileManager.remove(record)
        except:
            pass

        logging.info('Success : housing_load')

with DAG(
    dag_id = 'etl_seoul_housing',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_date': '{{ds}}'
    }
) as dag:
    aws_conn_id='aws_default'

    json = extract(req_params)
    filename = transform(json)
    load(filename)

