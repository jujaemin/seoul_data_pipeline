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


@task
def extract(base_url):

    result = []

    start_date = datetime.datetime(2024,1,1).date()
    end_date = datetime.datetime.today().date()
    current_date = start_date

    while current_date <= end_date:
        date = current_date.strftime("%Y-%m-%d").replace('-','')
        api= Variable.get('api_key_seoul')

        
        url = base_url+f'/{api}/json/tbLnOpendataRtmsV/1/1000/ / / / / / / / / /'+date

        result.append([url, str(current_date)])
        current_date += timedelta(days=1)
            

    logging.info('Success : housing_extract')

    return result

@task
def transform(responses):
    result = []

    for response in responses:

        try:
            res = response[0]
            date = response[1]
        
            res = requests.get(response[0])
            data = res.json()


            df = pd.DataFrame(data['tbLnOpendataRtmsV']['row'])

            housing_data = df[['DEAL_YMD', 'SGG_NM', 'BLDG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]
            result.append([housing_data, date])
        
        except:
            pass

    logging.info('Success : housing_transform')
        
    return result

@task
def upload(records):

    for record in records:
        data = record[0]
        date = record[1]

        file_name = f'{date}.csv'

        file_path = 'temp/seoul_housing'
        FileManager.mkdir(file_path)

        path = file_path + '/' + file_name

        s3_key = key + str(file_name)

        data.to_csv(path, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, path, True)


        FileManager.remove(path)

        logging.info('Success : housing_load')

with DAG(
    dag_id = 'Seoul_housing',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'
    key = 'raw_data/seoul_housing/'
    base_url = 'http://openAPI.seoul.go.kr:8088'



    records = transform(extract(base_url))

    upload(records)
