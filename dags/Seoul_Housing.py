from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from plugins.utils import FileManager
from plugins.s3 import S3Helper

import requests
import pandas as pd
import datetime
import os
import logging

@task
def extract(url):
    link = url
    result = []

    start_date = datetime.datetime(2024,1,1).date()
    end_date = datetime.datetime.today().date()
    current_date = start_date

    while current_date <= end_date:
        date = current_date.strftime("%Y-%m-%d").replace('-','')
        url = link + date

        result.append([url, str(current_date)])
        current_date += timedelta(days=1)

    logging.info(f'Success : housing_extract')

    return result

@task
def transform(responses):
    result = []

    for response in responses:
        res = requests.get(response[0])
        data = res.json()
        date = response[1]

        try:

            df = pd.DataFrame(data['tbLnOpendataRtmsV']['row'])

            housing_data = df[['DEAL_YMD', 'SGG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]
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

        file_path = '/works/Seoul_housing/'
        file_name = '{}.csv'.format(date)

        s3_key = key + str(file_name)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False)

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local_file, True)

        FileManager.remove(local_file)

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
    url = Variable.get('housing_url')
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'
    key = 'raw_data/Seoul_housing/'

    records = transform(extract(url))

    upload(records)

