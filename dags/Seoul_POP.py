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
def extract(url,**context):
    link = url
    execution_date = context["execution_date"] - timedelta(days=4)

    date = execution_date.date().strftime('%Y-%m-%d')
    url = link + date.replace('-', '')

    logging.info(f'Success : life_people_extract ({date})')

    return [url, date]

@task
def transform(response):

    res = requests.get(response[0])
    data = res.json()
    date = response[1]

    try:
        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        logging.info(f'Success : life_people_transform ({date})')

        return [life_people_data, date]
    
    except:

        logging.error(f'no data found : {date}')

        return None

@task
def load(record):

    try:
        data = record[0]
        date = record[1]

        file_path = '/works/Seoul_pop/'
        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False, encoding='utf-8-sig')

        logging.info(f'Success : life_people_load ({date})')

        return [local_file, file_name]
    
    except TypeError:
        logging.error('no data found')
        return None

    
@task
def upload(file):
    
    try:
        local_file = file[0]
        file_name = file[1]

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
    }
) as dag:
    url = Variable.get('pop_url')
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'
    key = 'raw_data/Seoul_POP/'

    records = transform(extract(url))

    upload(load(records))
