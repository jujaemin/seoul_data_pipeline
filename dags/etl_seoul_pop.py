from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from utils import FileManager
from s3 import S3Helper

import requests
import pandas as pd
import datetime
import logging




@task
def extract(base_url, **context):
    
    day = context["execution_date"] - timedelta(days=4)
    date = day.date().strftime('%Y-%m-%d')
    url = base_url+f'/{api}/json/SPOP_DAILYSUM_JACHI/1/1000/'+date.replace('-', '')
    res = requests.get(url)
    json = res.json()
    
    logging.info('Success : pop_extract')


    return [json, date]

@task
def transform(response):

    try:
        data = response[0]
        date = response[1]

        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        logging.info('Success : pop_transform')

        return [life_people_data, date]
    
    except Exception as e:

        logging.info(e)

        return None

@task
def load(record):

    try:
        data = record[0]
        date = record[1]

        file_name = f'{date}.csv'
        file_path = 'temp/seoul_pop'

        path = file_path + '/' + file_name
        
        FileManager.mkdir(file_path)

        data.to_csv(path, header = False, index = False, encoding='utf-8-sig')

        logging.info('Success : pop_load')

        return [path, file_name]
    
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

        logging.info(f'Success : pop_upload ({file_name})')
    
    except Exception as e:
        logging.info('no data found')
        logging.info(e)
        pass


with DAG(
    dag_id = 'etl_seoul_population1',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'
    key = 'raw_data/seoul_pop/'
    base_url = 'http://openAPI.seoul.go.kr:8088'
    api= Variable.get('api_key_seoul')

    records = transform(extract(base_url))

    upload(load(records))
