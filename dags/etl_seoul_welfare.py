from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from plugins.utils import RequestTool, FileManager
from plugins.s3 import S3Helper
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_date': '{{ds}}'
}

bucket_name = Variable.get('bucket_name')
s3_key_path = 'raw_data/seoul_welfare/'
base_url = 'http://openAPI.seoul.go.kr:8088'

req_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'fcltOpenInfo',
    "START_INDEX": 1,
    "END_INDEX": 1,
}

@task()
def extract_and_transform(req_params: dict, execution_date: str):
    #extract
    verify = False
    json = RequestTool.api_request(base_url, verify, req_params)
    total_count = json[req_params["SERVICE"]]["list_total_count"]
    logging.info('JSON data has been extracted.')
    
    #transform
    data_for_csv = []
    for i in range(total_count//1000 + 1):
        req_params["START_INDEX"] = i*1000 + 1
        req_params["END_INDEX"] = req_params["START_INDEX"] + 999
        json_extracted = RequestTool.api_request(base_url, verify, req_params)
        records = json_extracted[req_params["SERVICE"]]["row"]
        
        data_for_csv.extend([{
            'FCLT_NM': record.get('FCLT_NM', ''),
            'FCLT_CD': record.get('FCLT_CD', ''),
            'FCLT_KIND_NM': record.get('FCLT_KIND_NM', ''),
            'FCLT_KIND_DTL_NM': record.get('FCLT_KIND_DTL_NM', ''),
            'JRSD_SGG_SE': record.get('JRSD_SGG_SE', ''),
            'RPRSNTV': record.get('RPRSNTV', ''),
            'JRSD_SGG_CD': record.get('JRSD_SGG_CD', ''),
            'JRSD_SGG_NM': record.get('JRSD_SGG_NM', ''),
            'FCLT_ADDR': record.get('FCLT_ADDR', ''),
            'INMT_GRDN_CNT': record.get('INMT_GRDN_CNT', ''),
            'LVLH_NMPR': record.get('LVLH_NMPR', ''),
            'FCLT_TEL_NO': record.get('FCLT_TEL_NO', ''),
            'FCLT_ZIPCD': record.get('FCLT_ZIPCD', '')
        } for record in records])
        
    df = pd.DataFrame(data_for_csv)
    path = 'temp/seoul_welfare'
    filename = f'{path}/{execution_date}.csv'

    FileManager.mkdir(path)
    df.to_csv(filename, index=False, encoding="utf-8-sig")

    logging.info(f'Data has been transformed to CSV. The filename is {filename}')
    return filename

@task()
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
    dag_id='etl_seoul_welfare',
    schedule_interval='@monthly',
    catchup=True,
    default_args=default_args
) as dag:
    filename = extract_and_transform(req_params)
    load(filename)