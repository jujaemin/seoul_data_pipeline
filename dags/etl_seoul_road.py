from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from plugins.utils import RequestTool, FileManager
from plugins.s3 import S3Helper
from datetime import datetime, timedelta
import logging
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_date': '{{ds}}'
}

api_key = Variable.get('api_key_seoul')
bucket_name = Variable.get('bucket_name')
s3_key_path = 'raw_data/seoul_road/'
base_url = 'https://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TopisIccStTimesRoadDivTrfLivingStats/1.0'


@task()
def prepare(execution_date: str):
    # 데이터 자체는 1시간 단위지만, API 데이터 업데이트 주기가 하루에 1번 입니다. 즉, 하루 치 데이터가 모여서 한 번에 들어옵니다.
    startRow = 1
    # 도심생활권, 동남생활권, 동북생활권, 서남생활권, 서북생활권 5개 권역이 1시간마다 1번 기록됩니다. 5개 권역 x 24시간 = 120개
    rowCnt = "120"
    parsed_date = datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y%m%d")
    req_params = {
        "startRow": startRow,
        "apikey": api_key,
        "stndDt": f"{parsed_date}",
        "rowCnt": rowCnt,
    }
    logging.info('Parameters for request is ready.')
    return req_params


@task()
def extract(req_params: dict, execution_date: str):
    verify = False
    json_result = RequestTool.api_request(base_url, verify, req_params)
    logging.info('JSON data has been extracted.')
    return json_result


@task()
def transform(json_extracted, execution_date: str):
    # filename of the csv file to be finally saved
    path = 'temp/seoul_road'
    filename = f'{path}/{execution_date}.csv'
    df = pd.DataFrame(json_extracted)

    # # Cleansing
    # df.dropna(inplace=True)
    # df.drop_duplicates(inplace=True)

    # make temporary directory
    FileManager.mkdir(path)

    # Save as CSV
    df.to_csv(filename, header=False, index=False, encoding="utf-8-sig")

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


with DAG(
    dag_id='etl_seoul_road',
    schedule_interval='@daily',
    catchup=True,
    default_args=default_args
) as dag:
    # TaskFlow
    req_params = prepare()
    json = extract(req_params)
    filename = transform(json)
    load(filename)
