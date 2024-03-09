from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta,datetime
from plugins.utils import FileManager, RequestTool
from plugins.s3 import S3Helper
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


import pandas as pd
import logging

default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'execution_date': '{{ds}}'
    }

air_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'DailyAverageCityAir',
    "START_INDEX": 1,
    "END_INDEX": 1000,
    "MSRDT_DE": '{{ ds_nodash }}'
}

housing_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'tbLnOpendataRtmsV',
    "START_INDEX": 1,
    "END_INDEX": 1000,
    "EXTRA": " / / / / / / / / ",
    "DATE": '{{ ds_nodash }}'
}

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
def road_extract(req_params: dict):
    verify = False
    json_result = RequestTool.api_request(road_url, verify, req_params)
    logging.info('JSON data has been extracted.')
    return json_result

@task()
def air_extract(req_params: dict):
    verify = False
    json_result = RequestTool.api_request(base_url, verify, req_params)
    logging.info('JSON data has been extracted.')
    return json_result

@task
def pop_extract(**context):

    req_params = {
    "KEY": Variable.get('api_key_seoul'),
    "TYPE": 'json',
    "SERVICE": 'SPOP_DAILYSUM_JACHI',
    "START_INDEX": 1,
    "END_INDEX": 1000,
}
    
    day = context["execution_date"] - timedelta(days=4)
    date = day.date().strftime('%Y-%m-%d')
    req_params['DATE'] = date.replace('-', '')
    json = RequestTool.api_request(base_url, False, req_params)
    
    logging.info('Success : pop_extract')


    return [json, date]

@task
def housing_extract(req_params):
    verify = False
    json_result = RequestTool.api_request(base_url, verify, req_params)

    logging.info('Success : housing_extract')

    return json_result

@task()
def road_transform(json_extracted, execution_date: str):
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
def air_transform(json_extracted, execution_date: str):
    try:
        records = json_extracted[air_params["SERVICE"]]["row"]
        data_for_csv = [{
            'MSRDT_DE': record.get('MSRDT_DE', ''),
            'MSRRGN_NM': record.get('MSRRGN_NM', ''),
            'MSRSTE_NM': record.get('MSRSTE_NM', ''),
            'PM10': record.get('PM10', ''),
            'PM25': record.get('PM25', ''),
            'O3': record.get('O3', ''),
            'NO2': record.get('NO2', ''),
            'CO': record.get('CO', ''),
            'SO2': record.get('SO2', '')
        } for record in records]

        df = pd.DataFrame(data_for_csv)
        path = 'temp/seoul_air'
        filename = f'{path}/{execution_date}.csv'

        FileManager.mkdir(path)
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        logging.info(f'Data has been transformed to CSV. The filename is {filename}')

        return filename

    except Exception as e:
        logging.info(e)
        pass

@task
def pop_transform(response):

    try:
        data = response[0]
        date = response[1]

        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        path = 'temp/seoul_pop'
        filename = f'{path}/{date}.csv'

        FileManager.mkdir(path)
        life_people_data.to_csv(filename, index=False, header=False, encoding="utf-8-sig")

        logging.info('Success : pop_transform')

        return [filename, date]
    
    except Exception as e:

        logging.info(e)

        return None

@task
def housing_transform(json_extracted, execution_date: str):

    try:
        df = pd.DataFrame(json_extracted['tbLnOpendataRtmsV']['row'])

        housing_data = df[['DEAL_YMD', 'SGG_NM', 'BLDG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]

        path = '/works/Seoul_housing/'
        filename = f'{path}/{execution_date}.csv'

        FileManager.mkdir(path)
        housing_data.to_csv(filename, index=False, header=False, encoding="utf-8-sig")

        logging.info('Success : housing_transform')

        return filename
    
    except Exception as e:
        logging.info(e)
        pass

@task()
def road_load(filename: str, execution_date: str):
    s3_key_path = 'raw_data/seoul_road/'
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

@task()
def air_load(filename: str, execution_date: str, **context):
    s3_key_path = 'raw_data/seoul_air/'
    s3_conn_id = 'aws_conn_id'
    key = s3_key_path + f'{execution_date}.csv'
    replace = True

    try:
        S3Helper.upload(s3_conn_id, bucket_name, key, filename, replace)
        FileManager.remove(filename)
    except Exception as e:
        logging.error(f'Error occurred during loading to S3: {str(e)}')
        pass
    logging.info('CSV file has been loaded to S3.')


@task
def pop_load(file):
    key = 'raw_data/seoul_pop/'
    try:
        local_file = file[0]
        date = file[1]

        file_name = f'{date}.csv'
        s3_key = key + str(file_name)

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local_file, True)

        FileManager.remove(local_file)

        logging.info(f'Success : pop_upload ({file_name})')
    
    except Exception as e:
        logging.info(e)

@task
def housing_load(filename: str, execution_date: str, **context):
    s3_key_path = 'raw_data/seoul_housing/'
    s3_conn_id = 'aws_conn_id'
    key = s3_key_path + f'{execution_date}.csv'
    replace = True

    try:
        S3Helper.upload(s3_conn_id, bucket_name, key, filename, replace)
        FileManager.remove(filename)
        logging.info('CSV file has been loaded to S3.')

    except Exception as e:
        logging.error(f'Error occurred during loading to S3: {str(e)}')
        

with DAG(
    dag_id = 'etl_daily',
    start_date = datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = default_args
) as dag:
    aws_conn_id='aws_conn_id'
    bucket_name = 'de-team5-s3-01'
    base_url = 'http://openAPI.seoul.go.kr:8088'
    road_url = 'https://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TopisIccStTimesRoadDivTrfLivingStats/1.0'
    api= Variable.get('api_key_seoul')
    api_key = Variable.get('api_key_road')

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='cleaning_daily',
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success', 'failed', 'upstream_failed']
    )

    road_params = prepare()
    road_json = road_extract(road_params)
    road_file = road_transform(road_json)
    road_task = road_load(road_file)

    air_json = air_extract(air_params)
    air_file = air_transform(air_json)
    air_task = air_load(air_file)

    pop_json = pop_extract()
    pop_file = pop_transform(pop_json)
    pop_task = pop_load(pop_file)

    housing_json = housing_extract(housing_params)
    housing_file = housing_transform(housing_json)
    housing_task = housing_load(housing_file)

    [road_task, air_task, pop_task, housing_task] >> trigger_dag_task


