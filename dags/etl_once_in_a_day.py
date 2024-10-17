from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.timetables.trigger import CronTriggerTimetable

from s3 import S3Helper
from utils import RequestTool

from datetime import datetime
from datetime import timedelta

from typing import List

import csv
import io
import logging

default_args = {
    "logical_date": "{{macros.ds_add(ds, -14)}}"
    }

@task
def extract(api_key: str, reg_dttm: str) -> List[dict]:
    logging.info("extract start")
    
    base_url = 'http://openAPI.seoul.go.kr:8088'
    
    gu_names = ["Jongno-gu",    "Jung-gu",      "Yongsan-gu",       "Seongdong-gu",
                "Gwangjin-gu",  "Dongdaemun-gu","Jungnang-gu",      "Seongbuk-gu",
                "Gangbuk-gu",   "Dobong-gu",    "Nowon-gu",         "Eunpyeong-gu",
                "Seodaemun-gu", "Mapo-gu",      "Yangcheon-gu",     "Gangseo-gu",
                "Guro-gu",      "Geumcheon-gu", "Yeongdeungpo-gu",  "Dongjak-gu",
                "Gwanak-gu",    "Seocho-gu",    "Gangnam-gu",       "Songpa-gu",
                "Gangdong-gu"]
    
    responses = []
    for gu_name in gu_names:
        logging.info(f"extracting... -> {reg_dttm} {gu_name}")
        
        req_params = {
                        "KEY": api_key,
                        "FILE_TYPE": 'json',
                        "SERVICE_NAME": 'IotVdata017',
                        "START_INDEX": "1",
                        "END_INDEX": "1000",
                        "GU_NAMES": gu_name,
                        "REG_DTTM": reg_dttm
                }
        
        # GET Request
        response = RequestTool.api_request(base_url, verify=True, params=req_params)
        
        # API 측으로부터 정상적인 메시지를 받았는 지 체크    
        if 'IotVdata017' not in response:
            logging.info("Something wrong from the API, or you should check the date.")
            logging.info(response)
            raise Exception("Something wrong from the API, or you should check the date.")
        else:
            responses.append(response)
    
    logging.info("extract end")
    return responses

@task
def transform(responses: List[dict]) -> List[dict]:
    logging.info("transform start")
    
    transformed_records = []
    for response in responses:
        # 데이터 중 records 있는 부분만 선택
        records = response['IotVdata017']['row']
        # 해당 records들을 최종 records 목록에 추가
        transformed_records += records

    logging.info("transform end")
    return transformed_records

@task
def load(s3_conn_id, bucket_name, key, transformed_records):
    logging.info("load start")
    
    # 임시 Buffer에 csv 형식으로 record를 저장
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=transformed_records[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(transformed_records)
    
    string_data = csv_buffer.getvalue()
    
    # S3에 업로드
    try:
        S3Helper.upload_string(s3_conn_id, string_data, key, bucket_name, replace=True)
        csv_buffer.close()
        logging.info("File Upload to S3 Success")
    except Exception as e:
        logging.info("File Upload to S3 Fail")
        logging.info(e)
    
    logging.info("load end")

with DAG(
    dag_id='etl_seoul_noise',
    start_date = datetime(2024, 2, 22), # API로부터 현재 시점으로부터 14일 전의 데이터를 가져오고, API는 최근 한 달 간의 데이터만을 제공하므로, 2가지 사항을 고려하여 적절하게 세팅해야 합니다.
    schedule = CronTriggerTimetable("0 5 * * *", timezone="UTC"), # 한국 시각 기준 매일 14시 00분 실행
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='cleaning_once_day',
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success', 'failed', 'upstream_failed']
    )
    
    # OPEN API 키 가져오기
    api_key = Variable.get("api_key_seoul")
    # AWS Conn 정보 가져오기
    aws_conn_id = 'aws_conn_id'

    # S3 버킷 및 Key 지정
    s3_bucket_name = "de-team5-s3-01"
    s3_key = "raw_data/seoul_noise/" + default_args["logical_date"] + ".csv"
    
    # extract & transform
    transformed_data = transform(extract(api_key, default_args["logical_date"]))
    # load
    once_task = load(aws_conn_id, s3_bucket_name, s3_key, transformed_data)

    once_task >> trigger_dag_task
