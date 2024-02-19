from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.timetables.trigger import CronTriggerTimetable

from datetime import datetime
from datetime import timedelta

from typing import List

import requests
import csv
import boto3
import io
import logging

@task
def extract(api_key: str, reg_dttm: str) -> List[dict]:
    logging.info("extract start")
    
    base_url = "http://openapi.seoul.go.kr:8088"
    file_type = "json"
    service_name = "IotVdata017"
    start_index = "1"
    end_index = "1000"
    gu_names = ["Jongno-gu",    "Jung-gu",      "Yongsan-gu",       "Seongdong-gu",
                "Gwangjin-gu",  "Dongdaemun-gu","Jungnang-gu",      "Seongbuk-gu",
                "Gangbuk-gu",   "Dobong-gu",    "Nowon-gu",         "Eunpyeong-gu",
                "Seodaemun-gu", "Mapo-gu",      "Yangcheon-gu",     "Gangseo-gu",
                "Guro-gu",      "Geumcheon-gu", "Yeongdeungpo-gu",  "Dongjak-gu",
                "Gwanak-gu",    "Seocho-gu",    "Gangnam-gu",       "Songpa-gu",
                "Gangdong-gu"]

    responses = []
    for gu_name in gu_names:
        url =   base_url + "/" + \
                api_key + "/" + \
                file_type + "/" + \
                service_name + "/" + \
                start_index + "/" + \
                end_index + "/" + \
                gu_name + "/" + \
                reg_dttm
        
        # GET Request
        try:
            response = requests.get(url)
            logging.info("GET Request Success" + f" ({gu_name})")   
        except Exception as e:
            logging.info("GET Request Failed" + f" ({gu_name})")
            logging.info(e)
        
        # API 측으로부터 정상적인 메시지를 받았는 지 체크    
        if response.json()['IotVdata017']['RESULT']['MESSAGE'] != "정상 처리되었습니다":
            logging.info("Something wrong from the API, or you should check the date.")
            raise Exception("Something wrong from the API, or you should check the date.")
        else:
            responses.append(response.json())
    
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
def load(aws_access_key_id, aws_secret_access_key, region_name, s3_bucket_name, s3_key, transformed_records):
    logging.info("load start")
    
    # s3 Client 세팅
    try:
        s3_client = boto3.client('s3', 
                                aws_access_key_id = aws_access_key_id,
                                aws_secret_access_key = aws_secret_access_key,
                                region_name = region_name)
        logging.info("Set AWS Client Success")
    except Exception as e:
        logging.info("Set AWS Client Failed")
        logging.info(e)
    
    # 임시 Buffer에 csv 형식으로 record를 저장
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=transformed_records[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(transformed_records)
    
    # s3에 업로드
    try:
        s3_client.put_object(Bucket = s3_bucket_name, 
                            Key = s3_key,
                            Body = csv_buffer.getvalue())
        csv_buffer.close()
        logging.info("File Upload Success")
    except Exception as e:
        logging.info("File Upload Failed")
        logging.info(e)
    
    logging.info("load end")

with DAG(
    dag_id='seoul_noise',
    start_date = datetime(2024, 1, 1),
    schedule = CronTriggerTimetable("0 5 * * *", timezone="UTC"), # 한국 시각 기준 매일 14시 00분 실행
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    # OPEN API 키 설정
    api_key = Variable.get("api_key_seoul")
    # AWS 계정 및 자격 증명 설정
    aws_access_key_id = Variable.get("authorization_aws_access_key_id")
    aws_secret_access_key = Variable.get("authorization_aws_secret_access_key")
    region_name = Variable.get("authorization_region_name")

    # API로부터 가져올 데이터의 등록일시 지정
    target_date = datetime.now() - timedelta(days=10)
    reg_dttm = target_date.strftime("%Y-%m-%d")

    # S3 버킷 및 파일 경로 설정
    s3_bucket_name = "de-team5-s3-01"
    s3_key = "raw_data/seoul_noise/" + reg_dttm + ".csv"
    
    # extract & transform
    transformed_records = transform(extract(api_key, reg_dttm))
    # load
    load(aws_access_key_id, aws_secret_access_key, region_name, s3_bucket_name, s3_key, transformed_records)