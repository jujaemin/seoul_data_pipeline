from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import json
import csv
import boto3
import io
import logging

@task
def extract(api_key, reg_dttm):
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
            logging.info("GET Request Success")
        except Exception as e:
            logging.info("GET Request Failed", e)
        
        # Serialize
        serialized_data = json.dumps(response.json())
    
    logging.info("extract end")
    return serialized_data

@task
def transform(serialized_data):
    logging.info("transform start")
    
    # Deserialize
    deserialized_data = json.loads(serialized_data)
    # 데이터 중 record 있는 부분만 선택
    records = deserialized_data['IotVdata017']['row']

    transformed_records = []
    for record in records:
        # record에서 원하는 컬럼만 선택 (일단은 필수 요소들만 선택해서 적재 테스트)
        transformed_records.append((record['SENSING_TIME'],
                                    record['REG_DTTM'],
                                    record['REGION'],
                                    record['AUTONOMOUS_DISTRICT'],
                                    record['ADMINISTRATIVE_DISTRICT'],
                                    record['AVG_NOISE'],
                                    record['SENSING_TIME']))

    logging.info("transform end")
    return transformed_records

@task
def load(aws_access_key_id, aws_secret_access_key, region_name, s3_bucket_name, s3_key, records):
    logging.info("load start")
    
    # s3 Client 세팅
    try:
        s3_client = boto3.client('s3', 
                                aws_access_key_id = aws_access_key_id,
                                aws_secret_access_key = aws_secret_access_key,
                                region_name = region_name)
        logging.info("Set AWS Client Success")
    except Exception as e:
        logging.info("Set AWS Client Failed", e)
    
    # 임시 Buffer에 csv 형식으로 record를 저장
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(records)
    
    # s3에 업로드
    try:
        s3_client.put_object(Bucket = s3_bucket_name,
                             Key = s3_key,
                             Body = csv_buffer.getvalue())
        csv_buffer.close()
        logging.info("File Upload Success")
    except Exception as e:
        logging.info("File Upload Failed", e)
    
    logging.info("load end")

with DAG(
    dag_id='seoul_noise',
    start_date=datetime(2024, 1, 1),
    schedule='45 * * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
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
    reg_dttm = "2024-02-06"

    # S3 버킷 및 파일 경로 설정
    s3_bucket_name = "de-team5-s3-01"
    s3_key = "raw_data/seoul_noise/" + reg_dttm + ".csv"
    
    # extract & transform
    records = transform(extract(api_key, reg_dttm))
    # load
    load(aws_access_key_id, aws_secret_access_key, region_name, s3_bucket_name, s3_key, records)