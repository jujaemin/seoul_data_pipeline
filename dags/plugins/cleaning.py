from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from plugins.utils import FileManager
from plugins.s3 import S3Helper
from pydantic import BaseModel, validator
from datetime import date
from io import StringIO

import pandas as pd
import boto3

s3_client = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"),
                    aws_secret_access_key=Variable.get("aws_secret_access_key"))

pop_columns = ['날짜', '자치구', '생활인구수']
housing_columns = ['계약일', '자치구', '가격', '면적', '층수', '건축년도', '건물용도']
road_columns = []
noise_columns = []
air_columns = []
welfare_columns = []


class Cleaning(AirflowPlugin):

    def pop_cleaning(insert_date):

        try:
            response = s3_client.get_object(Bucket="de-team5-s3-01", Key='raw_data/seoul_pop/{}.csv'.format(insert_date))
            csv_content = response['Body'].read().decode('utf-8')

            df = pd.read_csv(StringIO(csv_content), header=None)
            df.columns = pop_columns

            # 자치구 목록에서 서울시, 결측치를 제거 (기본키로 사용될 자치구는 NULL 값을 가져서는 안 됨)
            df = df[df['자치구'] != '서울시']
            df = df.dropna(subset=['자치구'])

            # 데이터 타입에 맞춰서
            class Model(BaseModel):
                날짜: date
                자치구: str
                생활인구수: float

                @classmethod
                #데이터프레임 행마다
                def from_dataframe_row(cls, row):
                    return cls(**row)
    
                @validator('날짜', pre=True, always=True)
                #날짜 데이터 타입 처리
                def parse_date(cls, value):
                    if isinstance(value, int):
                        value = str(value)

                    return date(int(value[:4]), int(value[4:6]), int(value[6:]))
            
                @validator('자치구')
                def handle_string_column(cls, value):
                    # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
                    return 'NULL' if pd.isna(value) else value

                @validator('생활인구수')
                def handle_numeric_columns(cls, value):
                    # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
                    return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value

            models = [Model.from_dataframe_row(row) for _, row in df.iterrows()]
            result_df = pd.DataFrame([model.dict() for model in models])

            return [result_df, insert_date]
        
        except:
            pass
    

    def housing_cleaning(insert_date):

        try:
            response = s3_client.get_object(Bucket="de-team5-s3-01", Key='raw_data/seoul_housing/{}.csv'.format(insert_date))
            csv_content = response['Body'].read().decode('utf-8')

            df = pd.read_csv(StringIO(csv_content), header=None)
            df.columns = housing_columns

            # 자치구 목록에서 결측치를 제거 (기본키로 사용될 자치구는 NULL 값을 가져서는 안 됨)
            df = df.dropna(subset=['자치구'])

            # 데이터 타입에 맞춰서
            class Model(BaseModel):
                계약일: date
                자치구: str
                가격: int
                면적: float
                층수: int
                건축년도: int
                건물용도: str

                @classmethod
                def from_dataframe_row(cls, row):
                    # 건축년도 결측치 중 null이라는 문자열로 처리 된 것들도 있고 비워져 있는 것도 있어서 에러가 나는 듯 한데 아래 코드 추가하니까 괜찮아졌습니다.
                    row['건축년도'] = row['건축년도'] if pd.notna(row['건축년도']) else 0
                    return cls(**row)
    
                @validator('계약일', pre=True, always=True)
                def parse_date(cls, value):
                    # 정수로 받은 날짜를 datetime.date 객체로 변환
                    if isinstance(value, int):
                        value = str(value)
                    return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    
                @validator('자치구', '건물용도')
                def handle_string_column(cls, value):
                    # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
                    return 'NULL' if pd.isna(value) else value

                @validator('가격', '면적', '층수', '건축년도')
                def handle_numeric_columns(cls, value):
                    # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
                    return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value

            models = [Model.from_dataframe_row(row) for _, row in df.iterrows()]
            result_df = pd.DataFrame([model.dict() for model in models])

            return [result_df, insert_date]
        
        except:
            pass
    
    def pop_upload(result):
        result_df = result[0]
        date = result[1]

        file_path = '/works/test/test1/' # 깃허브 업로드 할 때는 그에 맞게 경로 변경
        file_name = f'{date}.csv'
        local_file = file_path+file_name

        s3_key = 'cleaned_data/seoul_pop/' + str(file_name)

        result_df.to_csv(local_file, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload('aws_default', 'de-team5-s3-01', s3_key, local_file, True)
        FileManager.remove(local_file)
    
    def housing_upload(result):
        result_df = result[0]
        date = result[1]

        file_path = '/works/test/test2' # 깃허브 업로드 할 때는 그에 맞게 경로 변경
        file_name = f'{date}.csv'
        local_file = file_path+file_name

        s3_key = 'cleaned_data/seoul_housing/' + str(file_name)

        result_df.to_csv(local_file, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload('aws_default', 'de-team5-s3-01', s3_key, local_file, True)
        FileManager.remove(local_file)
    
    