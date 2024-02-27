from pydantic import BaseModel, validator
from datetime import date, datetime
import pandas as pd

columns = {'air': ['날짜', '권역', '자치구', '미세먼지', '초미세먼지', '오존', '이산화질소농도', '일산화탄소농도', '아황산가스농도'],
           'pop': ['날짜', '자치구', '생활인구수'], 'housing': ['계약일', '자치구', '가격', '면적', '층수', '건축년도', '건물용도'],
           'road': ['날짜', '생활권역구분코드', '권역', '첨두시구분', '평균속도', '요일코드', '요일그룹코드', '시간코드', '시간대설명'], 
           'noise': ['측정시간', '자치구', '소음평균']}

en_to_ko = {'Jongno-gu': '종로구', 'Jung-gu': '중구', 'Yongsan-gu': '용산구', 'Seongdong-gu': '성동구', 'Gwangjin-gu': '광진구', 'Dongdaemun-gu': '동대문구',
            'Jungnang-gu': '중랑구', 'Seongbuk-gu': '성북구', 'Gangbuk-gu': '강북구', 'Dobong-gu': '도봉구', 'Nowon-gu': '노원구', 'Eunpyeong-gu': '은평구',
            'Seodaemun-gu': '서대문구', 'Mapo-gu': '마포구', 'Yangcheon-gu': '양천구', 'Gangseo-gu': '강서구', 'Guro-gu': '구로구', 'Geumcheon-gu': '금천구',
            'Yeongdeungpo-gu': '영등포구', 'Dongjak-gu': '동작구', 'Gwanak-gu': '관악구', 'Seocho-gu': '서초구', 'Gangnam-gu': '강남구', 'Songpa-gu': '송파구',
            'Gangdong-gu': '강동구'}

column_indexes = {'air': [0,1,2,3,4,5,6,7,8], 'pop': [0,1,2], 'housing': [0,1,2,3,4,5,6], 'road': [0,1,2,3,4,5,6,7,8], 'noise': [2,4,31]}

class air(BaseModel):
    날짜: date
    권역: str
    자치구: str
    미세먼지: int
    초미세먼지: int
    오존: float
    이산화질소농도: float
    일산화탄소농도: float
    아황산가스농도: float

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
        
    @validator('권역','자치구')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value

    @validator('미세먼지', '초미세먼지', '오존', '이산화질소농도', '일산화탄소농도', '아황산가스농도')
    def handle_numeric_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value


class pop(BaseModel):
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


class housing(BaseModel):
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
        return 0 if pd.isna(value) else value


class road(BaseModel):
    날짜: date
    생활권역구분코드: int
    권역: str
    첨두시구분: str
    평균속도: float
    요일코드: int
    요일그룹코드: int
    시간코드: int
    시간대설명: str

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
        
    @validator('권역', '첨두시구분', '시간대설명')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value

    @validator('생활권역구분코드', '평균속도', '요일코드', '요일그룹코드', '시간코드')
    def handle_numeric_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    

class noise(BaseModel):
    측정시간: datetime
    자치구: str
    소음평균: float

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('측정시간', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return datetime(int(value[:4]), int(value[5:7]), int(value[8:10]), int(value[11:13]), int(value[14:16]), int(value[17:]))
    
    @validator('자치구')
    def translate(cls, value):
        return en_to_ko[value]
        
    @validator('자치구')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value

    @validator('소음평균')
    def handle_numeric_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
