from pydantic import BaseModel, validator
from datetime import date, datetime
from typing import Union
import pandas as pd


columns = {'air': ['날짜', '권역', '자치구', '미세먼지', '초미세먼지', '오존', '이산화질소농도', '일산화탄소농도', '아황산가스농도'],
           'pop': ['날짜', '자치구', '생활인구수'], 'housing': ['계약일', '자치구', '건물명', '가격', '면적', '층수', '건축년도', '건물용도'],
           'road': ['날짜', '생활권역구분코드', '권역', '첨두시구분', '평균속도', '요일코드', '요일그룹코드', '시간코드', '시간대설명'], 
           'welfare': ['시설명', '시설코드', '시설종류명', '시설종류상세명', '자치구구분', '시설장명', '시군구코드', '자치구', '시설주소',
                       '정원', '현인원', '전화번호', '우편번호'], 'noise': ['지역', '자치구', '행정동','소음평균', '날짜']}

en_to_ko = {'Jongno-gu': '종로구', 'Jung-gu': '중구', 'Yongsan-gu': '용산구', 'Seongdong-gu': '성동구', 'Gwangjin-gu': '광진구', 'Dongdaemun-gu': '동대문구',
            'Jungnang-gu': '중랑구', 'Seongbuk-gu': '성북구', 'Gangbuk-gu': '강북구', 'Dobong-gu': '도봉구', 'Nowon-gu': '노원구', 'Eunpyeong-gu': '은평구',
            'Seodaemun-gu': '서대문구', 'Mapo-gu': '마포구', 'Yangcheon-gu': '양천구', 'Gangseo-gu': '강서구', 'Guro-gu': '구로구', 'Geumcheon-gu': '금천구',
            'Yeongdeungpo-gu': '영등포구', 'Dongjak-gu': '동작구', 'Gwanak-gu': '관악구', 'Seocho-gu': '서초구', 'Gangnam-gu': '강남구', 'Songpa-gu': '송파구',
            'Gangdong-gu': '강동구', 'Seoul_Grand_Park': '서울대공원'}

column_indexes = {'air': [0,1,2,3,4,5,6,7,8], 'pop': [0,1,2], 'housing': [0,1,2,3,4,5,6,7], 'road': [0,1,2,3,4,5,6,7,8], 'welfare': [0,1,2,3,4,5,6,7,8,9,10,11,12], 'noise': [4,5,6,32,64]}

class air(BaseModel):
    날짜: date
    권역: Union[str, None]
    자치구: str
    미세먼지: Union[float, None]
    초미세먼지: Union[float, None]
    오존: Union[float, None]
    이산화질소농도: Union[float, None]
    일산화탄소농도: Union[float, None]
    아황산가스농도: Union[float, None]

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

    @validator('미세먼지')
    def handle_pm10_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['미세먼지'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('초미세먼지')
    def handle_pm25_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['초미세먼지'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('오존')
    def handle_o3_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['오존'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('이산화질소농도')
    def handle_no2_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['이산화질소농도'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('일산화탄소농도')
    def handle_co_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['인산화탄소농도'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('아황산가스농도')
    def handle_so2_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['아황산가스농도'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value



class pop(BaseModel):
    날짜: date
    자치구: str
    생활인구수: Union[float, None]

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
    def handle_pop_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['생활인구수'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value


class housing(BaseModel):
    계약일: date
    자치구: str
    건물명: Union[str, None]
    가격: Union[int, None]
    면적: Union[float, None]
    층수: Union[int, None]
    건축년도: Union[int, None]
    건물용도: Union[str, None]

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
    
    @validator('자치구', '건물명', '건물용도')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value

    @validator('가격')
    def handle_price_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['가격'].mean() if pd.isna(value) else value
    
    @validator('면적')
    def handle_area_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['면적'].mean() if pd.isna(value) else value
    
    @validator('층수')
    def handle_height_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['층수'].mean() if pd.isna(value) else value
    
    @validator('건축년도')
    def handle_year_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['건축년도'].mean() if pd.isna(value) else value


class road(BaseModel):
    날짜: date
    생활권역구분코드: Union[int, None]
    권역: str
    첨두시구분: Union[str, None]
    평균속도: Union[float, None]
    요일코드: Union[int, None]
    요일그룹코드: Union[int, None]
    시간코드: Union[int, None]
    시간대설명: Union[str, None]

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

    @validator('생활권역구분코드')
    def handle_lac_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['생환권역구분코드'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('평균속도')
    def handle_speed_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['평균속도'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('요일코드')
    def handle_wkd_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['요일코드'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('요일그룹코드')
    def handle_wkdg_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['요일그룹코드'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('시간코드')
    def handle_tc_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['시간코드'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    

class welfare(BaseModel):
    시설명: Union[str, None]
    시설코드: Union[str, None]
    시설종류명: Union[str, None]
    시설종류상세명: Union[str, None]
    자치구구분: Union[str, None]
    시설장명: Union[str, None]
    시군구코드: Union[int, None]
    자치구: str
    시설주소: Union[str, None]
    정원: Union[float, None]
    현인원: Union[float, None]
    전화번호: Union[str, None]
    우편번호: Union[str, None]

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
        
    @validator('시설명','시설코드','시설종류명','시설종류상세명','자치구구분','시설장명','자치구','시설주소','전화번호', '우편번호')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value

    @validator('시군구코드')
    def handle_sggc_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['시군구코드'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('정원')
    def handle_full_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['정원'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('현인원')
    def handle_now_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['현인원'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value


class noise(BaseModel):
    지역: Union[str, None]
    자치구: str
    행정동: Union[str, None]
    소음평균: Union[float, None]
    날짜: datetime

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('날짜', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return datetime(int(value[:4]), int(value[5:7]), int(value[8:10]), int(value[11:13]), int(value[14:16]), int(value[17:19]))
    
    @validator('자치구')
    def translate(cls, value):
        return en_to_ko[value]
    
    @validator('소음평균')
    def handle_noise_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        noise_values = values.get('소음평균', None)
        return noise_values.mean() if noise_values is not None and not noise_values.isnull().all() else value
        
    @validator('지역', '자치구', '행정동')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if pd.isna(value) else value
