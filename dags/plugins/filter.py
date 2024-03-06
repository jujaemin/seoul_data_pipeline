from pydantic import BaseModel, validator
from datetime import date, datetime
from typing import Union

import pandas as pd


columns = {'air': ['measured_date', 'region', 'gu', 'pm10', 'pm25', 'o3', 'no2', 'co', 'so2'],
           'pop': ['standard_date', 'gu', 'total_living_pop'], 'housing': ['contracted_date', 'gu', 'house_name', 'price', 'house_area', 'floor', 'built_year', 'house_type'],
           'road': ['standard_date', 'division_code', 'division_name', 'time_group_name', 'avg_speed', 'day_code', 'day_group_code', 'time_code', 'time_explain'], 
           'welfare': ['name', 'code', 'category', 'detailed_category', 'shi_gu_category', 'manager_name', 'shi_gu_code', 'shi_gu', 'address',
                       'capacity', 'current_head_count', 'phone_number', 'zip_code'], 'noise': ['region_type', 'gu', 'dong','avg_noise', 'registered_date']}

en_to_ko = {'Jongno-gu': '종로구', 'Jung-gu': '중구', 'Yongsan-gu': '용산구', 'Seongdong-gu': '성동구', 'Gwangjin-gu': '광진구', 'Dongdaemun-gu': '동대문구',
            'Jungnang-gu': '중랑구', 'Seongbuk-gu': '성북구', 'Gangbuk-gu': '강북구', 'Dobong-gu': '도봉구', 'Nowon-gu': '노원구', 'Eunpyeong-gu': '은평구',
            'Seodaemun-gu': '서대문구', 'Mapo-gu': '마포구', 'Yangcheon-gu': '양천구', 'Gangseo-gu': '강서구', 'Guro-gu': '구로구', 'Geumcheon-gu': '금천구',
            'Yeongdeungpo-gu': '영등포구', 'Dongjak-gu': '동작구', 'Gwanak-gu': '관악구', 'Seocho-gu': '서초구', 'Gangnam-gu': '강남구', 'Songpa-gu': '송파구',
            'Gangdong-gu': '강동구', 'Seoul_Grand_Park': '서울대공원'}

column_indexes = {'air': [0,1,2,3,4,5,6,7,8], 'pop': [0,1,2], 'housing': [0,1,2,3,4,5,6,7], 'road': [0,1,2,3,4,5,6,7,8], 'welfare': [0,1,2,3,4,5,6,7,8,9,10,11,12], 'noise': [4,5,6,32,64]}

class air(BaseModel):
    measured_date: date
    region: Union[str, None]
    gu: str
    pm10: Union[float, None]
    pm25: Union[float, None]
    o3: Union[float, None]
    no2: Union[float, None]
    co: Union[float, None]
    so2: Union[float, None]

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('measured_date', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
        
    @validator('region')
    def handle_string_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('gu')
    def handle_string2_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('pm10')
    def handle_pm10_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['pm10'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('pm25')
    def handle_pm25_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['pm25'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('o3')
    def handle_o3_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['o3'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('no2')
    def handle_no2_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['no2'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('co')
    def handle_co_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['co'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('so2')
    def handle_so2_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['so2'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value



class pop(BaseModel):
    standard_date: date
    gu: str
    total_living_pop: Union[float, None]

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('standard_date', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
        
    @validator('gu')
    def handle_sgg_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    @validator('total_living_pop')
    def handle_pop_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['total_living_pop'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value


class housing(BaseModel):
    contracted_date: date
    gu: str
    house_name: Union[str, None]
    price: Union[int, None]
    house_area: Union[float, None]
    floor: Union[int, None]
    built_year: Union[float, None]
    house_type: Union[str, None]

    @classmethod
    def from_dataframe_row(cls, row):
        #row['건축년도'] = row['건축년도'] if pd.notna(row['건축년도']) else 0
        return cls(**row)
    
    @validator('contracted_date', pre=True, always=True)
    def parse_date(cls, value):
        # 정수로 받은 날짜를 datetime.date 객체로 변환
        if isinstance(value, int):
            value = str(value)
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    
    @validator('gu')
    def handle_sgg_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    
    @validator('house_name')
    def handle_bdnm_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('house_type')
    def handle_bduse_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    @validator('price')
    def handle_price_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['price'].mean() if pd.isna(value) else value
    
    @validator('house_area')
    def handle_area_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['house_area'].mean() if pd.isna(value) else value
    
    @validator('floor')
    def handle_height_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['floor'].mean() if pd.isna(value) else value
    
    @validator('built_year')
    def handle_year_columns(cls, value):
        return 0 if pd.isna(value) else value


class road(BaseModel):
    standard_date: date
    division_code: Union[int, None]
    division_name: str
    time_group_name: Union[str, None]
    avg_speed: Union[float, None]
    day_code: Union[int, None]
    day_group_code: Union[int, None]
    time_code: Union[int, None]
    time_explain: Union[str, None]

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('standard_date', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    
    @validator('division_name')
    def handle_sgga_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

        
    @validator('time_group_name')
    def handle_ts_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('time_explain')
    def handle_ex_ts_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    @validator('division_code')
    def handle_lac_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('avg_speed')
    def handle_speed_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['avg_speed'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('day_code')
    def handle_wkd_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('day_group_code')
    def handle_wkdg_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('time_code')
    def handle_tc_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 0으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    

class welfare(BaseModel):
    name: Union[str, None]
    code: Union[str, None]
    category: Union[str, None]
    detailed_category: Union[str, None]
    shi_gu_category: Union[str, None]
    manager_name: Union[str, None]
    shi_gu_code: Union[int, None]
    shi_gu: str
    address: Union[str, None]
    capacity: Union[float, None]
    current_head_count: Union[float, None]
    phone_number: Union[str, None]
    zip_code: Union[str, None]

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
        
    @validator('name')
    def handle_fanm_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('code')
    def handle_facd_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('category')
    def handle_fcct_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('detailed_category')
    def handle_fcct_detail_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('shi_gu_category')
    def handle_sggp_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('manager_name')
    def handle_manm_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('address')
    def handle_faad_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('phone_number')
    def handle_pn_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('zip_code')
    def handle_mn_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    @validator('shi_gu_code')
    def handle_sggc_columns(cls, value):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return 0 if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('shi_gu')
    def handle_sgg_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    
    @validator('capacity')
    def handle_full_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['capacity'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value
    
    @validator('current_head_count')
    def handle_now_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        return values['current_head_count'].mean() if pd.isna(value) or str(value).strip().lower() in ('null', '') else value


class noise(BaseModel):
    region_type: Union[str, None]
    gu: str
    dong: Union[str, None]
    avg_noise: Union[float, None]
    registered_date: datetime

    @classmethod
    #데이터프레임 행마다
    def from_dataframe_row(cls, row):
        return cls(**row)
    
    @validator('registered_date', pre=True, always=True)
    #날짜 데이터 타입 처리
    def parse_date(cls, value):
        if isinstance(value, int):
            value = str(value)

        return datetime(int(value[:4]), int(value[5:7]), int(value[8:10]), int(value[11:13]), int(value[14:16]), int(value[17:19]))
    
    @validator('gu')
    def translate(cls, value):
        return en_to_ko[value]
    
    @validator('avg_noise')
    def handle_noise_columns(cls, value, values):
        # 실수나 정수형으로 된 컬럼의 결측치를 평균으로 처리
        noise_values = values.get('avg_noise', None)
        return noise_values.mean() if noise_values is not None and not noise_values.isnull().all() else value
        
    @validator('region_type')
    def handle_area_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
    
    @validator('gu')
    def handle_sgg_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value

    
    @validator('dong')
    def handle_hjd_column(cls, value):
        # 문자열로 된 컬럼의 결측치를 'NULL'로 처리
        return 'NULL' if value == 'nan' else value
