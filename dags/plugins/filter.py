from pydantic import BaseModel, validator
from datetime import date
import pandas as pd

columns = {'pop': ['날짜', '자치구', '생활인구수'], 'housing': ['계약일', '자치구', '가격', '면적', '층수', '건축년도', '건물용도'],
           'road': [], 'noise': [], 'air': [], 'welfare': []}


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

