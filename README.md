# 나에게 가장 잘 맞는 자치구는❓
![screen-capture](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/a42e01cf-a6ae-4874-9be3-9454277ba047)

## 목차
프로젝트 개요
- 프로젝트 목표
- 사용 기술
- 데이터 아키텍처
- 인프라 아키텍처
- 역할 및 분담

프로젝트 진행
- 협업 방식 (Notion, Jira, Github, Slack, Gather)
- 요구 사항 도출: 랭킹 지표 정의

회고
- 잘한 점
- 아쉬웠던 점
- 배운 점

## 프로젝트 개요
### 프로젝트 목표
- **서울시 자치구**의 다양한 분야에 대한 종합적인 **통찰력**을 제공합니다.
- 데이터 시각화를 위한 **사용자 친화적인 대시보드**를 개발합니다.
- 데이터 수집 및 분석 프로세스를 **완전 자동화**합니다.

### 사용 기술
- **Data Analysis** :  SQL, **AWS Athena**
- **Data Catalog** : **AWS Glue**
- **Data Pre-Processing** : Python, Pandas
- **Data Lake** : **AWS S3**
- **Orchestration** : **Apache Airflow** (2.5.1)
- **Server** : AWS EC2, AWS RDS(postgres)
- **BI tool** : **Tableau**
- **Co-op** : Jira, GitHub, Slack

### 데이터 아키텍처
![Untitled](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/59e60161-ab50-4179-864f-e14ba93bc6e2)

### 인프라 아키텍쳐
![Untitled (1)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/e8fd1f4c-75b4-41e5-9faf-8339da0498cc)

### 역할 및 분담
|이름|역할 및 담당 업무|기여도|
|----|---------------|--------|
|공통 작업|	ETL 코드 및 Airflow Dag 작성, ELT 분석 테이블 SQL 코드 작성, Tableau 시트 작성, 아키텍처 구상||	
|김동연|	PM 및 팀장, 프로젝트 기획, ERD 설계, Tableau 환경 세팅 및 대시보드 작성, 회의록 작성,  협업 툴(Notion, Slack) 관리, Airflow 환경 세팅, API 키 관리, 발표 및 최종 보고서 작성	|20%|
|조성재|	AWS 매니저, AWS 환경 구축(VPC) 및 비용 관리, Tableau 및 대시보드 작성 및 디자인|	20%|
|주재민|	ERD 설계, Code Refactoring, Data Cleaning, Airflow Dag 스케줄링, 파일 적재 형식 개선(csv→parquet)|	20%|
|최봉승|	프로젝트 기획, 협업 툴(Jira, Github) 관리, CI/CD 자동화(Github Actions), 공통 모듈(플러그인) 작성, Airflow Dag 스케줄링|	20%|
|황진민|	ERD 설계, Code Refactoring, Data Cleaning, Raw Data Schema 정의 및 Glue Catalog 작성, 파일 적재 형식 개선(csv→parquet)|	20%|

## 프로젝트 진행
### 협업 방식 (Notion, Jira, Github, Slack, Gather)
- Notion: 협업 규칙, 소스 정리 및 회의 기록
![Untitled (2)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/dd8fa69d-55e2-4ed8-a64e-5fdf66bd2c29)
![Untitled (3)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/e235178e-7b71-411d-b56c-acc6d7f81774)
![Untitled (4)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/218939fc-25fc-4cd0-8adf-ec50e03e7d3c)
- Jira: 작업 현황 트래킹
![Untitled (5)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/3ad8e5f8-9171-4a41-9a26-0b169697eaa1)
- Github: 코드 형상 관리 및 배포
![Untitled (6)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/6a961435-55fb-437f-bfc5-0bf9e5f2663c)
- Slack, Gather: 실시간 이슈 공유 및 소통
![캡처](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/d614fb53-23ff-4bfc-88e1-5dba719ea503)
![Untitled (7)](https://github.com/jujaemin/seoul_data_pipeline/assets/147119777/e6c9027a-9c25-4e1b-bbec-33375846cf57)

### 요구 사항 도출: 랭킹 지표 정의
#### 환경적 요인
- 대기질 지수: 공기가 얼마나 좋은가?
- 녹색 환경 지역 지수: 녹지 및 공원 면적이 얼마나 넓은가? 산책할 데 많은가?
- 소음 지수: 얼마나 조용한 동네인가?

#### 경제 및 주택 요인
- 면적 대비 주택 가격 지수: 동일 자본 대비 구매할 수 있는 가용 면적?
- 고용 기회 지수: 지역 내 취업자수 및 고용률이 얼마나 높은가?

#### 인구 통계
- 연령 지수: 해당 지역의 평균 연령대는 어떠한가? (초등생, 노인, 직장인 등)
- 인구 밀도 지수: 동일 면적에 몇 명의 주민이 사는가? 얼마나 인구 밀집도가 높은가?

#### 안전과 보안
- 치안 지수: 범죄 발생 빈도와 치안 시설(경찰서) 개수?
- 복지 지수: 의료 시설 개수, 사회 복지 시설 개수가 얼마나 많은가?

#### 교통 및 접근성
- 유동 인구 지수: 하루에 자치구 간 이동하는 인구 수가 얼마나 되는가?
- 상권 지수: 지역 내 사업체 수가 얼마나 되는가?
- 교통 접근성 지수: 도로 평균 속도, 지하철 역 개수, 버스 노선 개수가 얼마나 많은가?

#### 문화 생활
- 문화 지수: 인구당 문화공간 수가 얼마나 많은가?

## 회고
### 잘한 점
- 일 단위/주 단위 목표를 설정하며 **애자일한 업무 방식**을 잘 소화했습니다.
- 추상적이었던 목표를 구체화하고 요구 사항을 정의하여 **명확한 공동의 목표**를 제시하였습니다.
- 각자의 업무 분야에 자유와 권한을 보장하고 책임을 부여함으로써 의지를 고취하고 **비동기적인 업무 처리 과정**을 구축했습니다.
- 막히거나 모르는 게 있을 때는 부끄러워하지 않고 **적극적으로 질문하고 의견을 개진할 수 있는 분위기**를 조성하였습니다.
- 특정 분야의 지식이 부족한 팀원이 있을 때는 페어프로그래밍, 화면 공유를 통한 간단한 강의를 통해 서로를 도왔습니다.
- 다양한 협업 툴(Jira, Notion, Slack)을 활용하여 비대면 협업에 있어 각자가 **무슨 일을 하는지 서로 알 수 있게** 하였습니다.
- 기술적 어려움이 발생했을 때는 중간 발표 모임에 참여하는 등 타 팀과의 기술 공유 및 협업에도 적극적이었습니다.

### 아쉬웠던 점
- 특정 업무에 기술적인 어려움이 발생했을 때 인원 재분배를 기민하게 하지 못해 목표 데드라인이 지체 되는 일이 발생했습니다.
- 시간적인 제약으로 목표했던 몇 가지 요구 사항(상관 관계 분석, 랭킹화)들을 완료하지 못했습니다.
- 낮은 EC2 사양과 같이 자원 사용에 제약이 있어서 아쉬웠습니다.

### 배운 점
- 다양한 **AWS Cloud 서비스** (S3, EC2, Glue, Athena 등) 활용 능력을 기를 수 있었습니다.
- Airflow를 활용해 python 코드 기반의 dag파일을 작성하고 스케줄링하여 **데이터 수집, 전처리, 분석까지 자동화하는 능력**을 기를 수 있었습니다.
- 데이터 ETL, ELT, 티어링, 클리닝, 아키텍처를 구상해보는 등 전반적인 **데이터 처리 프로세스에 대한 이해**를 높일 수 있었습니다.
- 다양한 분석 테이블을 작성해보면서 **SQL 활용 능력**을 기를 수 있었습니다.
- 다양한 협업 툴 (Jira, Github, Slack)들을 활용해보면서 **효율적으로 협업 하는 방식**에 대해서 배웠습니다.
- Github 브랜치 관리 규칙을 정의하고 Github Actions를 활용해보면서 **CI/CD를 경험**했습니다.
