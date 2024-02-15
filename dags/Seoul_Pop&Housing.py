from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import requests
import pandas as pd
import datetime
import os


def life_people_extract(**context):
    link = context['params']['url']
    execution_date = context["execution_date"]

    date = execution_date.date().strftime('%Y-%m-%d')
    url = link + date.replace('-', '')

    return [url, date]

def housing_extract(**context):
    link = context['params']['url']
    execution_date = context["execution_date"]

    date = execution_date.date().strftime('%Y-%m-%d')
    url = link + date.replace('-', '')

    return [url, date]

def life_people_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_extract")

    res = requests.get(response[0])
    data = res.json()
    date = response[1]

    try:
        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        return [life_people_data, date]
    
    except:
        return None


def housing_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_extract")

    res = requests.get(response[0])
    data = res.json()
    date = response[1]

    try:

        df = pd.DataFrame(data['tbLnOpendataRtmsV']['row'])

        housing_data = df[['DEAL_YMD', 'SGG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]

        return [housing_data, date]
        
    except:
        return None


def life_people_upload(**context):
    record = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_transform")
    print(record)
    #s3_hook = S3Hook(aws_conn_id='aws_default')
    file_path = '/opt/airflow/work_file'
    print(file_path)

    try:
        data = record[0]
        date = record[1]

        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        pd.DataFrame(data).to_csv(local_file, index = False)

        #s3_hook.load_file(file, key = 'key', bucket_name = 'de-team5-s3-01', replace = True)

        #os.remove(file)
    
    except:
        pass

def housing_upload(**context):
    record = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_transform")
    print(record)
    #s3_hook = S3Hook(aws_conn_id='aws_default')
    file_path = '/opt/airflow/work_file'

    try:
        data = record[0]
        date = record[1]

        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        pd.DataFrame(data).to_csv(local_file, index = False)

        #s3_hook.load_file(local_file, key = 'key', bucket_name = 'de-team5-s3-01', replace = True)

        #os.remove(file)
    
    except:
        pass

dag = DAG(
    dag_id = 'DE-Project_test',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

life_people_extract = PythonOperator(
    task_id = 'life_people_extract',
    python_callable = life_people_extract,
    provide_context=True,
    params = {
        'url':  'http://openapi.seoul.go.kr:8088/api_key/json/SPOP_DAILYSUM_JACHI/1/1000/'
    },
    dag = dag)

housing_extract = PythonOperator(
    task_id = 'housing_extract',
    python_callable = housing_extract,
    provide_context=True,
    params = {
        'url':  'http://openapi.seoul.go.kr:8088/api_key/json/tbLnOpendataRtmsV/1/1000/ / / / / / / / / /'
    },
    dag = dag)

life_people_transform = PythonOperator(
    task_id = 'life_people_transform',
    python_callable = life_people_transform,
    provide_context=True,
    params = { 
    },  
    dag = dag)

housing_transform = PythonOperator(
    task_id = 'housing_transform',
    python_callable = housing_transform,
    provide_context=True,
    params = { 
    },  
    dag = dag)

life_people_upload = PythonOperator(
    task_id = 'life_people_upload',
    python_callable = life_people_upload,
    provide_context=True,
    params = { 
    },  
    dag = dag)

housing_upload = PythonOperator(
    task_id = 'housing_upload',
    python_callable = housing_upload,
    provide_context=True,
    params = { 
    },  
    dag = dag)

life_people_extract >> life_people_transform >> life_people_upload
housing_extract >> housing_transform >> housing_upload
