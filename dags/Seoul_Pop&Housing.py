from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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


def life_people_load(**context):
    record = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_transform")

    try:
        data = record[0]
        date = record[1]

        file_path = '/works/Seoul_pop/'
        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False)

        return [local_file, file_name]
    
    except:
        return None

def housing_load(**context):
    record = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_transform")

    try:
        data = record[0]
        date = record[1]

        file_path = '/works/Seoul_housing/'
        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False)

        return [local_file, file_name]
    
    except:
        return None

def life_people_upload(**context):
    file = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_load")
    
    try:
        local_file = file[0]
        file_name = file[1]

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename = local_file, key = 'raw_data/Seoul_POP/{}'.format(file_name), bucket_name = 'de-team5-s3-01', replace = True)

        os.remove(local_file)
    
    except:
        pass

def housing_upload(**context):
    file = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_load")
    
    try:
        local_file = file[0]
        file_name = file[1]

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename = local_file, key = 'raw_data/Seoul_housing/{}'.format(file_name), bucket_name = 'de-team5-s3-01', replace = True)

        os.remove(local_file)
    
    except:
        pass

dag = DAG(
    dag_id = 'Seoul_pop_and_Seoul_housing',
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
        'url':  Variable.get('pop_url')
    },
    dag = dag)

housing_extract = PythonOperator(
    task_id = 'housing_extract',
    python_callable = housing_extract,
    provide_context=True,
    params = {
        'url':  Variable.get('housing_url')
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

life_people_load = PythonOperator(
    task_id = 'life_people_load',
    python_callable = life_people_load,
    provide_context=True,
    params = { 
    },  
    dag = dag)

housing_load = PythonOperator(
    task_id = 'housing_load',
    python_callable = housing_load,
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

life_people_extract >> life_people_transform >> life_people_load >> life_people_upload
housing_extract >> housing_transform >> housing_load >> housing_upload
