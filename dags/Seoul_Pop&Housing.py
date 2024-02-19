from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta

import requests
import pandas as pd
import datetime
import os
import logging



def life_people_extract(**context):
    link = context['params']['url']
    execution_date = context["execution_date"] - timedelta(days=5)

    date = execution_date.date().strftime('%Y-%m-%d')
    url = link + date.replace('-', '')

    logging.info(f'Success : life_people_extract ({date})')

    return [url, date]

def housing_extract(**context):
    link = context['params']['url']
    execution_date = context["execution_date"]
    result = []

    start_date = datetime.datetime(2024,1,1).date()
    end_date = execution_date.date()
    current_date = start_date

    while current_date <= end_date:
        date = current_date.strftime("%Y-%m-%d").replace('-','')
        url = link + date

        result.append([url, str(current_date)])
        current_date += timedelta(days=1)

    logging.info(f'Success : housing_extract ({date})')

    return result

def life_people_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_extract")

    res = requests.get(response[0])
    data = res.json()
    date = response[1]

    try:
        df = pd.DataFrame(data['SPOP_DAILYSUM_JACHI']['row'])

        life_people_data = df[['STDR_DE_ID', 'SIGNGU_NM', 'TOT_LVPOP_CO']]

        logging.info(f'Success : life_people_transform ({date})')

        return [life_people_data, date]
    
    except:

        logging.error(f'no data found : {date}')

        return None


def housing_transform(**context):
    responses = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_extract")
    result = []

    for response in responses:
        res = requests.get(response[0])
        data = res.json()
        date = response[1]

        try:

            df = pd.DataFrame(data['tbLnOpendataRtmsV']['row'])

            housing_data = df[['DEAL_YMD', 'SGG_NM', 'OBJ_AMT', 'BLDG_AREA', 'FLOOR', 'BUILD_YEAR', 'HOUSE_TYPE']]
            result.append([housing_data, date])
        
        except:

            pass

    logging.info('Success : housing_transform')
        
    return result


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

        logging.info(f'Success : life_people_load ({date})')

        return [local_file, file_name]
    
    except TypeError:
        logging.error('no data found')
        return None

def housing_upload(**context):
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="housing_transform")
    s3_hook = S3Hook(aws_conn_id='aws_default')

    for record in records:
        data = record[0]
        date = record[1]

        file_path = '/works/Seoul_housing/'
        file_name = '{}.csv'.format(date)

        os.makedirs(file_path, exist_ok=True)
        local_file = os.path.join(file_path, file_name)

        data.to_csv(local_file, header = False, index = False)

        s3_hook.load_file(filename = local_file, key = 'raw_data/Seoul_housing/{}'.format(file_name), bucket_name = 'de-team5-s3-01', replace = True)
        os.remove(local_file)

        logging.info(f'Success : housing_load ({date})')
    

def life_people_upload(**context):
    file = context["task_instance"].xcom_pull(key="return_value", task_ids="life_people_load")
    
    try:
        local_file = file[0]
        file_name = file[1]

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename = local_file, key = 'raw_data/Seoul_POP/{}'.format(file_name), bucket_name = 'de-team5-s3-01', replace = True)

        os.remove(local_file)

        logging.info(f'Success : life_people_upload ({file_name})')
    
    except:
        logging.error('no data found')
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
housing_extract >> housing_transform >> housing_upload
