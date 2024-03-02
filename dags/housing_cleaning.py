from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from plugins.utils import FileManager
from plugins.s3 import S3Helper

import datetime


@task
def cleaning(**context):
    try:
        execution_date = context['execution_date'].date()
        data = Cleaning.read_csv_to_df('housing', execution_date, filter.column_indexes['housing'])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'housing'), '자치구' if '자치구' in filter.columns['housing'] else '권역')
        result_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(result_data, 'housing')

        print(data)

        file_path = 'temp/seoul_housing/'
        file_name = '{}.csv'.format(execution_date)

        FileManager.mkdir(file_path)
        
        path = file_path+file_name

        s3_key = 'cleaned_data/seoul_housing/' + file_name

        result_data.to_csv(path, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, path, True)

        FileManager.remove(path)
    
    except:
        pass

with DAG(
    dag_id = 'Housing_Cleaning',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@daily',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'

    cleaning()