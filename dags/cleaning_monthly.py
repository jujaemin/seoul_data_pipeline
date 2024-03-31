from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from utils import FileManager
from s3 import S3Helper

import datetime


@task
def welfare_cleaning(**context):
    #try:
        execution_date = context['execution_date'].date()
        data = Cleaning.read_csv_to_df('welfare', execution_date, filter.column_indexes['welfare'])
        renamed_data = Cleaning.rename_cols(data, 'welfare')
        checked_data = Cleaning.check_pk_validation(renamed_data, 'shi_gu')
        unified_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(unified_data, 'welfare')

        save_path = 'temp/seoul_welfare/cleaning/'
        file_name = f'{execution_date}.parquet'
        path = save_path+file_name

        FileManager.mkdir(save_path)

        result_data.to_parquet(path, index=False)

        s3_key = 'cleaned_data/seoul_welfare/' + file_name

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, path, True)

        FileManager.remove(path)

    
    #except:
    #    pass

with DAG(
    dag_id = 'cleaning_monthly',
    start_date = datetime.datetime(2024,1,1),
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    aws_conn_id='aws_conn_id'
    bucket_name = 'de-team5-s3-01'


    welfare_cleaning()
