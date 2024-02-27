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
        execution_date = context['execution_date'].date() - timedelta(days=4)
        data = Cleaning.read_csv_to_df('pop', execution_date, filter.column_indexes['pop'])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'pop'), '자치구' if '자치구' in filter.columns['pop'] else '권역')
        result_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(result_data, 'pop')

        print(data)

        file_path = 'temp/seoul_pop/'
        file_name = '{}.csv'.format(execution_date)
        local = file_path+file_name

        s3_key = 'cleaned_data/seoul_pop/' + file_name

        result_data.to_csv(local, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local, True)

        FileManager.remove(local)
    
    except:
        pass

with DAG(
    dag_id = 'POP_Cleaning',
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
