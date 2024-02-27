from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from plugins.utils import FileManager
from plugins.s3 import S3Helper

import datetime

@task
def cleaning(subject: str, **context):
    try:
        execution_date = context['execution_date'].date()
        data = Cleaning.read_csv_to_df(subject, execution_date, filter.column_indexes[subject])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, subject), '자치구' if '자치구' in filter.columns[subject] else '권역')
        result_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(result_data, subject)

        return [result_data, execution_date]
    
    except:
        pass

@task
def upload(subject):
    try:
        result_data = subject[0]
        execution_date = subject[1]

        file_path = f'/works/test/{subject}/'
        file_name = '{}.csv'.format(execution_date)
        local = file_path+file_name

        s3_key = f'cleaned_data/seoul_{subject}/' + file_name

        result_data.to_csv(local, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local, True)

        FileManager.remove(local)
    
    except:
        pass

with DAG(
    dag_id = 'test_cleaning10',
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

    cleaning_task = cleaning('road')
    upload(cleaning_task)
    #cleaning('pop')
    #cleaning('housing')
    #cleaning('air')
    #cleaning('noise')