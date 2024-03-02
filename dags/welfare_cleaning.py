from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from plugins.utils import FileManager
from plugins.s3 import S3Helper
from airflow.sensors.external_task import ExternalTaskSensor

import datetime


@task
def cleaning(**context):
    try:
        execution_date = context['execution_date'].date()
        data = Cleaning.read_csv_to_df('welfare', execution_date, filter.column_indexes['welfare'])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'welfare'), '자치구' if '자치구' in filter.columns['noise'] else '권역')
        result_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(result_data, 'welfare')


        file_path = '/works/'
        file_name = '{}.csv'.format(execution_date)
        local = file_path+file_name

        s3_key = 'cleaned_data/seoul_welfare/' + file_name

        result_data.to_csv(local, header = False, index = False, encoding='utf-8-sig')

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, local, True)

        FileManager.remove(local)
    
    except Exception as e:
        pass

with DAG(
    dag_id = 'Welfare_Cleaning',
    start_date = datetime.datetime(2024,1,1),
    schedule = '@monthly',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'

    sensor = ExternalTaskSensor(
        task_id='externaltasksensor',
        external_dag_id='etl_seoul_welfare',
        external_task_id='load',
        timeout=5*60,
        mode='reschedule'
)

    cleaning_task = cleaning()

    sensor >> cleaning_task
