from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from plugins.utils import FileManager
from plugins.s3 import S3Helper
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.timetables.trigger import CronTriggerTimetable

import datetime
import logging



@task
def cleaning(**context):
    try:
        execution_date = context['execution_date'].date()
        data = Cleaning.read_csv_to_df('noise', execution_date, filter.column_indexes['noise'])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'noise'), '자치구' if '자치구' in filter.columns['noise'] else '권역')
        result_data = Cleaning.unify_null(data)

        result_data = Cleaning.filter(result_data, 'noise')

        save_path = 'temp/seoul_noise/cleaning/'
        file_name = f'{execution_date}.parquet'
        path = save_path+file_name

        FileManager.mkdir(save_path)

        result_data.to_parquet(path, index=False)

        s3_key = 'cleaned_data/seoul_noise/' + file_name

        S3Helper.upload(aws_conn_id, bucket_name, s3_key, path, True)

        FileManager.remove(path)
    
    except Exception as e:
        logging.info(e)
        pass

with DAG(
    dag_id = 'Noise_Cleaning',
    start_date = datetime.datetime(2024,1,1),
    schedule = CronTriggerTimetable("0 5 * * *", timezone="UTC"),
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
        external_dag_id='etl_seoul_noise',
        external_task_id='load',
        timeout=5*60,
        mode='reschedule'
)

    cleaning_task = cleaning()

    sensor >> cleaning_task
