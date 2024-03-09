from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from utils import FileManager
from s3 import S3Helper
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import logging
import datetime


@task
def cleaning(**context):
    try:
        execution_date = context['execution_date'].date() - timedelta(days=14)
        data = Cleaning.read_csv_to_df('noise', execution_date, filter.column_indexes['noise'])
        data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'noise'), 'gu')
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
    dag_id = 'cleaning_once_in_a_day',
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

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='ad_hoc_ELT',
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success', 'failed', 'upstream_failed']
    )

    cleaning() >> trigger_dag_task