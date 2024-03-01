from plugins.cleaning import Cleaning
from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from plugins import filter
from plugins.utils import FileManager
from plugins.s3 import S3Helper

import datetime


@task
def cleaning():
    dates = []
    start_date = datetime.datetime(2024,1,1).date()
    end_date = datetime.datetime.today().date()
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    for date in dates:
        try:
            execution_date = date
            data = Cleaning.read_csv_to_df('housing', execution_date, filter.column_indexes['housing'])
            data = Cleaning.check_pk_validation(Cleaning.rename_cols(data, 'housing'), '자치구' if '자치구' in filter.columns['housing'] else '권역')
            result_data = Cleaning.unify_null(data)

            result_data = Cleaning.filter(result_data, 'housing')
            result_data['건물명'] = result_data.apply(lambda row: 'NULL' if row['건물용도'] == '단독다가구' else row['건물명'], axis=1)


            save_path = 'temp/seoul_air/housing/'
            file_name = f'{execution_date}.parquet'
            path = save_path+file_name

            FileManager.mkdir(save_path)

            result_data.to_parquet(path, index=False)

            s3_key = 'cleaned_data/seoul_housing/' + file_name

            S3Helper.upload(aws_conn_id, bucket_name, s3_key, path, True)

            FileManager.remove(path)
    
        except:
            pass

with DAG(
    dag_id = 'Housing_Cleaning',
    start_date = datetime.datetime(2024,1,1),
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    aws_conn_id='aws_default'
    bucket_name = 'de-team5-s3-01'

    cleaning()

