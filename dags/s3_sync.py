from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

S3_BUCKET = Variable.get('bucket_name')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': timedelta(minutes=5),
    'catchup': False 
}

@dag(
    default_args=default_args,
)
def s3_sync():
    s3_sync_task = BashOperator(
        task_id='s3_sync_task',
        bash_command=f'aws s3 sync "s3://{S3_BUCKET}/airflow/dags" "." --delete'
    )

    s3_sync_task