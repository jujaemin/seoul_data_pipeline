from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from utils import FileManager
from datetime import datetime

from tasks.athena import AthenaTool

AWS_CONNECTION = 'aws_conn_id'
S3_REGION = Variable.get('s3_region')

ATHENA_DATABASE_RAW_DATA = Variable.get('athena_database_raw_data')
ATHENA_DATABASE_AD_HOC = Variable.get('athena_database_ad_hoc')
ATHENA_DATABASE_ANALYTICS = Variable.get('athena_database_analytics')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': None,
    'catchup': False 
}

@dag(
    default_args=default_args,

    # Default parameters for AthenaOperator
    aws_conn_id=AWS_CONNECTION,
    region_name=S3_REGION,
    sleep_time=30,
    max_tries=None,

    # Set the searchpath as current work directory to scan sql files
    template_searchpath=[FileManager.getcwd() + 'sqls/']
)
def unscheduled_ELT():
    athena = AthenaTool()

    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
    park_ratio = athena.ctas(ATHENA_DATABASE_AD_HOC,'park_ratio')

    target_condition = {'medical_num': '병원수', 
                        'medical_bed': '병상수', 
                        'sports_num': '개소 (개소)', 
                        'sports_area':'면적 (㎡)'}
    
    ad_hocs = [park_ratio]
    
    # Make CTAS queries with conditions
    for table_name, category in target_condition.items():
        generated_operator = athena.ctas_num_area(ATHENA_DATABASE_AD_HOC, table_name, category)
        ad_hocs.append(generated_operator)


    congestion_by_division = athena.ctas(ATHENA_DATABASE_ANALYTICS,'congestion_by_division')
    congestion = athena.ctas(ATHENA_DATABASE_ANALYTICS,'congestion')
    welfare_index = athena.ctas(ATHENA_DATABASE_ANALYTICS,'welfare_index')

    # Task Flow
    start_task >> ad_hocs >> [congestion_by_division >> congestion, welfare_index] >> end_task