from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from utils import FileManager
from datetime import datetime

from tasks.athena import AthenaTool as athena

GLUE_DATABASE_AD_HOC = Variable.get('glue_database_ad_hoc')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    # This DAG runs only by triggering
    'schedule_interval': None,
}

@dag(
    default_args=default_args,
    # Set the searchpath as current work directory to scan sql files
    template_searchpath=[FileManager.getcwd() + 'sqls/']
)
def ad_hoc_ELT():
    exec_date = '{{ ds }}'
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
    ad_hoc_tables = [
        'air_pm25_index',
        'cultural_facilities_per_pop',
        'green_area_subtotal',
        'housing_price_sqm_stats',
        'noise_scale',
        'pop_area',
        'pop_by_age',
        'pop_per_month'
    ]

    ctas_tasks = [] 
    # Generate tasks with AthenaOperator
    for table in ad_hoc_tables:
        drop_task = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, table)
        ctas_task = athena.ctas(GLUE_DATABASE_AD_HOC, table, exec_date)
        ctas_tasks.append(drop_task >> ctas_task)

    # Task Flow
    start_task >> ctas_tasks >> end_task