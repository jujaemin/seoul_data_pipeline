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

    # Generate tasks with AthenaOperator
    ctas_tasks = [athena.ctas(GLUE_DATABASE_AD_HOC, t) for t in ad_hoc_tables]

    # Task Flow
    start_task >> ctas_tasks >> end_task