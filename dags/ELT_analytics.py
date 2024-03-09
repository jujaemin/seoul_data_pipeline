from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from utils import FileManager
from datetime import datetime

from tasks.athena import AthenaTool as athena

GLUE_DATABASE_AD_HOC = Variable.get('glue_database_ad_hoc')
GLUE_DATABASE_ANALYTICS = Variable.get('glue_database_analytics')

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
def analytics_ELT():
    exec_date = '{{ ds }}'
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
    # Iterable dict for seperate num adn bed
    target_condition = {'medical_num': '병원수', 
                        'medical_bed': '병상수', 
                        'sports_num': '개소 (개소)', 
                        'sports_area':'면적 (㎡)'}

    ad_hoc_tasks = []
    analytics_tasks = []

    drop_park_ratio = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, 'park_ratio')
    ctas_park_ratio = athena.ctas(GLUE_DATABASE_AD_HOC, 'park_ratio', exec_date)

    drop_congestion_by_division = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, 'congestion_by_division')
    ctas_congestion_by_division = athena.ctas_num_area(GLUE_DATABASE_AD_HOC,'congestion_by_division', exec_date)
    drop_congestion = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, 'congestion')
    ctas_congestion = athena.ctas(GLUE_DATABASE_ANALYTICS,'congestion', exec_date)
    drop_welfare_index = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, 'welfare_index')
    ctas_welfare_index = athena.ctas_num_area(GLUE_DATABASE_AD_HOC,'welfare_index', exec_date)
    
    for table, category in target_condition.items():
        drop = athena.drop_if_exists(GLUE_DATABASE_AD_HOC, table)
        ctas = athena.ctas_num_area(GLUE_DATABASE_AD_HOC, table, category, exec_date)
        ad_hoc_tasks.append(drop >> ctas)

    ad_hoc_tasks.append(drop_park_ratio >> ctas_park_ratio)
    analytics_tasks.append([
        drop_congestion_by_division >> ctas_congestion_by_division >> drop_congestion >> ctas_congestion,
        drop_welfare_index >> ctas_welfare_index
    ])

    # Task Flow
    start_task >> ad_hoc_tasks >> analytics_tasks >> end_task