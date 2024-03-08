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
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
    park_ratio = athena.ctas(GLUE_DATABASE_AD_HOC,'park_ratio')

    target_condition = {'medical_num': '병원수', 
                        'medical_bed': '병상수', 
                        'sports_num': '개소 (개소)', 
                        'sports_area':'면적 (㎡)'}
    
    ad_hocs = [park_ratio]
    
    # Make CTAS queries with conditions
    for table_name, category in target_condition.items():
        generated_operator = athena.ctas_num_area(GLUE_DATABASE_AD_HOC, table_name, category)
        ad_hocs.append(generated_operator)


    congestion_by_division = athena.ctas(GLUE_DATABASE_ANALYTICS,'congestion_by_division')
    congestion = athena.ctas(GLUE_DATABASE_ANALYTICS,'congestion')
    welfare_index = athena.ctas(GLUE_DATABASE_ANALYTICS,'welfare_index')

    # Task Flow
    start_task >> ad_hocs >> [congestion_by_division >> congestion, welfare_index] >> end_task