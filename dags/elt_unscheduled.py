from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from utils import FileManager
from datetime import datetime

AWS_CONNECTION = 'aws_conn_id'
S3_BUCKET = Variable.get('s3_bucket')
S3_AD_HOC_KEY = Variable.get('s3_ad_hoc_key')
S3_ANALYTICS_KEY = Variable.get('s3_analytics_key')
S3_REGION = Variable.get('s3_region')
ATHENA_DATABASE_ANALYTICS = Variable.get('athena_database_analytics')
ATHENA_DATABASE_AD_HOC = Variable.get('athena_database_ad_hoc')
ATHENA_DATABASE_RAW_DATA = Variable.get('athena_database_raw_data')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    
    # Default parameters for AthenaOperator
    'database':ATHENA_DATABASE_AD_HOC,
    'aws_conn_id':AWS_CONNECTION,
    'region_name':S3_REGION,
    'output_location':f's3://{S3_BUCKET}/{S3_AD_HOC_KEY}',
    'sleep_time':30,
    'max_tries':None,
}

with DAG(
    dag_id='create_tables_unscheduled',
    catchup=False,
    default_args=default_args,

    # Unscheduled
    schedule_interval=None,

    # Set the searchpath as current work directory to scan sql files
    template_searchpath=[FileManager.getcwd()]
) as dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')

    create_table_medical_num = AthenaOperator(
        task_id='create_table_medical_num',
        query='sqls/tmpl_ctas_seperated_num_area.sql',

        # Parameters will be rendered in jinja templated sql file.
        params={
            'output_table_name': 'medical_num',
            'source_database': ATHENA_DATABASE_RAW_DATA,
            'source_table': 'seoul_medical',
            'col_category': '병원수',
            'extra': '' 
        }
    )

    create_table_medical_bed = AthenaOperator(
        task_id='create_table_medical_bed',
        query='sqls/tmpl_ctas_seperated_num_area.sql',
        params={
            'output_table_name': 'medical_bed',
            'source_database': ATHENA_DATABASE_RAW_DATA,
            'source_table': 'seoul_medical',
            'col_category': '병상수',
            'extra': '' 
        }
    )

    create_table_sports_num = AthenaOperator(
        task_id='create_table_sports_num',
        query='sqls/tmpl_ctas_seperated_num_area.sql',
        params={
            'output_table_name': 'sports_num',
            'source_database': ATHENA_DATABASE_RAW_DATA,
            'source_table': 'seoul_sports',
            'col_category': '개소 (개소)',
            'extra': '' 
        },
    )

    create_table_sports_area = AthenaOperator(
        task_id='create_table_sports_area',
        query='sqls/tmpl_ctas_seperated_num_area.sql',
        params={
            'output_table_name': 'sports_area',
            'source_database': ATHENA_DATABASE_RAW_DATA,
            'source_table': 'seoul_sports',
            'col_category': '면적 (㎡)',
            'extra': '' 
        },
    )

    create_table_congestion = AthenaOperator(
        task_id='create_table_congestion',
        query='sqls/create_congestion.sql',
        params={
            'output_table_name': 'congestion_by_area',
            'source_database': ATHENA_DATABASE_RAW_DATA
        }
    )

    create_table_welfare = AthenaOperator(
        task_id='create_table_welfare_2022',
        query='sqls/create_welfare.sql',
        params={
            'output_table_name': 'welfare_2022'
        },

        # Override parameters to change database to select
        database=ATHENA_DATABASE_ANALYTICS,
        output_location=f's3://{S3_BUCKET}/{S3_ANALYTICS_KEY}',
    )

    start_task >> [
        create_table_congestion,
        [
            create_table_medical_num,
            create_table_medical_bed,
            create_table_sports_num,
            create_table_sports_area,

        ] >> [
            create_table_welfare,
        ]
    ] >> end_task
