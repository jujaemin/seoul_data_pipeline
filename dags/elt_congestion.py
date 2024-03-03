from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from plugins.utils import FileManager
from datetime import datetime

AWS_CONNECTION = 'aws_conn_id'
S3_BUCKET = Variable.get('s3_bucket')
S3_AD_HOC_KEY = Variable.get('s3_ad_hoc_key')
S3_ANALYTICS_KEY = Variable.get('s3_analytics.key')
S3_REGION = Variable.get('s3_region')
ATHENA_DATABASE_AD_HOC = Variable.get('athena_database_ad_hoc')
ATHENA_DATABASE_RAW_DATA = Variable.get('athena_database_raw_data')  # de-team5-glue-database-01

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='create_ad_hoc_table',
    catchup=False,
    default_args=default_args,

    # 반복하지 않습니다.
    schedule_interval=None,

    # sql템플릿을 가져오기 위해 검색경로를 지정합니다.
    template_searchpath=[FileManager.getcwd()]
) as dag:

    ###
    # ...
    # create adhoc table 파이프라인이 작성되어야 함
    # ...
    ###

    create_table_congestion = AthenaOperator(
        task_id='create_table_congestion',
        query='sql/create_congestion.sql',

        # jinja template이 포함된 sql 파일에 전달할 파라미터입니다.
        params={
            'output_table_name': 'congestion_by_area',
            'source_database': ATHENA_DATABASE_RAW_DATA
        },

        database=ATHENA_DATABASE_AD_HOC,
        aws_conn_id=AWS_CONNECTION,
        region_name=S3_REGION,
        output_location=f's3://{S3_BUCKET}/{S3_AD_HOC_KEY}',
        sleep_time=30,
        max_tries=None,
    )

    create_table_congestion
