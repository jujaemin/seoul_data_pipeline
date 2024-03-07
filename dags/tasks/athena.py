from airflow.models import Variable
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

S3_BUCKET = Variable.get('bucket_name')

class AthenaTool():
    
    def ctas(output_database:str, table_name:str):
        return AthenaOperator(
            task_id=f'create_table_{table_name}',
            query=f'{table_name}.sql',
            params={
                'output_table': table_name,
                'output_database': output_database
            },
            database=output_database,
            output_location=f's3://{S3_BUCKET}/{output_database}/{table_name}',
        )

    def ctas_num_area(output_database:str, table_name:str, category:str):
        return AthenaOperator(
            task_id=f'create_table_{table_name}',
            query='tmpl_num_area.sql',
            params={
                'output_table': table_name,
                'output_database': output_database,
                'col_category': category
            },
            database=output_database,
            output_location=f's3://{S3_BUCKET}/{output_database}/{table_name}',
        )