from airflow.plugins_manager import AirflowPlugin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Helper(AirflowPlugin):
    #Upload files to S3
    @classmethod
    def upload(s3_conn_id: str, bucket_name: str, key: str, filename: str, replace: bool):
        s3_hook = S3Hook(s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            bucket_name=bucket_name,
            key=key,
            replace=replace
        )