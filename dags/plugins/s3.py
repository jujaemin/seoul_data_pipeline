from airflow.plugins_manager import AirflowPlugin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Helper(AirflowPlugin):

    def upload(s3_conn_id, bucket_name, key, filename, replace):
        """
        Upload files to S3
        """
        s3_hook = S3Hook(s3_conn_id)

        s3_hook.load_file(
            filename=filename,
            bucket_name=bucket_name,
            key=key,
            replace=replace
        )