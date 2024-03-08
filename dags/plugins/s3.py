from airflow.plugins_manager import AirflowPlugin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Helper(AirflowPlugin):
    # Upload files to S3
    @classmethod
    def upload(cls, s3_conn_id: str, bucket_name: str, key: str, filename: str, replace: bool):
        s3_hook = S3Hook(s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            bucket_name=bucket_name,
            key=key,
            replace=replace
        )
    
    # Delete key
    @classmethod
    def delete(cls, s3_conn_id: str,bucket_name: str, key: str):
        s3_hook = S3Hook(s3_conn_id)
        if s3_hook.check_for_key(bucket_name, key):
            s3_hook.delete_objects(bucket_name, key)
    
    def upload_string(s3_conn_id, string_data, key, bucket_name, replace=False, encrypt=False, acl_policy=None):
        """
        Upload string to S3
        """
        s3_hook = S3Hook(s3_conn_id)

        s3_hook.load_string(
            string_data=string_data,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
            encrypt=encrypt,
            acl_policy=acl_policy
        )
