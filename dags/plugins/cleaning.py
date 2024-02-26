from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from io import StringIO
from plugins import filter

import pandas as pd
import boto3

s3_client = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"),
                    aws_secret_access_key=Variable.get("aws_secret_access_key"))

class Cleaning(AirflowPlugin):

    def read_csv_to_df(subject: str, file, column_indexes: list):
        response = s3_client.get_object(Bucket="de-team5-s3-01", Key=f'raw_data/seoul_{subject}/{file}.csv')
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content), header=None, usecols=column_indexes)
            
        return df

    
    def rename_cols(df: pd.DataFrame, subject: str):
        column_names = filter.columns[subject]
        df.columns = column_names

        return df
    
    def check_pk_validation(df: pd.DataFrame, pk: str):
        df = df.dropna(subset=[pk])

        return df
    
    def unify_null(df: pd.DataFrame):
        df.replace('null', pd.NA, inplace=True)
        df.replace('-', pd.NA, inplace=True)

        return df
    
    def filter(df: pd.DataFrame, subject):
        sub = getattr(filter, subject)
        models = [sub.from_dataframe_row(row) for _, row in df.iterrows()]
        result_df = pd.DataFrame([model.dict() for model in models])

        return result_df
