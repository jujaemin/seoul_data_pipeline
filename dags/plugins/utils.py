from airflow.plugins_manager import AirflowPlugin

import requests
import os


class RequestTool(AirflowPlugin):
    @classmethod
    def api_request(api_url: str, verify: bool, params: dict):
        try:
            response = requests.get(api_url, verify=verify, params=params)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as e:
            # HTTP error (e.g. 404, 500 etc)
            raise e

        except requests.exceptions.RequestException as e:
            # For other errors
            raise e


class FileManager(AirflowPlugin):
    @staticmethod
    def getcwd():
        return os.getcwd()
    
    @staticmethod
    def remove(filename: str):
        os.remove(filename)
        
    @staticmethod
    def mkdir(path: str):
        if not os.path.exists(path):
            os.makedirs(path)
