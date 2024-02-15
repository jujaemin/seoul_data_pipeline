from airflow.plugins_manager import AirflowPlugin

import requests
import os


class RequestTool(AirflowPlugin):
    name = "request_tool"

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
    name = 'file_manager'

    def getcwd():
        return os.getcwd()

    def remove(filename: str):
        os.remove(filename)

    def mkdir(path):
        if not os.path.exists(path):
            os.makedirs(path)
