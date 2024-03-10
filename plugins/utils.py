from airflow.plugins_manager import AirflowPlugin

import requests
import os


class RequestTool(AirflowPlugin):
    name = 'RequestTool'
    @staticmethod
    def api_request(api_url: str, verify: bool, params: dict):
        try:
            #추후 api_url 을 Variable 형태로 변환해서 사용하는걸로 변경 필요
            if api_url == 'http://openAPI.seoul.go.kr:8088':
                for value in params.values():
                    api_url += f'/{value}'
                response = requests.get(api_url, verify=verify)
                response.raise_for_status()
            elif api_url == 'https://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TopisIccStTimesRoadDivTrfLivingStats/1.0':
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
    name = 'FileManager'
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
