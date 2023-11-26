import requests
import json
from abc import ABC
from requests.auth import HTTPBasicAuth

# UNUSED 
class AirflowAPI(ABC):
    def __init__(self, host, username, password):
        self.host = host
        self.basic = HTTPBasicAuth(username, password)
    
    def get_dags(self):
        url = f"{self.host}/dags"
        response = requests.get(url, auth=self.basic)
        return json.loads(response.content)['dags']
    
    def get_tasks(self, dag_id):
        url = f"{self.host}/dags/{dag_id}/tasks"
        response = requests.get(url, auth=self.basic)
        return json.loads(response.content)['tasks']
    
    def get_dag_runs(self, dag_id):
        url = f"{self.host}/dags/{dag_id}/dagRuns"
        response = requests.get(url, auth=self.basic)
        return json.loads(response.content)['dag_runs']

    def get_latest_dag_runs(self, dag_id):
        url = f"{self.host}/dags/{dag_id}/dagRuns?order_by=-end_date&limit=1"
        response = requests.get(url, auth=self.basic,)
        return json.loads(response.content)['dag_runs'][0]

    def get_task_instances(self, dag_id, dag_run_id):
        url = f"{self.host}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        response = requests.get(url, auth=self.basic)
        return json.loads(response.content)['task_instances']
        