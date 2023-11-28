from typing import Union
from requests import Session, Response
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AirbyteCLI:
    def __init__(self, url: str, session: Session):
        self._url: str = url
        self._session: Session = session
        self._encoding = 'utf-8'

    def _get_response_data(self, response: Response, caller: str, json_key: str = None) -> Union[list, dict]:
        if response.status_code != 200:
            # print(f"Bad {caller} request: {response.status_code}: {response.content.decode(response.encoding)}")
            response.raise_for_status()
            return []
        if json_key:
            return response.json().get(json_key)
        return response.json()

    def get_workspaces(self) -> list[str]:
        workspaces = []
        url = f"{self._url}/api/v1/workspaces/list"
        response_data = self._get_response_data(self._session.post(url), 'get_workspaces', 'workspaces')
        for workspace in response_data:
            workspaces.append(workspace.get("workspaceId"))
        return workspaces

    def get_destination_definition(self, destination_id) -> dict:
        url = f"{self._url}/api/v1/destinations/get"
        data = {"destinationId": destination_id}
        return self._get_response_data(self._session.post(url, json=data), 'get_destination_definition')

    def get_connections(self, connection_names: list[str] = None) -> list:
        connections: list = []
        workspaces = self.get_workspaces()
        for workspace_id in workspaces:
            url = f"{self._url}/api/v1/connections/list"
            data = {"workspaceId": workspace_id}
            response_data = self._get_response_data(self._session.post(url, json=data), 'get_connections', 'connections')
            for connection in response_data:
                destination_config = self.get_destination_definition(connection.get('destinationId'))
                connection.update({"destinationName": destination_config.get("destinationName")})
                connections.append(connection)

        return connections

    def trigger_connection_sync_job(self, connection_id: str) -> dict:
        url = f"{self._url}/api/v1/connections/sync"
        data = {"connectionId": connection_id}
        return self._get_response_data(self._session.post(url, json=data), 'trigger_connection_sync_job',
                                       json_key="job")

    def get_connection_sync_job_state(self, job_id: str) -> dict:
        url = f"{self._url}/api/v1/jobs/get"
        data = {"id": job_id}
        return self._get_response_data(self._session.post(url, json=data), 'trigger_connection_sync_job')
