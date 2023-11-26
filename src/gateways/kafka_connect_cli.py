from requests import Session
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KafkaConnectCLI:

    def __init__(self,  url: str, session: Session):
        self._url: str = url
        self._session: Session = session
        self._encoding = 'utf-8'

    def get_connectors(self, connector_names: list[str] = None) -> list:
        connector_names = [] if connector_names is None else connector_names
        response = self._session.get(f"{self._url}/connectors")
        connectors: list = []
        if response.status_code == 200:
            for connector in response.json():
                if connector in connector_names or len(connector_names) == 0:
                    connectors.append(connector)

            return connectors
        return connectors

    def get_connector_config(self, connector) -> dict:
        response = self._session.get(f"{self._url}/connectors/{connector}/config")
        if response.status_code == 200:
            return response.json()
        return {}

    def get_connector_state(self, connector) -> dict:
        response = self._session.get(f"{self._url}/connectors/{connector}/status")
        if response.status_code == 200:
            return response.json()
        return {}
    
    def trigger_connector_restart(self, connector):
        response = self._session.post(f"{self._url}/connectors/{connector}/restart")
        if response.status_code == 200:
            return True
        return False
