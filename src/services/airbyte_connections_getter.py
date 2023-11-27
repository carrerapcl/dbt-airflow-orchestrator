from src.services.utils import load_config, get_config_value, get_default_config_path
from requests.auth import HTTPBasicAuth
from requests import Session
from src.gateways.airbyte_cli import AirbyteCLI
from src.entities.airbyte_connection import AirbyteConnection

class AirbyteConnectionGetter:
    def __init__(self, config_path=get_default_config_path()):
        self.config = load_config(config_path)
        self._prefix = '_AIRBYTE_RAW_'
        _session = self.get_session()
        url = get_config_value(self.config, 'airbyte', 'url')
        self.airbyte_cli = AirbyteCLI(url, _session)

    def get_session(self):
        _session = Session()
        _session.verify = False
        username = get_config_value(self.config, 'airbyte', 'username')
        password = get_config_value(self.config, 'airbyte', 'password')
        _session.auth = HTTPBasicAuth(username, password)
        return _session

    def main(self):
        airbyte_connections = []
        connections = self.airbyte_cli.get_connections()

        warehouse_destinations = get_config_value(self.config, 'airbyte', 'warehouse_destinations')
        for c in connections:
            if c.get('destinationName') not in warehouse_destinations:
                continue
            connection_tables = []
            connection_id = c.get('connectionId')
            connection_name = c.get('name')
            connection_prefix = c.get('prefix')

            # TODO: Need to make table names configurable (including self._prefix)
            for stream in c.get('syncCatalog').get('streams'):
                stream_name = stream.get('stream').get('name')
                table_name = f"{self._prefix}{connection_prefix}{stream_name}".upper()
                connection_tables.append(table_name.upper())
            airbyte_connections.append(AirbyteConnection(connection_id, connection_name, connection_tables))
        return airbyte_connections
