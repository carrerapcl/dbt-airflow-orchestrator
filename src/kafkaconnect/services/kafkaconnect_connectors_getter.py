from requests import Session
from src.core.services.utils import load_config, get_config_value, get_default_config_path
from src.kafkaconnect.gateways.kafka_connect_cli import KafkaConnectCLI
from src.kafkaconnect.entities.kafkaconnect_connector import KafkaConnectConnector

# TODO: solve for multiple scopes
class KafkaConnectConnectorsGetter:
    def __init__(self, config_path=get_default_config_path()):
        self.config = load_config(config_path)
        self._session = self.get_session()
        # Fetch and enable the Kafka to Warehouse mapping if configured
        self.kafka_warehouse_enabled = get_config_value(
            self.config, 'kafka', 'warehouse_mapping', {}
        ).get('enabled', False)
        # Get the topic-to-table mapping key, defaulting to 'snowflake.topic2table.map'
        self.topic2table_map_key = get_config_value(
            self.config, 'kafka', 'warehouse_mapping', {}
        ).get('topic2table_map_key')

    def get_session(self):
        _session = Session()
        _session.verify = False
        return _session

    def main(self):
        urls = get_config_value(self.config, 'kafka', 'urls')

        kafka_connectors = []
        for url in urls:
            kafkaconnect_cli = KafkaConnectCLI(url, self._session)

            connections_names = kafkaconnect_cli.get_connectors()
            for connector_name in connections_names:
                connector_tables = []
                connector_config = kafkaconnect_cli.get_connector_config(connector_name)
                if self.kafka_warehouse_enabled and self.topic2table_map_key in connector_config:
                    topics_tables = connector_config.get(self.topic2table_map_key)
                    for topic_table in topics_tables.split(','):
                        _, table_name = topic_table.split(':')
                        connector_tables.append(table_name.upper())
                    kafka_connectors.append(KafkaConnectConnector(connector_name, connector_tables))

        return kafka_connectors

# If this file is ran directly, print Kafka Connect connectors and their configurations:
if __name__ == '__main__':
    acg = KafkaConnectConnectorsGetter()
    kccs = acg.main()
    for kcc in kccs:
        print(kcc)
