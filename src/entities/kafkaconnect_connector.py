
class KafkaConnectConnector:
    def __init__(self, name, tables):
        self.type = "kafkaconnect_connector"
        self._name = name
        self._tables = tables

    def __repr__(self):
        return f"KafkaConnector Name: {self._name}; tables: {'; '.join(self._tables)}"

    @property
    def name(self):
        return self._name

    @property
    def tables(self):
        return self._tables

    def table_exist(self, table):
        if table in self._tables:
            return True
        return False

    @property
    def sanitized_name(self):
        name = self._name
        name = name.replace('-', '_')
        return name.lower()
