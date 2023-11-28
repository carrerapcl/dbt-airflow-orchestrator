class AirbyteConnection:
    def __init__(self, connection_id, name, tables):
        self.type = "airbyte_connection"
        self._id = connection_id
        self._name = name
        self._tables = tables

    def __repr__(self):
        return f"AirbyteConnection ID:{self._id}, Name: {self._name}; tables: {'; '.join(self._tables)}"

    def table_exist(self, table):
        if table in self._tables:
            return True
        return False

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def sanitized_name(self):
        name = self._name
        name = name.replace(' <> ', '_')
        name = name.replace('-', '_')
        name = name.replace(' ', '_')
        name = name.replace("'", '')
        name = name.replace('"', '')
        return name.lower()

    @property
    def tables(self):
        return self._tables
