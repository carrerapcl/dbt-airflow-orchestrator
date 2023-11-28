class DBTSource:
    def __init__(self, table_name, schema, database, source_name, source_group_name):
        self._type = "dbt_source"
        self._table = table_name
        self._database = database
        self._schema = schema
        self._source_name = source_name
        self._source_group_name = source_group_name

    @property
    def table(self):
        return self._table


class DBTSourceGroup:
    def __init__(self, name):
        self._name = name
        self.dbt_sources: list[DBTSource] = []
