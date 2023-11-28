class DBTTest:
    def __init__(self, name, refs, tags, schema, database):
        self.type = "dbt_model"
        self._name = name
        self._refs = refs
        self._tags = tags
        self._schema = schema
        self._database = database

    def __repr__(self):
        return f"DBTTest: {self._name}"

    @property
    def fqdn(self):
        return f"{self.database}.{self.schema}.{self.name}".upper()

    @property
    def name(self):
        return self._name

    @property
    def refs(self):
        return self._refs

    @property
    def tags(self):
        return self._tags

    @property
    def schema(self):
        return self._schema

    @property
    def database(self):
        return self._database
