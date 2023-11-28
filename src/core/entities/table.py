class Table:
    def __init__(self, database: str, schema: str, name: str):
        self._name = name
        self._database = database
        self._schema = schema

    def __repr__(self):
        return f"DB: {self.database}; Schema: {self.schema}; Table: {self.name}"

    @property
    def name(self):
        return self._name

    @property
    def database(self) -> str:
        return self._database

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def fqdm(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"
