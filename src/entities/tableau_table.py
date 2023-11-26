from src.entities.table import Table

class TableauTable(Table):
    def __init__(self, database: str, schema: str, name: str, tableau_id, tableau_luid):
        super().__init__(database, schema, name)
        self._tableau_id = tableau_id
        self._tableau_luid = tableau_luid

    @property
    def tableau_id(self):
        return self._tableau_id

    @property
    def tableau_luid(self):
        return self._tableau_luid
