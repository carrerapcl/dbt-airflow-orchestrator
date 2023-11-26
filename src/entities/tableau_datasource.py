from src.entities.tableau_table import TableauTable


class TableauDatasource:

    def __init__(self, datasource_id, luid, name, tables: list[TableauTable]):
        self.type: str = 'tableau_datasource'
        self._id: str = datasource_id
        self._luid: str = luid
        self._name: str = name
        self._tables: list[TableauTable] = tables

    def __repr__(self):
        return f"{self.type}: {self.name} ID: {self._id}"

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def tables(self) -> list[TableauTable]:
        return self._tables
