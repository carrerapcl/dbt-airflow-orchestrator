from src.dbt.entities.dbt_test import DBTTest


class DBTModel:
    def __init__(self, name, sources, refs, tags, schedule, priority, owner, schema, database, sla):
        self.type = "dbt_model"
        self._name = name
        self._sources = sources
        self._refs = refs
        self._tags = tags
        self._schedule = schedule
        self._priority = priority
        self._owner = owner
        self._schema = schema
        self._database = database
        self._sla = sla

    def __repr__(self):
        return f"DBTModel: {self._name}"

    @property
    def fqdn(self):
        return f"{self.database}.{self.schema}.{self.name}".upper()

    @property
    def name(self):
        return self._name

    @property
    def sources(self):
        return self._sources

    @property
    def refs(self):
        return self._refs

    @property
    def tags(self):
        return self._tags

    @property
    def schedule(self):
        return self._schedule

    @property
    def priority(self):
        return self._priority

    @property
    def owner(self):
        return self._owner

    @property
    def schema(self):
        return self._schema

    @property
    def database(self):
        return self._database

    @property
    def sla(self):
        return self._sla
