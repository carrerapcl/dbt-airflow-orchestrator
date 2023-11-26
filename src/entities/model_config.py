class ModelConfig:

    def __init__(self, name, owner, tags, schedule, sla, description):
        self._name = name
        self._owner = owner
        self._tags = tags
        self._schedule = schedule
        self._sla = sla
        self._description = description

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def tags(self):
        return self._tags

    @property
    def schedule(self):
        return self._schedule

    @property
    def sla(self):
        return self._sla    

    @property 
    def description(self):
        return self._description
