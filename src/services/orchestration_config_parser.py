import yaml
from typing import List

from src.entities.model_config import ModelConfig

class OrchestrationConfigParser:
    ORCHESTRATION_FILE_PATH = "resources/orchestration.yaml"

    def _load_orchestration_file(self, file_path=None):
        if file_path is None:
            file_path = self.ORCHESTRATION_FILE_PATH

        # consider returning error as value "go style"
        with open(file_path, "r") as f:
            return yaml.safe_load(f)

    def read_models(self, file_path=None) -> List[ModelConfig]:
        data = self._load_orchestration_file(file_path)

        models = [] 
        models_from_file = data.get('models')
        if models_from_file is None:
            return []
        for model in models_from_file:

            model_name = model.get('name')
            model_schedule = model.get('schedule')
            model_sla = model.get('sla', 24)
            model_owner = model.get('owner')
            model_tags = model.get('tags')
            model_description = model.get('description')

            models.append(ModelConfig(
                model_name,
                model_owner,
                model_tags,
                model_schedule,
                model_sla,
                model_description
            ))

        return models
