import json
import yaml
from typing import Dict
from src.dbt.entities.dbt_model import DBTModel
from src.dbt.entities.dbt_source import DBTSource
from src.dbt.entities.dbt_test import DBTTest

# to map generic FileNotFound to a specific error
class ManifestNotFoundError(FileNotFoundError):
    pass

class DBTManifestParser:
    def _load_manifest_file(self, manifest_path: str) -> Dict:
        try:
            with open(manifest_path, "r") as f:
                self._manifest = json.load(f)
                return self._manifest
        except FileNotFoundError as e:
            raise ManifestNotFoundError(f"DBT manifest not found, either the path is wrong or the file has not been generated path={manifest_path}") from e

    # TODO this is not actually returning tests
    def _load_manifest_models_and_tests(self, manifest: Dict) -> Dict[str, DBTModel]:
        dbt_models: dict[str, DBTModel] = {}
        for model_name, model_data in manifest["nodes"].items():
            if model_data["resource_type"] == "model":
                name = model_data.get('name')
                sources = model_data.get('sources')
                refs = model_data.get('refs')
                tags = model_data.get('tags')
                priority = model_data.get("priority", "P3")
                schema = model_data.get('schema')
                database = model_data.get('database')
                model_group_name = "/".join(model_data.get('path').split('/')[:-1])
                schedule = None
                sla = None
                owner = None

                dbt_models.update(
                    {name: DBTModel(name, sources, refs, tags, schedule, priority, owner, schema, database, sla)})

        return dbt_models

    def _load_manifest_sources(self, manifest: Dict) -> Dict[str, DBTSource]:
        """
        Loads manifest.json and optionally filter out non-model assets.
        """
        dbt_sources = {}
        for source_name, source_data in manifest["sources"].items():

            name = source_data.get('name')
            identifier = source_data.get('identifier')  # table_name

            source_group_name = source_data.get('source_name')

            database = source_data.get('database')
            schema = source_data.get('schema')
            dbt_source = DBTSource(table_name=identifier, schema=schema, database=database, source_name=name,
                                   source_group_name=source_group_name)

            unique_identifier = f"{source_group_name}.{name}"

            dbt_sources[unique_identifier] = dbt_source

        return dbt_sources

    def get_models_and_sources(self, manifest_path: str) -> (Dict[str, DBTModel], Dict[str, DBTSource]):
        manifest = self._load_manifest_file(manifest_path)
        models = self._load_manifest_models_and_tests(manifest)
        sources = self._load_manifest_sources(manifest)
        return models, sources

