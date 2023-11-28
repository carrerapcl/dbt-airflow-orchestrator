import argparse
import traceback
import os

from src.core.services.orchestration_config_parser import OrchestrationConfigParser
from src.core.services.lineage_service import full_lineage, use_cached_lineage
from src.dbt.services.dbt_manifest_parser import ManifestNotFoundError
from src.core.services.utils import bcolors


if __name__ == "__main__":

    argParser = argparse.ArgumentParser()
    argParser.add_argument("-m", "--manifest", dest='manifest_path', help="path to dbt manifest file", required=False)
    argParser.add_argument("-f", "--file", dest='file', help="path to file to use for models instead of default", required=False)
    argParser.add_argument("-d", "--dagpath", dest='dag_path', help="path of directory to output generated dags to", required=False)
    argParser.add_argument("-c", "--cached", dest='cache', help="use cached lineage from previous run", action='store_true', default=False, required=False)

    args = argParser.parse_args()
   
    shouldUseCached = args.cache

    file = "./resources/orchestration.yaml"
    if args.file is not None:
        file = args.file
        
    dag_path = './generated_dags/'
    if args.dag_path is not None:
        dag_path = args.dag_path
        # force path to be a directory
        if dag_path[-1] != '/':
            dag_path += '/'
    # Check and create the directory if it doesn't exist
    if not os.path.exists(dag_path):
        os.makedirs(dag_path)

    #  structured config class for all these values (file_path, manifest_path, airbyte_url etc)
    manifest_path = args.manifest_path

    models_to_orchestrate = OrchestrationConfigParser().read_models(file)
    if models_to_orchestrate is None or models_to_orchestrate == []:
        print(f"{bcolors.WARN}No models to orchestrate (orchrestration.yaml is empty). Closing.{bcolors.ENDC}")
        exit(0)
    else:
        model_names = [model.name for model in models_to_orchestrate]
        print(f"Models to orchestrate: {model_names}")

    if shouldUseCached:
        use_cached_lineage(models_to_orchestrate, dag_path)
        exit()

    try:
        if manifest_path is None:
            print(f"{bcolors.FAIL}[ERROR]:{bcolors.ENDC}: manifest_path parameter is required if cache is not being used; pass it with -m <PATH>")
            exit(1)
        full_lineage(models_to_orchestrate, manifest_path, dag_path)
    except Exception as e:
        if isinstance(e, ManifestNotFoundError):
            print(f"{bcolors.FAIL}[ERROR]:{bcolors.ENDC}:", e)
            exit(1)
        else:
            # if it's not an expected error, print the full strack trace
            traceback.print_exc()
            exit(1)
