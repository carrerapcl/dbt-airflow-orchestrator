# dbt-airflow-orchestrator
Orchestration tool to generate Airflow DAGs based on model dependencies, from EL tools to dbt transformations to visualization tools.

## What does it do?
The Orchestrator project generates full lineage DAGs that collect all required tables and sources together and produce a final target table.
This includes dbt models, data ingestion tools like Kafka Connect or Airbyte, and data sources included in data visualization tools (like Tableau extracts).

### How does it achieve this?
The project requries that the user defines a list of tables to generate DAGs for.
It then reads the pre-generated DBT manifest file and uses this to determine the full lineage of the requested tables and to construct a DAG that contains all the dependencies of the target table.
It generates these DAGs and then places them in the configured output directory.

A recommended use of this project is to run it in CICD and then output the generated DAGs to a location shared with Airflow (i.e. an S3 bucket) so that it can read the files and schedule the DAGs.

## Code structure
The entry point of the project is `main.py`. It collects the CLI params, reads the list of DAGs to orchestrate from a file and then triggers the lineage and dag generation.

The core of the projct is `services/lineage_service.py`. Along with the classes it depends on, it reads the dbt manifest, generates the lineage and then builds the DAGs requested in the orchestration file.

The project requires the user create a `config.yaml` file at the root, following the example provided in `config_template.yaml`, and add the appropiate credentials.

## Running Locally
#### Requirements
`python@3.9`
`dbt-core >= 1.4`

This assumes you're comfortable with python and are already running a virtualenv for this project.

1. Install requirements `$ pip install -r requirements.txt`
2. `$ python main.py -m <PATH_TO_DBT_MANIFEST_JSON> `

#### Command line options
`-c` Use cache - this is a faster way to run the project, instead of generated the lineage it will use a previously generated lineage and graph (stored in the lineage and graph .pickles) - this also does not require a manifest file.
`-m` Path to dbt `manifest.json`, this is required if running without the cache option enabled but can be excluded otherwise.
`-f` Path to orchestration file, your list of tables you'd like to orchestrate. If not specified, it defaults to `./resources/orchestration.yaml`.
`-d` Path to a dir to output the generated DAGs to. If not specified, defaults to `./generated_dags/`, creating the directory if it doesn't exist.


### Testing end to end locally
Please refer to the README in the `local_testing` directory for instructions on how to run/test this project locally.