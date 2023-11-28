# dbt-airflow-orchestrator
This project is an data pipeline orchestration tool to generate Airflow DAGs based on data model dependencies across systems.

The base system is [dbt](https://github.com/dbt-labs/dbt-core), used in the Transformation layer, but this orchestrator can integrate the data lineage from the EL stage (i.e. Airbyte, Kafka Connect) to the visualization layer (i.e. Tableau).

Currently, this project will work better with a Snowflake data warehouse. Better support for other warehouses, plus integrations with other ETL and visualization tools, will be coming soon!

## What does it do, and how?
The dbt-airflow-orchestrator project generates full lineage DAGs that collect all required tables and sources together and produce a final target table.
This includes dbt models, data ingestion tools like Kafka Connect or Airbyte, and data sources included in data visualization tools (like Tableau extracts).

The project requries that the user defines a list of tables to generate DAGs for. This is done in a simple yaml file, where the schedule and SLAs (minimum freshness of the data) are also defined.

It then reads the pre-generated dbt `manifest` file and uses this to determine the full lineage of the requested tables and to construct a DAG that contains all the dependencies of the target table. It generates these DAGs and then places them in the configured output directory.

A recommended use of this project is to run it in CICD and then output the generated DAGs to a location shared with Airflow (i.e. an S3 bucket), so that it can read the files and schedule the DAGs.

## Code structure
The entry point of the project is `main.py`. It collects the CLI params, reads the list of DAGs to orchestrate from a file and then triggers the lineage and dag generation.

The core of the projct is `services/lineage_service.py`. Along with the classes it depends on, it reads the dbt manifest, generates the lineage and then builds the DAGs requested in the orchestration file.

The project requires the user to create a `config.yaml` file at the root, following the example provided in `resources/config_template.yaml`, and add the appropiate credentials.

## Creating DAGs
#### Requirements
- `python@3.9`
- `dbt-core >= 1.6`

This assumes you're comfortable with python and are already running a virtualenv for this project.

1. Install requirements `$ pip install -r requirements.txt`
2. Generate the DAGs with `$ python main.py -m <PATH_TO_DBT_MANIFEST_JSON> `

#### Command line options
- `-c` Use cache - this is a faster way to run the project, instead of generated the lineage it will use a previously generated lineage and graph (stored in the lineage and graph .pickles) - this also does not require a manifest file.
- `-m` Path to dbt `manifest.json`, this is required if running without the cache option enabled but can be excluded otherwise.
- `-f` Path to orchestration file, the list of tables you'd like to orchestrate. If not specified, it defaults to `./resources/orchestration.yaml`.
- `-d` Path to a directory to output the generated DAGs to. If not specified, defaults to `./generated_dags/`, creating the directory if it doesn't exist.

## Testing end to end locally
Please refer to the README in the `local_testing` directory for instructions on how to run or test this project locally.