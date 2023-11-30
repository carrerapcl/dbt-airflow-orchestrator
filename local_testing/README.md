# Local testing of the orchestrator
This part of the project offers all the components required to test the orchestrator locally. It gets a local instance of Airflow running using Docker, and a sample of a dbt manifest file is also provided so some sample DAGs can be generated.

NOTE: Airbyte and Kafka Connect modules are disabled in this local environment.

## How to set up the docker compose setup
This file can stand up Airflow for local development of the orchestrator script and its generated DAGs.

You can get the local environment up and running using the Makefile by running the `make setup` command. What it does is:
1. Build the dockerfile and tag it was `my-airflow:0.0.1` (this is needed in the compose as we add some plugins to the base airflow image)
2. Run `docker-compose up` - this will bring up the entire Airflow stack
3. Airflow should be available at `localhost:8080` with the user/password being airflow/airflow

You need to install and activate Docker first.

There are only 2 things needed to do before `make setup`:
1.	Generate some DAGs with the main script (check the README for information on how to do this)

2.	Copy the files from your own dbt setup into `local_testing/artifacts/dags`:
    
    a.	Copy your `manifest.json` file into `local_testing/artifacts/dags/dbt/target`.

    b.	Copy your `dbt_project.yml` file into `local_testing/artifacts/dags/dbt`.

    c.	Copy your `profiles.yml` file into `local_testing/artifacts/dags/dbt/profiles`. This is not necessary if you don’t want the DAGs to actually run the models in your target database.


Dummy versions for all these files are provided by default, so you could actually test the local build first with the files provided.

All DAGs in the `local_testing/dags` directory are loaded into Airflow. You can generate DAGs using the sample manifest file provided in the `resources` directory (follow the README instructions at the root of this repo).
