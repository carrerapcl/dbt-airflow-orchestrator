import os
from datetime import datetime
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from operators.dbt_sla_operator import DbtRunOperator, DbtTestOperator
# from operators.airbyte_sla_operator import AirbyteSLAOperator
# from operators.tableau_sla_operator import TableauSLAOperator
# configs for opsgenie in case of prod failure
# from plugins.opsgenie.alert import create_opsgenie_alert
# from plugins.slack.error_notification import error_notification


# if os.getenv("ENVIRONMENT") == "prd":
#     alert_notifier = create_opsgenie_alert
# else:
#     alert_notifier = error_notification
    
PROJECT_DIR = "/opt/airflow/dags/artifacts/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

DEFAULT_DAG_ARGS = {
    # "on_failure_callback": alert_notifier,
    "retries": 0,
    "owner": "DataAnalysts",
}


DEFAULT_TASK_ARGS = {
    "profiles_dir": PROJECT_DIR + "/profiles",
    "dir": PROJECT_DIR,
    "warn_error": False,
    "dbt_bin": DBT_BIN,
}


def get_dbt_run_task(model_name: str, sla: int):
    return DbtRunOperator(
        task_id="run",
        models=model_name,
        sla=sla,
        **DEFAULT_TASK_ARGS,
    )


def get_dbt_test_task(model_name: str, sla: int):
    return DbtTestOperator(
        task_id="test",
        models=model_name,
        sla=sla,
        **DEFAULT_TASK_ARGS,
    )


@dag(
    dag_id="saltid_accounts",
    default_args=DEFAULT_DAG_ARGS,
    params={
        "priority": "P3",
        "component": "DBT",
    },
    start_date=datetime(2022, 6, 8),
    description="TODO Description",
    schedule_interval="0 8 * * *",
    max_active_runs=1,
    catchup=False,
    tags=['dbt'] + [''],
)
def tasks():

    with TaskGroup(group_id='saltid___aux_mirror_accounts') as dbt_saltid___aux_mirror_accounts:
        dbt_saltid___aux_mirror_accounts_run = get_dbt_run_task('saltid___aux_mirror_accounts', sla=24) 
        dbt_saltid___aux_mirror_accounts_test = get_dbt_test_task('saltid___aux_mirror_accounts', sla=24) 
        dbt_saltid___aux_mirror_accounts_run >> dbt_saltid___aux_mirror_accounts_test 

    with TaskGroup(group_id='saltid__accounts') as dbt_saltid__accounts:
        dbt_saltid__accounts_run = get_dbt_run_task('saltid__accounts', sla=24) 
        dbt_saltid__accounts_test = get_dbt_test_task('saltid__accounts', sla=24) 
        dbt_saltid__accounts_run >> dbt_saltid__accounts_test 

    with TaskGroup(group_id='saltid__delegations') as dbt_saltid__delegations:
        dbt_saltid__delegations_run = get_dbt_run_task('saltid__delegations', sla=24) 
        dbt_saltid__delegations_test = get_dbt_test_task('saltid__delegations', sla=24) 
        dbt_saltid__delegations_run >> dbt_saltid__delegations_test 

    with TaskGroup(group_id='gmd__stores') as dbt_gmd__stores:
        dbt_gmd__stores_run = get_dbt_run_task('gmd__stores', sla=24) 
        dbt_gmd__stores_test = get_dbt_test_task('gmd__stores', sla=24) 
        dbt_gmd__stores_run >> dbt_gmd__stores_test 

    with TaskGroup(group_id='saltid_accounts') as dbt_saltid_accounts:
        dbt_saltid_accounts_run = get_dbt_run_task('saltid_accounts', sla=24) 
        dbt_saltid_accounts_test = get_dbt_test_task('saltid_accounts', sla=24) 
        dbt_saltid_accounts_run >> dbt_saltid_accounts_test 

    with TaskGroup(group_id='saltid_int_accounts') as dbt_saltid_int_accounts:
        dbt_saltid_int_accounts_run = get_dbt_run_task('saltid_int_accounts', sla=24) 
        dbt_saltid_int_accounts_test = get_dbt_test_task('saltid_int_accounts', sla=24) 
        dbt_saltid_int_accounts_run >> dbt_saltid_int_accounts_test 

    with TaskGroup(group_id='saltid__linked_accounts') as dbt_saltid__linked_accounts:
        dbt_saltid__linked_accounts_run = get_dbt_run_task('saltid__linked_accounts', sla=24) 
        dbt_saltid__linked_accounts_test = get_dbt_test_task('saltid__linked_accounts', sla=24) 
        dbt_saltid__linked_accounts_run >> dbt_saltid__linked_accounts_test 

    with TaskGroup(group_id='gmd__organisation_members') as dbt_gmd__organisation_members:
        dbt_gmd__organisation_members_run = get_dbt_run_task('gmd__organisation_members', sla=24) 
        dbt_gmd__organisation_members_test = get_dbt_test_task('gmd__organisation_members', sla=24) 
        dbt_gmd__organisation_members_run >> dbt_gmd__organisation_members_test 

    with TaskGroup(group_id='saltid_int_accounts_mappings') as dbt_saltid_int_accounts_mappings:
        dbt_saltid_int_accounts_mappings_run = get_dbt_run_task('saltid_int_accounts_mappings', sla=24) 
        dbt_saltid_int_accounts_mappings_test = get_dbt_test_task('saltid_int_accounts_mappings', sla=24) 
        dbt_saltid_int_accounts_mappings_run >> dbt_saltid_int_accounts_mappings_test 

    [dbt_saltid_int_accounts, dbt_saltid_int_accounts_mappings] >> dbt_saltid_accounts

    [dbt_saltid__accounts, dbt_saltid___aux_mirror_accounts, dbt_saltid__delegations, dbt_saltid__linked_accounts, dbt_gmd__organisation_members, dbt_gmd__stores] >> dbt_saltid_int_accounts

    [dbt_saltid_int_accounts] >> dbt_saltid_int_accounts_mappings

    [dbt_saltid__accounts, dbt_saltid___aux_mirror_accounts, dbt_saltid__delegations, dbt_saltid__linked_accounts, dbt_gmd__organisation_members, dbt_gmd__stores] >> dbt_saltid_int_accounts


tasks()
