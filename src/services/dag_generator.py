from src.entities.airbyte_connection import AirbyteConnection
from src.entities.dbt_model import DBTModel
from src.services.utils import load_config, get_config_value, get_default_config_path
from graph import Graph

# TODO: Needs work, support configs, etc

class DAGGenerator:
    def __init__(self, config_path=get_default_config_path()):
        self.config = load_config(config_path)

    DAG_FILE_SUFFIX = '_dag.py'
    DAG_FOOTER = "\ntasks()\n"

    def generate_dag(self, graph, lineages, model_name, schedule, sla, dag_path: str):
        lineage = lineages[model_name]
        parent_node = graph.node(model_name)

        # Common header for all DAGs
        dag_code = self._generate_dag_header_code(parent_node.name, schedule, parent_node.tags)

        # Define tasks
        dag_code += self._generate_task_code(lineage, graph, sla)

        # Define relationships between dask
        dag_code += self._generate_task_dependency_code(lineage, graph)

        dag_code += self.DAG_FOOTER

        full_path = dag_path + parent_node.name + self.DAG_FILE_SUFFIX
        with open(full_path, 'w') as f:
            f.write(dag_code)
        print(f'Wrote file: {full_path}')


    def _get_node_task_name(self, node):
        if node.type == 'dbt_model':
            return f"dbt_{node.name}"
        if node.type == 'airbyte_connection':
            return f"airbyte_{node.sanitized_name}"
        # if node.type == 'kafkaconnect_connector':
        #     return f"kafkaconnect_{node.sanitized_name}"
        if node.type == 'tableau_datasource':
            return f"tableau_{node.name}"

    def _get_dbt_task_group_code(self, dbt_model: DBTModel, default_sla: int):
        task_name = self._get_node_task_name(dbt_model)
        task_group_name = task_name
        task_name_run = f"{task_name}_run"
        task_name_test = f"{task_name}_test"

        sla = dbt_model.sla if dbt_model.sla is not None else default_sla

        return f'''
    with TaskGroup(group_id='{dbt_model.name}') as {task_group_name}:
        {task_name_run} = get_dbt_run_task('{dbt_model.name}', sla={sla}) 
        {task_name_test} = get_dbt_test_task('{dbt_model.name}', sla={sla}) 
        {task_name_run} >> {task_name_test} 
'''

    def _get_airbyte_task_code(self, airbyte_connection: AirbyteConnection, default_sla):
        task_name_run = self._get_node_task_name(airbyte_connection)
        return f"""
    {task_name_run} = AirbyteSLAOperator(
        task_id='{task_name_run}',
        airbyte_conn_id='airbyte_local', 
        sla={default_sla}, 
        connection_id='{airbyte_connection.id}',
        timeout=3600,
        asynchronous=False,
    )
"""
## unused right now, restoring soon
#     def get_kafkaconnect_task_code(self, connector: KafkaConnectConnector, sla):
#         task_name_run = self.get_node_task_name(connector)
#         return f"""
#     {task_name_run} = KafkaConnectSLAOperator(
#         task_id='{task_name_run}',
#         sla={sla},
#     )
# """

    def _get_tableau_task_code(self, node, default_sla):
        return ""

    # BFS search implemented with a queue
    def _generate_task_dependency_code(self, sub_lineage, graph):
        queue = [sub_lineage]
        code = "\n"

        while queue:
            vertex = queue.pop(0)

            for node_name, children in vertex.items():
                if children is not None:
                    queue.append(children)
                    code += "    ["
                    for child in children:
                        child_node = graph.node(child)
                        child_task_name = self._get_node_task_name(child_node)
                        code += f"{child_task_name}, "

                    node = graph.node(node_name)
                    node_task_name = self._get_node_task_name(node)
                    code = code[:-2] + f"] >> {node_task_name}\n\n"

        return code

    # DFS implementation
    def _flatten_lineage_tree_to_set(self, lineage_tree, graph, visited):
        for key, children in lineage_tree.items():
            if key in visited:
                continue
            visited.add(key)

            if children is not None:
                self._flatten_lineage_tree_to_set(children, graph, visited=visited)

        return visited

    def _generate_task_code(self, lineage, graph, sla): 
        nodes = self._flatten_lineage_tree_to_set(lineage, graph, set())

        code = ""
        for node_name in nodes:
            node = graph.node(node_name)
            if node.type == 'dbt_model':
                code += self._get_dbt_task_group_code(node, sla)
            elif node.type == 'airbyte_connection':
                code +=self._get_airbyte_task_code(node, sla)
            # if node.type == 'kafkaconnect_connector':
            #     code = self.get_kafkaconnect_task_code(node, sla)
            elif node.type == 'tableau_datasource':
                code +=self._get_tableau_task_code(node, sla)
            else:
                print(f"unrecognised node_type found [name={node_name}, type={node.type}] continuing...")

        return code

    def _generate_dag_header_code(self, dag_name, schedule, tags):
        dbt_dir = get_config_value(self.config, 'dbt', 'project_dir')
        dag_header_code = f'''import os
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
    
PROJECT_DIR = "{dbt_dir}"
DBT_BIN = "/home/airflow/.local/bin/dbt"

DEFAULT_DAG_ARGS = {{
    # "on_failure_callback": alert_notifier,
    "retries": 0,
    "owner": "DataAnalysts",
}}


DEFAULT_TASK_ARGS = {{
    "profiles_dir": PROJECT_DIR + "/profiles",
    "dir": PROJECT_DIR,
    "warn_error": False,
    "dbt_bin": DBT_BIN,
}}


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
    dag_id="{dag_name}",
    default_args=DEFAULT_DAG_ARGS,
    params={{
        "priority": "P3",
        "component": "DBT",
    }},
    start_date=datetime(2022, 6, 8),
    description="TODO Description",
    schedule_interval="{schedule}",
    max_active_runs=1,
    catchup=False,
    tags=['dbt'] + ['{"', '".join(tags)}'],
)
def tasks():
'''
        return dag_header_code
