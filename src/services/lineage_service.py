from graph import Graph
import pickle
from typing import List

from src.entities.model_config import ModelConfig
from src.services.dbt_manifest_parser import DBTManifestParser
# from src.services.airbyte_connections_getter import AirbyteConnectionGetter
from src.services.tableau_data_sources_getter import TableauDataSourcesGetter
from src.services.dag_generator import DAGGenerator
from src.services.utils import bcolors


def load_dbt_objects(manifest_path: str):
    return DBTManifestParser().get_models_and_sources(manifest_path)

# unused, see comment in class
# def load_kafkaconnect_connectors():
#     kccg = KafkaConnectConnectorsGetter()
#     return kccg.main()


# def load_airbyte_connections():
#     acg = AirbyteConnectionGetter()
#     return acg.main()


def load_tableau_data_sources():
    tableau_data_sources_getter = TableauDataSourcesGetter()
    return tableau_data_sources_getter.main()


def create_source_mapper(dbt_sources, airbyte_connections):
    sources_mapper = {}
    for dbt_source_name, dbt_source in dbt_sources.items():

        # for kafkaconnect_connector in kafkaconnect_connectors:
        #     if dbt_source.table in kafkaconnect_connector.tables:
        #         sources_mapper.update({dbt_source_name: kafkaconnect_connector})
        #         break

        for airbyte_connection in airbyte_connections:
            if dbt_source.table in airbyte_connection.tables:
                sources_mapper.update({dbt_source_name: airbyte_connection})
                break
    return sources_mapper


def create_tableau_dbt_models_mapper(tableau_data_sources, dbt_models):
    mapper = {}

    for _, tableau_data_source in tableau_data_sources.items():
        mapper.update({tableau_data_source.name: []})
        for table in tableau_data_source.tables:
            dbt_model = dbt_models.get(table.name.upper())
            if dbt_model is None:
                dbt_model = dbt_models.get(table.name.lower())
            if dbt_model:
                mapper[tableau_data_source.name].append(dbt_model.name)
    return mapper


def dfs_lineage(graph, node, visited=None):
    if visited is None:
        visited = set()
    visited.add(node)
    if len(graph.nodes(from_node=node)) == 0:
        lineage = {node: None}
    else:
        lineage = {node: {}}
    for neighbor in graph.nodes(from_node=node):
        if neighbor not in visited:
            lineage[node].update(dfs_lineage(graph, neighbor))
    return lineage


def calculate_lineage(graph):
    full_lineages = {}
    for node in graph.nodes():
        full_lineages[node] = dfs_lineage(graph, node)
    return full_lineages


def save_lineage(graph, full_lineages):
    with open('graph.pickle', 'wb') as handle:
        pickle.dump(graph, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('full_lineages.pickle', 'wb') as handle:
        pickle.dump(full_lineages, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_lineage():
    with open('full_lineages.pickle', 'rb') as handle:
        full_lineages = pickle.load(handle)

    with open('graph.pickle', 'rb') as handle:
        graph = pickle.load(handle)

    return graph, full_lineages

def _build_graph_nodes(graph, dbt_models) -> Graph:
    # Register dbt models nodes
    for dbt_model_name, dbt_model in dbt_models.items():
        graph.add_node(dbt_model_name, dbt_model)

    # Register kafkaconnect nodes
    # for kafka_connector in kafka_connectors:
    #     graph.add_node(kafka_connector.name, kafka_connector)

    # Airbyte disabled for now
    # Register airbyte connections nodes
    # for airbyte_connection in airbyte_connections:
    #     graph.add_node(airbyte_connection.name, airbyte_connection)

    return graph

def _build_destination_graph_nodes(graph, tableau_data_sources) -> Graph:
    # Register Tableau DataSources nodes
    for tableau_data_source_name, tableau_data_source in tableau_data_sources.items():
        graph.add_node(tableau_data_source.name, tableau_data_source)
    
    return graph

def _add_graph_edges(graph, dbt_models, dbt_sources) -> Graph:
    for dbt_model_name, dbt_model in dbt_models.items():

        # between models and models
        for model_ref in dbt_model.refs:
            graph.add_edge(dbt_model_name, model_ref['name'])

        # between models and sources
        # for dbt_source in dbt_model.sources:
        #     # source_identifier = f""dbt_source[0]
        #     source_identifier = ".".join(dbt_source)
        #     source_object = sources_mapper.get(source_identifier)
        #     if source_object:
        #         graph.add_edge(dbt_model.name, source_object.name)

    return graph

def _add_destination_graph_edges(graph, dbt_models) -> Graph:
    tableau_data_sources = load_tableau_data_sources()
    tableau_dbt_models_mapper = create_tableau_dbt_models_mapper(tableau_data_sources, dbt_models)

    # between tableau data sources and dbt models
    for tableau_data_source, tableau_dbt_models in tableau_dbt_models_mapper.items():
        for tableau_dbt_model in tableau_dbt_models:
            graph.add_edge(tableau_data_source, tableau_dbt_model)

    return graph

def _build_lineage(manifest_path: str):
    dbt_models, dbt_sources = load_dbt_objects(manifest_path)

    # airbyte_connections = load_airbyte_connections()
    # kafka_connectors = load_kafkaconnect_connectors()
    # sources_mapper = create_source_mapper(dbt_sources)
    
    ## (angusb) tableau disabled for now
    graph = Graph()
    graph = _build_graph_nodes(graph, dbt_models)
    # graph = _build_destination_graph_nodes(graph, tableau_data_sources)
    graph = _add_graph_edges(graph, dbt_models, dbt_sources)
    # graph = _add_destination_graph_edges(graph, dbt_models)

    lineages = calculate_lineage(graph)
    save_lineage(graph, lineages)
    return graph, lineages

def _generate_dags(graph, lineages, models, dag_path: str) -> None:
    dag_generator = DAGGenerator()
    for model in models:
        # side effect: writes files
        dag_generator.generate_dag(graph, lineages, model.name, model.schedule, model.sla, dag_path)

    print(f"{bcolors.OKGREEN}Finished!{bcolors.ENDC}")


def full_lineage(models: List[ModelConfig], manifest_path: str, dag_path: str) -> None:
    print("Generating lineage from manifest...")
    graph, lineages = _build_lineage(manifest_path)
    _generate_dags(graph, lineages, models, dag_path)


def use_cached_lineage(models: List[ModelConfig], dag_path: str) -> None:
    print("Using cached lineage...")
    graph, lineages = load_lineage()
    _generate_dags(graph, lineages, models, dag_path)
