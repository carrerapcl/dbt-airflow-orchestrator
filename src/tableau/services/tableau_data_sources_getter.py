from src.core.services.utils import load_config, get_config_value, get_default_config_path
from tableauserverclient import Server, PersonalAccessTokenAuth
from src.tableau.entities.tableau_table import TableauTable
from src.tableau.gateway.tableau_cli import TableauCLI
from src.tableau.entities.tableau_datasource import TableauDatasource

class TableauDataSourcesGetter:
    def __init__(self, config_path=get_default_config_path()):
        self.config = load_config(config_path)
        self.tableau_cli = TableauCLI(self._setup_tableau_server())

    def _setup_tableau_server(self):
        # Create authentication credentials using Personal Access Token
        endpoint = get_config_value(self.config, 'tableau', 'server_endpoint')
        token_name = get_config_value(self.config, 'tableau', 'token_name')
        token_value = get_config_value(self.config, 'tableau', 'token_value')
        site_id = get_config_value(self.config, 'tableau', 'site_id')
        credentials = PersonalAccessTokenAuth(token_name, token_value, site_id='saltpayreportingco')

        # Connect to Tableau Server using the authentication token
        server = Server(endpoint, use_server_version=True)
        server.auth.sign_in(credentials)
        return server

    def main(self):
        tableau_data_sources = {}
        data_sources = self.tableau_cli.get_data_sources()

        for source in data_sources['publishedDatasources']:

            if source['has_extracts'] and source['tables'] and len(source['tables']) > 0:

                tableau_tables: list[TableauTable] = []
                for table in source['tables']:
                    table_id = table['id']
                    table_luid = table['luid']
                    table_name = table['table_name']
                    schema = table['schema']
                    database = table.get('database', {}).get('name')

                    tableau_table = TableauTable(database, schema, table_name, table_id, table_luid)
                    tableau_tables.append(tableau_table)

                tableau_data_sources[source['name']] = TableauDatasource(source['id'], source['luid'], source['name'], tableau_tables)

        return tableau_data_sources

# If this file is ran directly, print tableau_data_sources:
if __name__ == '__main__':
    tdsg = TableauDataSourcesGetter()
    acs = tdsg.main()
    for ac in acs:
        print(ac)
