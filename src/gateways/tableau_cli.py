from tableauserverclient import Server

class TableauCLI:
    def __init__(self, server: Server):
        self._server = server

    def get_data_sources(self):

        # Define the GraphQL query to get a list data sources, and for each data source, the Snowflake tables they use
        query = """
        {
            publishedDatasources {
                id: id
                name: name
                luid: luid
                extract_last_refresh_time: extractLastRefreshTime
                extract_last_incremental_update_time: extractLastIncrementalUpdateTime
                created_at: createdAt
                updated_at: updatedAt
                has_extracts: hasExtracts
                tables: upstreamTables {
                    id
                    luid
                    table_name: name
                    schema
                    connectionType
                    description
                    referencedByQueries {
                        name
                    }
                    database{
                        id
                        luid
                        name
                    }
                }
            }
        }
        """

        response = self._server.metadata.query(query)
        return response['data']
