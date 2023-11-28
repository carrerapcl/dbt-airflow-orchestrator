from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.context import Context
from operators.base_sla_operator import SLABaseOperator
# Import custom DBT operators from airflow_dbt_snowflake library
from airflow_dbt_snowflake.operators.dbt_operator import (
    DbtRunOperator as BaseDbtRunOperator,
    DbtTestOperator as BaseDbtTestOperator
)

class DummySLAOperator(SLABaseOperator):
    def execute(self, context: Context):
        pass

class DbtRunOperator(SLABaseOperator, BaseDbtRunOperator):
    """
    Custom Operator to run dbt models, extending the functionality
    from the airflow_dbt_snowflake package. Use this if there are any
    additional behaviors or checks specific to your project.
    """
    pass  # You can add project-specific methods or override base class methods if necessary

class DbtTestOperator(SLABaseOperator, BaseDbtTestOperator):
    """
    Custom Operator to run dbt tests, extending the functionality
    from the airflow_dbt_snowflake package. Use this if there are any
    additional behaviors or checks specific to your project.
    """
    pass  # You can add project-specific methods or override base class methods if necessary
