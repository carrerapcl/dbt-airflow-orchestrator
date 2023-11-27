from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.context import Context
from operators.base_sla_operator import SLABaseOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator as BaseDbtRunOperator
from airflow_dbt.operators.dbt_operator import DbtTestOperator as BaseDbtTestOperator


class DummySLAOperator(SLABaseOperator):
    def execute(self, context: Context):
        pass


class DbtRunOperator(SLABaseOperator, BaseDbtRunOperator):
    """Operator to run dbt models in Airflow."""

    def execute(self, context):
        full_refresh = context["dag_run"].conf.get("full_refresh", False)
        self.full_refresh = full_refresh
        # If the DAG has been triggered with a configuration like:
        # { "full_refresh": true,"models": ["model_name"]}
        # Then trigger only the models in that dictionary,
        # and with a --full-refresh flag.
        # If not, just trigger them as usual (full-refresh = False)
        if "models" in context["dag_run"].conf and self.models not in context[
            "dag_run"
        ].conf.get("models", []):
            self.log.info(f"Skipping execution for model {self.models}...")
            return
        try:
            return super().execute(context)
        except AirflowException as ae:
            # If we get a `dbt run` error, and happens to an incremental model,
            # then run the model again with --full-refresh flag automatically,
            # excepting for "essential models"
            essential_models = Variable.get(
                "dbt_essential_models", deserialize_json=True, default_var=[]
            )
            incremental_models = Variable.get(
                "dbt_incremental_models", deserialize_json=True, default_var=[]
            )
            if (
                "dbt command failed" in str(ae)
                and self.models in incremental_models
                and not self.full_refresh
            ):
                if self.models not in essential_models:
                    self.log.info(
                        "Error may be caused by schema change on incremental model, retrying with full_refresh = True"
                    )
                    self.full_refresh = True
                    return super().execute(context)
                else:
                    self.log.info(
                        "Not retrying with full_refresh = True because the model is considered an essential model"
                        "Please check the source table and trigger a full-refresh manually if required"
                    )
                    raise ae
            else:
                raise ae


class DbtTestOperator(SLABaseOperator, BaseDbtTestOperator):
    def execute(self, context):
        if "models" in context["dag_run"].conf and self.models not in context[
            "dag_run"
        ].conf.get("models", []):
            self.log.info(f"Skipping execution for model {self.models}...")
            return
        return super().execute(context)