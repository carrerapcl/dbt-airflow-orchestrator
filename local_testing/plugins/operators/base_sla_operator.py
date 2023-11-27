from airflow.models import BaseOperator
from airflow.utils.db import provide_session
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
from psycopg2.extras import DictCursor
from sqlalchemy import text
from abc import ABC

# This doesn't work as expected, task_ids are unique per DAG meaning if a model is included in multiple DAGs they will appear as different models to this code, meaning it will not be able to track the last run across the stack but only for this specific DAG, essentially useless
class SLABaseOperator(BaseOperator, ABC):
    """
    Custom Airflow operator to check the last time the task ran using metadata database.
    If the last run was more than the specified number of hours (sla), the run is skipped.
    """

    ui_color = '#F0E68C'

    def __init__(self, sla: int = None, **kwargs):
        super().__init__(**kwargs)
        self.sla: int = sla
        self.skip = False

    @provide_session
    def pre_execute(self, context, session=None):
        last_run = self._get_last_run(session)
        self.log.warning(last_run)

        if last_run and self._can_skip_run(last_run['start_date']):
            self.log.info(f"Last run for task '{self.task_id}' was at '{last_run}'. Skipping the run.")
            self.skip = True
            raise AirflowSkipException()

    def _get_last_run(self, session):
        sql_query = f'''
        SELECT 
            task_id, dag_id, run_id, start_date, end_date, duration, state, try_number, job_id
        FROM 
            task_instance ti
        WHERE 
            ti.task_id = '{self.task_id}'
            AND ti.state = 'success'
        ORDER BY 
            start_date DESC
        LIMIT 1'''

        result = session.execute(text(sql_query), execution_options={"cursor_factory": DictCursor})
        if result:
            columns = result.keys()
            dict_result = [dict(zip(columns, row)) for row in result.fetchall()]
            if len(dict_result) > 0:
                return dict_result[0]

        return None

    def _can_skip_run(self, last_run):
        self.log.info(last_run)
        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        current_time.replace()
        sla_time = last_run + timedelta(hours=self.sla)
        return current_time < sla_time