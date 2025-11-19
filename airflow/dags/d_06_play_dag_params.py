"""
CLI command:
airflow dags trigger -c '{"start_date": "2024-01-01", "end_date": "2024-03-01"}' d_06_example_pass_parameters_cli
airflow dags test -c '{"start_date": "2024-01-01", "end_date": "2024-03-01"}' d_06_example_pass_parameters_cli
"""
from datetime import datetime

from airflow.decorators import dag, task, bash_task
from airflow.operators.python import get_current_context
from airflow.models.param import Param


# Default values for start_date and end_date
# DEFAULT_START_DATE = "2023-01-01"
# DEFAULT_END_DATE = "2023-12-31"

@dag(
    dag_id="d_06_play_with_params",
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example", "cli_variables_defaults"],
    params = {"start_date":Param("2023-01-01",type='string'),
              "end_date":Param("2023-12-01",type='string')}
)
def d_06_example_pass_parameters_cli():

    @task
    def extract_parameters():
        ctx = get_current_context()
        config = ctx["dag_run"].conf or {}
        default_params = ctx["params"]
        start_date = config.get('start_date',default_params['start_date'])
        end_date = config.get('end_date',default_params['end_date'])
        return {"start_date": start_date, "end_date": end_date}

    @bash_task
    def report_info():
        return "echo {{ dag_run.conf['start_date'] if dag_run.conf else params.start_date }}"

    extract_param_ti = extract_parameters()
    report_info().set_upstream(extract_param_ti)


# Create the DAG instance
d_06_example_pass_parameters_cli()
