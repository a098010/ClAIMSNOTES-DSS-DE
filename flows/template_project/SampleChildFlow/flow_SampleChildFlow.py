from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect import context, task
from pgr_prefect.tasks import SFSQLFile, S3ObjectTrigger
from datetime import timedelta
from pgr_prefect.utils import is_flow_running
from prefect import Parameter
from prefect.engine import signals
import os
import datetime

secret_key = os.getenv("secret_key")
secret_bucket = os.getenv("secret_bucket")
trigger_bucket = os.getenv("trigger_bucket")
snowflake_env = os.getenv("snowflake_env")
trigger_delay = os.getenv("trigger_delay")
trigger_max_attempts = os.getenv("trigger_max_attempts")
sql_warehouse = os.getenv("sql_warehouse")
sql_path = "flows/DS1/SampleChildFlow/sql"


@task(name="validate_parameters")
def validate_parameters_task(process_date):
    logger = context.get("logger")
    # Validate the parameter is the correct date format
    date_format = "%Y-%m-%d"
    try:
        date_obj = datetime.datetime.strptime(process_date, date_format)
    except ValueError:
        logger.error("Incorrect data format, should be YYYY-MM-DD")
        raise signals.FAIL()
    logger.info(f"Processing data from {process_date}")


@task(name="predecessor_checks")
def predecessor_checks_task():
    logger = context.get("logger")
    # check to see if the flow is already running
    if is_flow_running():
        logger.error(
            "Flow run {0} stopped because {1} flow is already running".format(
                context.get("flow_run_name"), context.get("flow_name")
            )
        )
        raise signals.FAIL()


@task(name="sample_child_flow_trigger")
def sample_child_flow_trigger_task(process_date: str):
    S3ObjectTrigger().run(
        filename=f"app_name/triggers/sample_child_flow_{process_date}",
        bucket=trigger_bucket,
        delay=int(trigger_delay),
        max_attempts=int(trigger_max_attempts),
    )


run_sql_task = SFSQLFile(
    log_stdout=True,
    max_retries=1,
    retry_delay=timedelta(seconds=0),
    secret_key=secret_key,
    secret_bucket=secret_bucket,
    snowflake_env=snowflake_env,
    warehouse=sql_warehouse,
    database="app_name",
)

with Flow(
    name="sample_child_flow",
    executor=LocalDaskExecutor(num_workers=4),
) as flow:

    process_date = Parameter(name="process_date", default="1900-01-01")
    validate_param = validate_parameters_task(process_date)
    predecessor_checks = predecessor_checks_task(upstream_tasks=[validate_param])

    sample_child_flow_trigger = sample_child_flow_trigger_task(
        process_date, upstream_tasks=[predecessor_checks]
    )

    sql_file_1 = run_sql_task(
        sql_file=f"{sql_path}/elt/sql_file_1.sql",
        upstream_tasks=[sample_child_flow_trigger],
        data=[process_date, "flow_run_id", "task_run_id"],
        task_args={"name": "sql_file_1_task"},
    )

    sql_file_2 = run_sql_task(
        sql_file=f"{sql_path}/elt/sql_file_2.sql",
        upstream_tasks=[sql_file_1],
        data=[process_date, "flow_run_id", "task_run_id"],
        task_args={"name": "sql_file_2_task"},
    )
