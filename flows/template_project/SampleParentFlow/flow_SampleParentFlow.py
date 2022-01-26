import datetime
import pendulum
import os
from prefect import Client, Flow, task, Parameter, context, Task
from prefect.engine import signals
from prefect.executors import LocalDaskExecutor
from prefect.tasks.prefect import StartFlowRun
from prefect.engine.state import State
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.backend import set_key_value, get_key_value
from pgr_prefect.utils import is_flow_running
from pgr_prefect.state_handlers import send_alert
from typing import Optional, Union


prefect_project = os.getenv("prefect_project")
voice_alert = os.getenv("voice_alert")
text_alert = os.getenv("text_alert")
environment = os.getenv("PGR_PREFECT_ENVIRONMENT")
process_date_key = f"{environment}_sample_parent_flow_process_date"

if environment == "prod":
    schedule = Schedule(
        clocks=[
            CronClock(cron="0 14 * * 0", start_date=pendulum.now("US/Eastern")),
            CronClock(cron="0 5 * * 1-6", start_date=pendulum.now("US/Eastern")),
        ],
    )
else:
    schedule = None


def state_handler(
    obj: Union[Task, Flow], old_state: State, new_state: State
) -> Optional[State]:
    logger = context.get("logger")
    message = f"This is an automated alert to let you know that the flow {obj.name} has failed"
    if new_state.is_failed():
        if voice_alert:
            voice_list = [(voice, "voice") for voice in voice_alert.split(",")]
            send_alert(recipients=voice_list, message=message, logger=logger)

        if text_alert:
            text_list = [(text, "sms") for text in text_alert.split(",")]
            send_alert(recipients=text_list, message=message, logger=logger)

        # Disable schedule if the flow fails
        schedule_client = Client()
        schedule_mutation = """
        mutation ($input: set_schedule_inactive_input!)
        {
            set_schedule_inactive(input: $input)
            {
                success,
                error
            }
        }
        """

        flow_name = context.get("flow_name")
        logger.info(
            f"Turning off schedule for flow {flow_name} because it failed.  "
            "Schedule will need to be turned back on manually after problems are fixed."
        )
        schedule_client.graphql(
            schedule_mutation,
            variables=dict(
                input=dict(
                    flow_id=context.get("flow_id"),
                )
            ),
        )

    return new_state


@task(name="validate parameters")
def validate_parameters(date_to_process) -> str:
    logger = context.get("logger")
    date_format = "%Y-%m-%d"

    if date_to_process == "1900-01-01":
        logger.info("Default date found, looking for process date in kv store...")
        date_to_process = get_key_value(process_date_key)

    try:
        date_obj = datetime.datetime.strptime(date_to_process, date_format)
    except ValueError:
        logger.error(
            f"Incorrect date format ({date_to_process}): Only YYYY-MM-DD is valid"
        )
        raise signals.FAIL()

    logger.info(f"Processing data from {date_to_process}")
    return date_to_process


@task(name="predecessor_checks_sample_parent_flow")
def predecessor_checks_sample_parent_flow_task():
    logger = context.get("logger")

    # check to see if the flow is already running
    if is_flow_running():
        logger.error(
            "Flow run {0} stopped because {1} flow is already running".format(
                context.get("flow_run_name"), context.get("flow_name")
            )
        )
        raise signals.FAIL()


@task(name="set process date")
def set_process_date(date_parameter: str, date_to_process: str):
    logger = context.get("logger")
    date_format = "%Y-%m-%d"

    if date_parameter == "1900-01-01":
        date_to_process = (
            datetime.datetime.strptime(date_to_process, date_format)
            + datetime.timedelta(days=1)
        ).strftime(date_format)
        logger.info(f"Setting process date in KV store to {date_to_process}")
        set_key_value(key=process_date_key, value=date_to_process)


@task()
def start_flow_run(process_date: str, flow_name: str):
    StartFlowRun(
        project_name=prefect_project,
        flow_name=flow_name,
        wait=True,
        parameters={
            "process_date": process_date,
        },
    ).run()


with Flow(
    name="sample_parent_flow",
    executor=LocalDaskExecutor(num_workers=4),
    state_handlers=[state_handler],
    schedule=schedule,
) as flow:

    process_date = Parameter(name="process_date", default="1900-01-01")
    validate_param = validate_parameters(process_date)
    predecessor_checks = predecessor_checks_sample_parent_flow_task(
        upstream_tasks=[validate_param]
    )
    sample_child_flow = start_flow_run(
        validate_param,
        "sample_child_flow",
        upstream_tasks=[predecessor_checks],
        task_args={"name": "Run sample_child_flow"},
    )

    set_process_date(
        process_date,
        validate_param,
        upstream_tasks=[sample_child_flow],
    )
