import os
import prefect
import logging

from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect import context, task
from pgr_prefect.tasks import SFSQLFile
from datetime import timedelta

secret_key = "snowflake_credentials_zsflk01d.json"
secret_bucket = "pgr-eds-datacoreresearchsquid-dev-aws0jd-secret"
snowflake_env = "progressive_dev.us-east-1.privatelink"
azure_ad_resource = "e9e387b9-ea52-4eed-887c-ec466507f58c"
azure_ad_client = "d258a05a-2e82-45ab-b87e-f32b08085682"

#The below task was used for debugging
# @task
# def directory_debug_task():
#     logger = prefect.context.get("logger")
#     logger.info(f"This will show up in prefect cloud now")

#     try:
#         base_list = os.listdir()
#         logger.info(f"ls ./ => {base_list}")

#         flows_list = os.listdir('flows')
#         logger.info(f"ls flows/ => {flows_list}")

#         dss_list = os.listdir('flows/DSS-DE-Dev')
#         logger.info(f"ls flows/DSS-DE-Dev => {dss_list}")
#     except Exception as e:
#         logger.info(f"Error: {e}")


################################################
raw_analytics_logs_sample_copy_into_task = SFSQLFile(
    name='raw_analytics_logs_sample_copy_into',
    log_stdout=True,
    max_retries=1,
    retry_delay=timedelta(seconds=0),
    secret_key=secret_key,
    secret_bucket=secret_bucket,
    snowflake_env=snowflake_env,
    azure_ad_resource=azure_ad_resource,
    azure_ad_client=azure_ad_client,
    warehouse='DATA_CORE_ENABLEMENT',
    database='DATA_CORE_SANDBOX',
)

target_analytics_logs_sample_insert_task = SFSQLFile(
    name='target_analytics_logs_sample_insert',
    log_stdout=True,
    max_retries=1,
    retry_delay=timedelta(seconds=0),
    secret_key=secret_key,
    secret_bucket=secret_bucket,
    snowflake_env=snowflake_env,
    azure_ad_resource=azure_ad_resource,
    azure_ad_client=azure_ad_client,
    warehouse='DATA_CORE_ENABLEMENT',
    database='DATA_CORE_SANDBOX',
)

with Flow(
    name='dummy_snowflake_copy',
    executor=LocalDaskExecutor(),
) as flow:

  #directory_debug = directory_debug_task()

  raw_analytics_logs_sample_copy_into = raw_analytics_logs_sample_copy_into_task(
      sql_file='flows/DSS-DE-Dev/dummy_snowflake_copy/sql/raw_analytics_logs_copy_into.sql'
  )

  target_analytics_logs_sample_insert = target_analytics_logs_sample_insert_task(
      sql_file='flows/DSS-DE-Dev/dummy_snowflake_copy/sql/target_analytics_logs_insert.sql', upstream_tasks=[raw_analytics_logs_sample_copy_into]
  )

if __name__ == '__main__':
    flow.run()

