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
#trigger_bucket = runtime_config.get('trigger_bucket')
snowflake_env = "progressive_dev.us-east-1.privatelink"
#snowflake_env = "progressive_dev.us-east-1"
azure_ad_resource = "e9e387b9-ea52-4eed-887c-ec466507f58c"
azure_ad_client = "d258a05a-2e82-45ab-b87e-f32b08085682"


#---------------------------------------testing
#working_dir = os.getcwd()
#test_path = os.listdir('flows/DSS-DE-Dev/sandbox_analyics_logs')[0]
#sql_path = working_dir + '/' + test_path + '/test.sql'

#logger = logging.getLogger(__name__) #this is your normal logger


@task
def directory_debug_task():
    logger = prefect.context.get("logger")
    logger.info(f"This will show up in prefect cloud now")
    
    try:
        base_list = os.listdir()
        logger.info(f"ls ./ => {base_list}")

        flows_list = os.listdir('flows')
        logger.info(f"ls flows/ => {flows_list}")

        dss_list = os.listdir('flows/DSS-DE-Dev')
        logger.info(f"ls flows/DSS-DE-Dev => {dss_list}")
    except Exception as e:
        logger.info(f"Error: {e}")


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

with Flow(
    name='anthony_dummy_sf_example',
    executor=LocalDaskExecutor(),
) as flow:

  directory_debug = directory_debug_task()

  raw_analytics_logs_sample_copy_into = raw_analytics_logs_sample_copy_into_task(
      sql_file='flows/DSS-DE-Dev/anthony_dummy_sf_example/raw_analytics_logs_copy_into.sql'
      #sql_file=sql_path
      #sql_file='raw_analytics_logs_copy_into.sql',
  )


if __name__ == '__main__':
    flow.run()

