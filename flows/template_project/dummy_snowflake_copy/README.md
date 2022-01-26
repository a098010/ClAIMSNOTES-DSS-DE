This dummy flow copies a small json file from an external stage into a RAW snowflake table. The flow then parses a few fields from the unstructured RAW snowflake table and loads the data into a TARGET table. 

### Initial Considerations

**Source Data:** Initial source data was pulled from the external stage SHELL_CREATOR_DEV.STAGING.ANALYTICS_LOGS. The stage points to an S3 bucket located at URL = s3://pgr-dmade-dmaderesearch-dev-aws01d-nonversioned/snowflake_deal_test/analytics_log/. The bucket contains a large number of small log files in JSON format. The flow ingests a single file containing ~700 records. Because this was a simple test flow, the contents of the log file were not particularly important. The data was chosen because an existing ZID had access to it and the target snowflake database (see below).

**Target Database:** The flow loads data into the Snowflake database DATA_CORE_SANDBOX. This is a new database that will be used by the Data Core team for general testing. I created a [PR](https://github.com/PCDST/snowflake-shell-configs-data-core-platform/pull/13) to have the database added to the shell creator configs. 

**ZID:** A ZID is needed to perform the ETL defined in the flow. The ZID needs access to both the external stage and the DATA_CORE_SANDBOX database. The ZID ZSFLK01D was chosen because it already had access to the external stage. Additionally, the ZID was used by the shell creator to create the DATA_CORE_SANDBOX (defined in shell configs [here](https://github.com/PCDST/snowflake-shell-configs-data-core-platform/blob/main/dev/dev_config.json#L8-L10)) so the ZID was given access to the database by default. This meant the ZID ZSFLK01D could perform all required flow tasks without any additional permission requests. It is generally preferred to use a separate ZID for ETL tasks and infra provisioning but because this was a small test case it was determined to be ok.

### Flow Definition
The RAW and TARGET snowflake table ddl is defined [here](https://github.com/PCDST/data_core_flows/tree/main/flows/DSS-DE-Dev/dummy_snowflake_copy/ddl). This step is run manually and not via prefect orchestration. This is the same for Telematics flows as well.

The flow has two steps. First is a [COPY_INTO](https://github.com/PCDST/data_core_flows/blob/main/flows/DSS-DE-Dev/dummy_snowflake_copy/sql/raw_analytics_logs_copy_into.sql) from the external stage into the RAW table. This sql statement only copies a single file from the external stage. The file is in JSON format. The second step is an [INSERT](https://github.com/PCDST/data_core_flows/blob/main/flows/DSS-DE-Dev/dummy_snowflake_copy/sql/target_analytics_logs_insert.sql) from the RAW to TARGET table. The sql statement parses a few of the JSON fields and loads them into structured format. 

The [flow definition](https://github.com/PCDST/data_core_flows/blob/main/flows/DSS-DE-Dev/dummy_snowflake_copy/flow.py) has two tasks, one for the COPY_INTO step and another for the INSERT. I created a new [credentials file](https://github.com/PCDST/data_core_flows/blob/main/flows/DSS-DE-Dev/dummy_snowflake_copy/flow.py#L11-L12) for ZSFLK01D. 

### Registering and Running the Flow
All flows under [this](https://github.com/PCDST/data_core_flows/tree/main/flows/DSS-DE-Dev) directory are automatially registered in Prefect Cloud via a [jenkins job](https://jenkinsx.pgraws98.com/job/data_core_flows/job/main/). The job will check for repo changes every 5 minutes but you can kick off a build anytime after you commit to main. The flow will be registered under the Prefect Cloud team [PGR - DSS Core](https://cloud.prefect.io/pgr-dss-core). Once registered you can navigate to the flow details page a click Quick Run or Run. 
