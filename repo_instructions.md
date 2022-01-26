### Configure Flows Repo

In order for the customer to use the repo and register their flows, there is minimal setup.

Go to Jenkinsfile and change line 10:

`PGR_PREFECT_Label="enterlabelhere"`

Remove `enterlabelhere` and replace it <new_customer> within the quotes.

Now users are able to start building flows in their repo.

```bash
├── flows
|   ├── <Prefect Project Name>
|   |   ├── snowflake_cost_usage
|   |   |   ├── ddl
|   |   |   |   ├── *.sql
|   |   |   ├── sql
|   |   |   |   ├── *.sql
|   |   |   ├── flow.py or flow_*.py
|   |   |   ├── flow_config.yml
|   |   |   ├── readme.md
├── Dockerfile
├── Jenkinsfile
└── global_config.yml
```

The Prefect Project Name should match the project name defined in Prefect, or if it doesn't exist the registration process will create it.  For example, if you have a project called DSS-DE-Dev the folder in your repo should be called DSS-DE.  The old model that doesn't have folders for each project will still work but has been deprecated so the new structure should be used going forward.

Each flow will have their own folder - like the snowflake_cost_usage folder in the example above.  Within this folder, the flow.py (or a file matching the pattern flow_*.py) will define the actual flow that gets registered into Prefect. You can define variables in your global_config.yml - which is available to all flows.  Or you can keep it local to your flow in the flow_config.yml.  The readme.md follows the same concept.
