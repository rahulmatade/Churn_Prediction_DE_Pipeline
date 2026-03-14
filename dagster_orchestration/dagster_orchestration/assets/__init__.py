import os
from dagster import op, job
from dagster_dbt import DbtCliResource
from dagster_airbyte import airbyte_resource, airbyte_sync_op

resources = {
    "airbyte": airbyte_resource.configured(
        {
            "host": "localhost",  
            "port": "8000",         # Airbyte API port
        }
    ),
    "dbt": DbtCliResource(
        project_dir=os.getenv("DBT_PROJECT_DIR"),
        profiles_dir=os.getenv("DBT_PROFILES_DIR")
    )
}

# dbt CLI resource
'''dbt = DbtCliResource(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)'''

@op(required_resource_keys={"dbt"})
def run_dbt_models(context):
    context.resources.dbt.run()

@op(required_resource_keys={"dbt"})
def test_dbt_models(context):
    context.resources.dbt.test()

@job(resource_defs={"dbt": resources}) #Check and change back to dbt if error exists
def dbt_job():
    run_dbt_models()
    test_dbt_models()
