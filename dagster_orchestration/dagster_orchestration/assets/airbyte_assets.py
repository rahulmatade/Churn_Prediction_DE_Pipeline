import os
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance

# Define the Airbyte resource
airbyte_instance = AirbyteResource(
    host="localhost",
    port="8000",
    username="rahulmatade21@gmail.com",
    password=os.getenv("AIRBYTE_PASSWORD"),
)

# Load all connections from your workspace
airbyte_assets_list = load_assets_from_airbyte_instance(
    airbyte_instance,
    workspace_id="62f3b256-f490-44c7-bd0f-5ba55c6735d2",
    key_prefix=["bigquery", "raw"],
)

