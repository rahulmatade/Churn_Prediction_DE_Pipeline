from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, DagsterDbtTranslator, dbt_assets
from typing import Any, Mapping
import os

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        # Get the default asset key from parent class
        asset_key = super().get_asset_key(dbt_resource_props)
        
        # Only modify source asset keys to match Airbyte's format
        if dbt_resource_props.get("resource_type") == "source":
            # For sources, use the format: ["bigquery", "raw", "table_name"]
            source_name = dbt_resource_props.get("source_name")
            table_name = dbt_resource_props.get("name")
            
            # Match the Airbyte asset key format
            return AssetKey(["bigquery", "raw", table_name])
        
        # For models, keep the default behavior
        return asset_key

@dbt_assets(
    manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


