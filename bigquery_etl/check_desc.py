#Load libraries
import yaml
import pytest
import click
import glob
from pathlib import Path
from bigquery_etl.config import ConfigLoader


def skip_check_for_column_descriptions():
    """Return a list of configured queries for which formatting should be skipped."""
    return [
        file
        for skip in ConfigLoader.get("column_descriptions", "skip", fallback=[])
        for file in glob.glob(skip, recursive=True)
    ]

def check_schema_has_descriptions(sqlfile):

    query_file_path = Path(sqlfile)

    table_name = query_file_path.parent.name
    dataset_name = query_file_path.parent.parent.name
    project_name = query_file_path.parent.parent.parent.name

    table_schema = Schema.for_table(
        project_name,
        dataset_name,
        table_name,
        client=self.client,
        id_token=self.id_token,
        partitioned_by=partitioned_by,
    )

    #Initialize that all columns have a description as True to start
    all_cols_have_desc = True

    #Loop through each entry of the schema.yaml file
    for entry in table_schema:
        #If th entry doesn't have a description, set it to false
        if ?:
            all_cols_have_desc = False
        
    return all_cols_have_desc





#If query is not on the skip list, check if it has descriptions
if sqlfile not in skip_check_for_column_descriptions():
    res = check_schema_has_descriptions()
    if res is False:
        raise ValueError(f"Missing column descriptions: {sqlfile}")
