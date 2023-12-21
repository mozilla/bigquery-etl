# Import needed libraries
import os
import shutil
from pathlib import Path
import click
import yaml
from jinja2 import Environment, FileSystemLoader
from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util.common import write_sql
import json

# Define needed filepaths
fpath = Path(os.path.dirname(__file__))
template_fpath = fpath / "templating.yaml"
query_fpath = fpath / "query.sql"
config_fpath = fpath / "config.json"
metadata_fpath = fpath / "metadata.yaml"
schema_fpath = fpath / "schema.yaml"

########Define functions for later use########
#Generate query 
def generate_query(prjct, dataset, destination_table, write_dir, tmplt_fpath):
    """Generate query and write to directory"""
    with open(tmplt_fpath, mode="r", encoding="UTF-8") as f:
        render_kwargs = yaml.safe_load(f) or {}
    env = Environment(loader=FileSystemLoader(tmplt_fpath))
    template = env.get_template("query.sql")

    write_sql(
        write_dir / project,
        f"{prjct}.{dataset}.{destination_table}",
        "query.sql",
        reformat(template.render(**render_kwargs)),
    )

#Generate view
# def generate_view(prjct, dataset, destination_table, write_dir):
#     """Generate feature usage table view."""
#     view_name = destination_table.split("_v")[0]
#     view_dataset = dataset.split("_derived")[0]

#     sql = reformat(
#         f"""
#         CREATE OR REPLACE VIEW `{prjct}.{view_dataset}.{view_name}` AS
#         SELECT
#             *
#         FROM
#             `{prjct}.{dataset}.{destination_table}`
#     """
#     )

#     write_sql(
#         write_dir / project, f"{prjct}.{view_dataset}.{view_name}", "view.sql", sql
#     )

# #Generate metadata
# def generate_metadata(prjct, dataset, destination_table, write_dir):
#     """Copy metadata.yaml file to destination directory."""
#     shutil.copyfile(
#         fpath / "templates" / "metadata.yaml",
#         write_dir / prjct / dataset / destination_table / "metadata.yaml",
#     )

# #Generate schema
# def generate_schema(prjct, dataset, destination_table, write_dir):
#     """Generate the table schema."""
#     # get schema
#     table_schema = Schema.for_table(prjct, dataset, destination_table)
#     query_schema = Schema.from_query_file(
#         write_dir / prjct / dataset / destination_table / "query.sql",
#         use_cloud_function=True,
#     )
#     query_schema.merge(table_schema)
#     schema_path = write_dir / project / dataset / destination_table / SCHEMA_FILE
#     query_schema.to_yaml_file(schema_path)


def generate(prjct, dataset, destination_table, write_dir, tmplt_fpath):
    """Generate the feature usage table."""
    generate_query(prjct, dataset, destination_table, write_dir, tmplt_fpath) 
    #generate_view(target_project, dataset, destination_table, output_dir)
    #generate_metadata(target_project, dataset, destination_table, output_dir)
    #generate_schema(target_project, dataset, destination_table, output_dir)

######Load the configs
with open(config_fpath, mode="r", encoding = "UTF-8") as jsonfile:
    config = json.load(jsonfile)

#Get the project from the config file
project = config["project"]

#For each table we want to create listed in config
for key, value in config["objects"].items():
    #Get the configs for this table/view combo
    platform = value["platform"]
    friendly_name = value["friendly_name"]
    table_dataset_name = value["table_dataset_name"]
    view_dataset_name = value["view_dataset_name"]
    table_name = value["table_name"]

    #Run the generate for this object 
    generate(prjct = project, 
             dataset = table_dataset_name, 
             destination_table = table_name, 
             write_dir = "sql", 
             tmplt_fpath = template_fpath)
    

