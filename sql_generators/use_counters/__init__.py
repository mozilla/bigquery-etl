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
def generate_query(prjct, dataset, destination_table, write_dir, tmplt_fpath, query_fname):
    """Generate query and write to directory"""
    with open(tmplt_fpath, mode="r", encoding="UTF-8") as f:
        render_kwargs = yaml.safe_load(f) or {}
    env = Environment(loader=FileSystemLoader(fpath))
    template = env.get_template(query_fname)

    write_sql(
        write_dir + "/" + prjct,
        f"{prjct}.{dataset}.{destination_table}",
        "query.sql",
        reformat(template.render(**render_kwargs)),
    )

#Generate view
def generate_view(prjct, vw_dataset, dataset, view_nm, destination_table, write_dir):
    """Generate use counters V2 views"""
    sql = reformat(
        f"""
        CREATE OR REPLACE VIEW `{prjct}.{vw_dataset}.{view_nm}` AS
        SELECT
            *
        FROM
            `{prjct}.{dataset}.{destination_table}`
    """
    )

    write_sql(
        write_dir + "/" + prjct, f"{prjct}.{vw_dataset}.{view_nm}", "view.sql", sql
    )


#Generate metadata
def generate_metadata(prjct, dataset, destination_table, write_dir):
    """Copy metadata.yaml file to destination directory."""
    #Render the metadata yaml file with parameters
    env = Environment(loader=FileSystemLoader(fpath))
    metadata_template = env.get_template("metadata.yaml")
    #metadata_template.render(tbl_friendly_name= friendly_name)

    write_sql(
        write_dir + "/" + prjct,
        f"{prjct}.{dataset}.{destination_table}",
        "metadata.yaml",
        metadata_template.render(tbl_friendly_name= friendly_name),
    )


#Generate schema
def generate_schema(schema_filepath, prjct, dataset, destination_table, write_dir):
    """Generate the table schema."""
    shutil.copyfile(
        schema_filepath, 
        write_dir + "/" + prjct + "/"+ dataset +"/"+ destination_table + "/" "schema.yaml",
    )


def generate(prjct, dataset, destination_table, write_dir, tmplt_fpath, query_fname, vw_dataset, view_nm, schema_filepath):
    """Generate the feature usage table."""
    generate_query(prjct, dataset, destination_table, write_dir, tmplt_fpath, query_fname) 
    generate_view(prjct, vw_dataset, dataset, view_nm, destination_table, write_dir)
    generate_metadata(prjct, dataset, destination_table, write_dir)
    generate_schema(schema_filepath, prjct, dataset, destination_table, write_dir)

######Load the configs
with open(config_fpath, mode="r", encoding = "UTF-8") as jsonfile:
    config = json.load(jsonfile)

#Get the project from the config file
project = config["project"]

#For each table we want to create listed in config
for key, value in config["objects"].items():
    #Get the configs for this table/view combo
    friendly_name = value["friendly_name"]
    table_dataset_name = value["table_dataset_name"]
    view_dataset_name = value["view_dataset_name"]
    table_name = value["table_name"]
    query_filename = value["query"]

    #Run the generate for this object
    generate(prjct = project,
             dataset = table_dataset_name, 
             destination_table = table_name, 
             write_dir = "sql",
             tmplt_fpath = template_fpath,
             query_fname = query_filename, 
             vw_dataset = view_dataset_name,
             view_nm = table_name,
             schema_filepath = schema_fpath)