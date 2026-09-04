"""Generate metadata and bigconfig files for stable tables."""

from functools import partial
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util.process_pool import process_pool


def write_file(content, file_path):
    """Write metadata.yaml and bigconfig.yml to correct directories."""
    with open(file_path, "w") as file:
        file.write(content)


def parse_config_name(config):
    """Parse config name into name and version parts."""
    if "_" in config:
        return config.rsplit("_", 1)
    return config, "v1"


def _generate_table_files(
    dataset_and_table,
    target_project,
    templates_dir,
    bigeye_collection,
    bigeye_slack_channel,
    enable_monitoring,
    sql_base_dir,
):
    dataset_name, table = dataset_and_table
    env = Environment(loader=FileSystemLoader(str(templates_dir)))
    name_part, version_part = parse_config_name(table)

    rendered_content = env.get_template("bigconfig.yml.jinja").render(
        project_id=target_project,
        dataset=dataset_name,
        name=name_part,
        version=version_part,
        bigeye_collection=bigeye_collection,
        bigeye_notification_slack_channel=bigeye_slack_channel,
    )

    metadata_rendered = env.get_template("metadata.yaml.jinja").render(
        bigeye_collection=bigeye_collection,
        enable_monitoring=enable_monitoring,
        name=name_part,
    )

    stable_table_bigconfig_dir = sql_base_dir / dataset_name / table
    stable_table_bigconfig_dir.mkdir(parents=True, exist_ok=True)

    write_file(metadata_rendered, stable_table_bigconfig_dir / "metadata.yaml")
    write_file(rendered_content, stable_table_bigconfig_dir / "bigconfig.yml")

    schema = Schema.for_table(
        project=target_project,
        dataset=dataset_name,
        table=table,
        partitioned_by="submission_timestamp",
    )
    schema.to_yaml_file(stable_table_bigconfig_dir / SCHEMA_FILE)


def generate_stable_table_bigconfig_files(target_project, output_dir, enable_monitoring, parallelism=8):
    """Generate the metadata and bigconfig files and write to correct directories."""
    templates_dir = Path(__file__).parent / "templates"
    sql_base_dir = Path(output_dir) / target_project

    bigeye_collection = "Operational Checks"
    bigeye_slack_channel = "#de-bigeye-triage"

    stable_table_bigconfigs = ConfigLoader.get("monitoring", "stable_tables_monitoring")

    tables = [
        (dataset_name, table)
        for dataset_name, table_names in stable_table_bigconfigs.items()
        for table in table_names
    ]

    with process_pool(parallelism, task_count=len(tables)) as pool:
        pool.map(
            partial(
                _generate_table_files,
                target_project=target_project,
                templates_dir=templates_dir,
                bigeye_collection=bigeye_collection,
                bigeye_slack_channel=bigeye_slack_channel,
                enable_monitoring=enable_monitoring,
                sql_base_dir=sql_base_dir,
            ),
            tables,
        )
