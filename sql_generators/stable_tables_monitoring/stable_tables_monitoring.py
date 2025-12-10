"""Generate metadata and bigconfig files for stable tables."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader


def write_file(content, file_path):
    """Write metadata.yaml and bigconfig.yml to correct directories."""
    with open(file_path, "w") as file:
        file.write(content)


def parse_config_name(config):
    """Parse config name into name and version parts."""
    if "_" in config:
        return config.rsplit("_", 1)
    return config, "v1"


def generate_stable_table_bigconfig_files(target_project, output_dir, enable_monitoring):
    """Generate the metadata and bigconfig files and write to correct directories."""
    templates_dir = Path(__file__).parent / "templates"
    sql_base_dir = Path(output_dir) / target_project

    bigeye_collection = "Operational Checks"
    bigeye_slack_channel = "#de-bigeye-triage"

    env = Environment(loader=FileSystemLoader(str(templates_dir)))
    bigconfig_template = env.get_template("bigconfig.yml.jinja")
    metadata_template = env.get_template("metadata.yaml.jinja")

    stable_table_bigconfigs = ConfigLoader.get("monitoring", "stable_tables_monitoring")

    # Create directory for each dataset
    for dataset_name, table_names in stable_table_bigconfigs.items():
        target_dir = sql_base_dir / dataset_name
        target_dir.mkdir(parents=True, exist_ok=True)

        # Create directory for each table each with a metadata.yaml and bigconfig.yml file
        for table in table_names:
            name_part, version_part = parse_config_name(table)

            rendered_content = bigconfig_template.render(
                project_id=target_project,
                dataset=dataset_name,
                name=name_part,
                version=version_part,
                bigeye_collection=bigeye_collection,
                bigeye_notification_slack_channel=bigeye_slack_channel,
            )

            metadata_rendered = metadata_template.render(
                bigeye_collection=bigeye_collection,
                enable_monitoring=enable_monitoring,
                name=name_part,
            )

            stable_table_bigconfig_dir = target_dir / table
            stable_table_bigconfig_dir.mkdir(exist_ok=True)

            write_file(metadata_rendered, stable_table_bigconfig_dir / "metadata.yaml")
            write_file(rendered_content, stable_table_bigconfig_dir / "bigconfig.yml")
