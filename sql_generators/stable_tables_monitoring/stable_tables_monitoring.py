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


def generate_stable_table_bigconfig_files(target_project, enable_monitoring):
    """Generate the metadata and bigconfig files and write to correct directories."""
    THIS_PATH = Path(__file__).parent
    TEMPLATES_DIR = THIS_PATH / "templates"
    SQL_BASE_DIR = THIS_PATH.parent.parent / "sql" / target_project

    BIGEYE_COLLECTION = "Operational Checks"
    BIGEYE_SLACK_CHANNEL = "#de-bigeye-triage"

    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    bigconfig_template = env.get_template("stable_tables_monitoring.bigconfig.yml")
    metadata_template = env.get_template("stable_tables_monitoring.metadata.yaml")

    stable_table_bigconfigs = ConfigLoader.get("monitoring", "stable_tables_monitoring")

    # Create directory for each dataset
    for dataset_name, table_names in stable_table_bigconfigs.items():

        target_dir = SQL_BASE_DIR / dataset_name
        target_dir.mkdir(parents=True, exist_ok=True)

        # Create directory for each table each with a metadata.yaml and bigconfig.yml file
        for table in table_names:
            name_part, version_part = parse_config_name(table)

            rendered_content = bigconfig_template.render(
                project_id=target_project,
                dataset=dataset_name,
                name=name_part,
                version=version_part,
                bigeye_collection=BIGEYE_COLLECTION,
                bigeye_notification_slack_channel=BIGEYE_SLACK_CHANNEL,
            )

            metadata_rendered = metadata_template.render(
                bigeye_collection=BIGEYE_COLLECTION,
                enable_monitoring=enable_monitoring,
                name=name_part,
            )

            stable_table_bigconfig_dir = target_dir / table
            stable_table_bigconfig_dir.mkdir(exist_ok=True)

            write_file(metadata_rendered, stable_table_bigconfig_dir / "metadata.yaml")
            write_file(rendered_content, stable_table_bigconfig_dir / "bigconfig.yml")
