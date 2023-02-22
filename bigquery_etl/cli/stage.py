"""bigquery-etl CLI stage commands."""

import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import click
from google.cloud import bigquery

from ..cli.query import deploy as deploy_query_schema
from ..cli.query import update as update_query_schema
from ..cli.routine import publish as publish_routine
from ..cli.utils import paths_matching_name_pattern, sql_dir_option
from ..cli.view import publish as publish_view
from ..routine.parse_routine import (
    UDF_FILE,
    RawRoutine,
    accumulate_dependencies,
    read_routine_dir,
)
from ..schema import SCHEMA_FILE, Schema
from ..view import View

VIEW_FILE = "view.sql"
QUERY_FILE = "query.sql"
QUERY_SCRIPT = "query.py"
ROOT = Path(__file__).parent.parent.parent
TEST_DIR = ROOT / "tests" / "sql"


@click.group(help="Commands for managing stage deploys")
def stage():
    """Create the CLI group for the stage command."""
    pass


@stage.command(
    help="""
        Deploy artifacts to the configured stage project. The order of deployment is:
        UDFs, views, tables.

    Examples:
    ./bqetl stage deploy sql/moz-fx-data-shared-prod/telemetry_derived/
    """
)
@click.argument(
    "paths",
    nargs=-1,
)
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project to deploy artifacts to",
    default="bigquery-etl-integration-test",
)
@sql_dir_option
@click.option(
    "--dataset-suffix",
    "--dataset_suffix",
    help="Suffix appended to the deployed dataset",
)
@click.option(
    "--update-references",
    "--update_references",
    default=True,
    is_flag=True,
    help="Update references to deployed artifacts with project and dataset artifacts have been deployed to.",
)
@click.option(
    "--copy-sql-to-tmp-dir",
    "--copy_sql_to_tmp_dir",
    help="Copy existing SQL from the sql_dir to a temporary directory and apply updates there.",
    default=False,
    is_flag=True,
)
@click.option(
    "--remove-updated-artifacts",
    "--remove_updated_artifacts",
    default=False,
    is_flag=True,
    help="Remove artifacts that have been updated and deployed to stage from prod folder. This ensures that"
    + " tests don't run on outdated or undeployed artifacts (required for CI)",
)
@click.pass_context
def deploy(
    ctx,
    paths,
    project_id,
    sql_dir,
    dataset_suffix,
    update_references,
    copy_sql_to_tmp_dir,
    remove_updated_artifacts,
):
    """Deploy provided artifacts to destination project."""
    if copy_sql_to_tmp_dir:
        # copy SQL to a temporary directory
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(sql_dir, tmp_dir)
        sql_dir = tmp_dir / sql_dir.name

    artifact_files = set()

    # get SQL files for artifacts that are to be deployed
    for path in paths:
        artifact_files.update(
            [
                p
                for p in paths_matching_name_pattern(
                    path, sql_dir, None, files=["*.sql", "*.py"]
                )
                if p.suffix in [".sql", ".py"]
            ]
        )

    # any dependencies need to be determined an deployed as well since the stage
    # environment doesn't have access to the prod environment
    artifact_files.update(_udf_dependencies(artifact_files))
    artifact_files.update(_view_dependencies(artifact_files, sql_dir))

    # update references of all deployed artifacts
    # references needs to be set to the stage project and the new dataset identifier
    if update_references:
        _update_references(artifact_files, project_id, dataset_suffix, sql_dir)

    updated_artifact_files = set()
    (Path(sql_dir) / project_id).mkdir(parents=True, exist_ok=True)
    # copy updated files locally to a folder representing the stage env project
    for artifact_file in artifact_files:
        project = artifact_file.parent.parent.parent.name
        dataset = artifact_file.parent.parent.name
        name = artifact_file.parent.name
        test_path = TEST_DIR / project / dataset / name

        if dataset_suffix:
            dataset = f"{dataset}_{dataset_suffix}"

        new_artifact_path = Path(sql_dir) / project_id / dataset / name
        new_artifact_path.mkdir(parents=True, exist_ok=True)
        shutil.copytree(artifact_file.parent, new_artifact_path, dirs_exist_ok=True)
        updated_artifact_files.add(new_artifact_path / artifact_file.name)

        # copy tests to the right structure
        if test_path.exists():
            shutil.copytree(
                test_path, TEST_DIR / project_id / dataset / name, dirs_exist_ok=True
            )
            if remove_updated_artifacts:
                shutil.rmtree(test_path)

    # remove artifacts from the "prod" folders
    if remove_updated_artifacts:
        for artifact_file in artifact_files:
            if artifact_file.parent.exists():
                shutil.rmtree(artifact_file.parent)

    # deploy to stage
    _deploy_artifacts(ctx, updated_artifact_files, project_id, dataset_suffix, sql_dir)


def _udf_dependencies(artifact_files):
    """Determine UDF dependencies."""
    udf_dependencies = set()
    raw_routines = read_routine_dir()
    udf_files = [file for file in artifact_files if file.name == UDF_FILE]
    for udf_file in udf_files:
        # all referenced UDFs need to be deployed in the same stage project due to access restrictions
        raw_routine = RawRoutine.from_file(udf_file)
        udfs_to_publish = accumulate_dependencies([], raw_routines, raw_routine.name)

        for dependency in udfs_to_publish:
            dataset, name = dependency.split(".")
            file_path = (
                raw_routine.filepath.parent.parent.parent / dataset / name / UDF_FILE
            )
            if file_path not in artifact_files:
                artifact_files.add(file_path)

    return udf_dependencies


def _view_dependencies(artifact_files, sql_dir):
    """Determine view dependencies."""
    view_dependencies = set()
    view_dependency_files = [file for file in artifact_files if file.name == VIEW_FILE]
    for dep_file in view_dependency_files:
        # all references views and tables need to be deployed in the same stage project
        if dep_file not in artifact_files:
            view_dependencies.add(dep_file)

        if dep_file.name == VIEW_FILE:
            view = View.from_file(dep_file)

            for dependency in view.table_references:
                project, dataset, name = dependency.split(".")
                file_path = Path(view.path).parent.parent.parent / dataset / name

                file_exists_for_dependency = False
                for file in [VIEW_FILE, QUERY_FILE, QUERY_SCRIPT]:
                    if (file_path / file).is_file():
                        if (file_path / file) not in artifact_files:
                            view_dependency_files.append(file_path / file)

                        file_exists_for_dependency = True
                        break

                path = Path(sql_dir) / project / dataset / name
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    partitioned_by = "submission_timestamp"
                    schema = Schema.for_table(
                        project=project,
                        dataset=dataset,
                        table=name,
                        partitioned_by=partitioned_by,
                    )
                    schema.to_yaml_file(path / SCHEMA_FILE)

                if not file_exists_for_dependency:
                    (path / QUERY_SCRIPT).write_text("")
                    view_dependencies.add(path / QUERY_SCRIPT)

    return view_dependencies


def _update_references(artifact_files, project_id, dataset_suffix, sql_dir):
    replace_references = []
    for artifact_file in artifact_files:
        name = artifact_file.parent.name
        original_dataset = artifact_file.parent.parent.name
        deployed_dataset = original_dataset
        if dataset_suffix:
            deployed_dataset += f"_{dataset_suffix}"
        original_project = artifact_file.parent.parent.parent.name
        deployed_project = project_id

        # replace partially qualified references (like "telemetry.main")
        replace_references.append(
            (
                re.compile(
                    rf"(?<![\._])`?{original_dataset}`?\.{name}(?![a-zA-Z0-9_])`?"
                ),
                f"`{deployed_project}`.{deployed_dataset}.{name}",
            )
        )
        # replace fully qualified references (like "moz-fx-data-shared-prod.telemetry.main")
        replace_references.append(
            (
                rf"(?<![a-zA-Z0-9_])`?{original_project}`?\.{original_dataset}`?\.{name}(?![a-zA-Z0-9_])`?",
                f"`{deployed_project}`.{deployed_dataset}.{name}",
            )
        )

    for path in Path(sql_dir).rglob("*.sql"):
        # apply substitutions
        if path.is_file():
            sql = path.read_text()

            for ref in replace_references:
                sql = re.sub(ref[0], ref[1], sql)

            path.write_text(sql)


def _deploy_artifacts(ctx, artifact_files, project_id, dataset_suffix, sql_dir):
    """Deploy UDFs, tables and views."""
    # deploy UDFs
    udf_files = [file for file in artifact_files if file.name == UDF_FILE]
    for udf_file in udf_files:
        dataset = udf_file.parent.parent.name
        create_dataset_if_not_exists(
            project_id=project_id, dataset=dataset, suffix=dataset_suffix
        )
    ctx.invoke(publish_routine, name=None, project_id=project_id, dry_run=False)

    # deploy table schemas
    query_files = [
        file for file in artifact_files if file.name in [QUERY_FILE, QUERY_SCRIPT]
    ]
    for query_file in query_files:
        dataset = query_file.parent.parent.name
        create_dataset_if_not_exists(
            project_id=project_id, dataset=dataset, suffix=dataset_suffix
        )
        ctx.invoke(
            update_query_schema,
            name=str(query_file),
            sql_dir=sql_dir,
            project_id=project_id,
            respect_dryrun_skip=False,
        )
        ctx.invoke(
            deploy_query_schema,
            name=str(query_file),
            sql_dir=sql_dir,
            project_id=project_id,
            force=True,
            respect_dryrun_skip=False,
            skip_external_data=True,
        )

    # deploy views
    view_files = [file for file in artifact_files if file.name == VIEW_FILE]
    for view_file in view_files:
        dataset = view_file.parent.parent.name
        create_dataset_if_not_exists(
            project_id=project_id, dataset=dataset, suffix=dataset_suffix
        )

    ctx.invoke(
        publish_view,
        name=None,
        sql_dir=sql_dir,
        project_id=project_id,
        dry_run=False,
        skip_authorized=True,
        force=True,
    )


def create_dataset_if_not_exists(project_id, dataset, suffix=None):
    """Create a temporary dataset if not already exists."""
    client = bigquery.Client(project_id)
    dataset = bigquery.Dataset(f"{project_id}.{dataset}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = 60 * 60 * 1000  # ms

    # mark dataset as expired 1 hour from now; can be removed by CI
    expiration = int(
        ((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() + 60 * 60) * 1000
    )
    dataset.labels = {"expires_on": expiration}
    if suffix:
        dataset.labels["suffix"] = suffix

    dataset = client.update_dataset(dataset, ["default_table_expiration_ms", "labels"])


@stage.command(
    help="""
        Remove deployed artifacts from stage environment

    Examples:
    ./bqetl stage clean
    """
)
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project to deploy artifacts to",
    default="bigquery-etl-integration-test",
)
@click.option(
    "--dataset-suffix",
    "--dataset_suffix",
    help="Suffix appended to the deployed dataset",
)
@click.option(
    "--delete-expired",
    "--delete_expired",
    help="Remove datasets that have expired",
    default=True,
    is_flag=True,
)
def clean(project_id, dataset_suffix, delete_expired):
    """Reset the stage environment."""
    client = bigquery.Client(project_id)
    datasets = list(client.list_datasets())
    current_timestamp = int(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000
    )

    for dataset in datasets:
        if dataset.labels:
            # remove datasets that either match the suffix or are expired
            for label, value in dataset.labels.items():
                if (
                    dataset_suffix and label == "suffix" and value == dataset_suffix
                ) or (
                    delete_expired
                    and label == "expires_on"
                    and int(value) < current_timestamp
                ):
                    client.delete_dataset(dataset, delete_contents=True)
